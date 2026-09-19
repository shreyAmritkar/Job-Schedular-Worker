package com.example.scheduler.queue;

import com.example.scheduler.queue.exception.QueueBackpressureException;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Wraps the Redis sorted-set priority queue: jobs:queue for ordering, a
 * companion hash jobs:queue:enqueued-at for true original wait-time
 * (needed by aging — see the design writeup for why the score alone can't
 * be decoded back into priority+timestamp once aging has touched it).
 *
 * Score = priority * PRIORITY_WEIGHT + enqueuedAtMillis, where priority 1
 * is most urgent and 10 is least (see design writeup — this direction, and
 * the '+' rather than '-' on the timestamp, is a correction to the spec's
 * literal formula, which produced LIFO instead of the required FIFO).
 */
@Service
public class JobQueueService {

    private static final Logger log = LoggerFactory.getLogger(JobQueueService.class);

    static final String QUEUE_KEY = "jobs:queue";
    static final String ENQUEUED_AT_KEY = "jobs:queue:enqueued-at";
    static final long PRIORITY_WEIGHT = 1_000_000_000L;

    // Boost stale jobs' scores only if they still exist in the queue (XX) —
    // never re-insert a job a worker already popped (see race #2 in the
    // design writeup). CH so we can count how many were actually touched.
    private static final String AGING_SCRIPT =
            "local staleCount = 0 "
                    + "local entries = redis.call('HGETALL', KEYS[2]) "
                    + "for i = 1, #entries, 2 do "
                    + "  local jobId = entries[i] "
                    + "  local enqueuedAt = tonumber(entries[i + 1]) "
                    + "  if enqueuedAt < tonumber(ARGV[1]) then "
                    + "    local currentScore = redis.call('ZSCORE', KEYS[1], jobId) "
                    + "    if currentScore then "
                    + "      local newScore = tonumber(currentScore) - tonumber(ARGV[2]) "
                    + "      redis.call('ZADD', KEYS[1], 'XX', 'CH', newScore, jobId) "
                    + "      staleCount = staleCount + 1 "
                    + "    else "
                    + "      redis.call('HDEL', KEYS[2], jobId) "
                    + "    end "
                    + "  end "
                    + "end "
                    + "return staleCount";

    private final RedisCommands<String, String> redis;
    private final JobQueueProperties properties;

    private final ScheduledExecutorService agingExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "queue-aging");
        thread.setDaemon(true);
        return thread;
    });

    public JobQueueService(RedisCommands<String, String> redis, JobQueueProperties properties,
                            MeterRegistry meterRegistry) {
        this.redis = redis;
        this.properties = properties;
        Gauge.builder("scheduler.queue.depth", this, JobQueueService::depth)
                .description("Number of jobs currently waiting in jobs:queue")
                .register(meterRegistry);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void startAgingLoop() {
        long intervalMillis = properties.getAgingInterval().toMillis();
        agingExecutor.scheduleWithFixedDelay(this::runAgingPassSafely, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    private void runAgingPassSafely() {
        try {
            applyAging();
        } catch (RuntimeException e) {
            log.error("Aging pass failed; will retry on the next interval", e);
        }
    }

    // @PreDestroy is used when a bean owns resources that need explicit cleanup, like executor threads, sockets, or connections.
    // Normal beans are just objects and need no manual shutdown; Spring/frameworks handle cleanup automatically where necessary
    @PreDestroy
    public void shutdown() {
        agingExecutor.shutdownNow();
    }

    /**
     * Backpressure is a soft limit: ZCARD then ZADD are two round trips, so
     * concurrent producers can momentarily push depth slightly past the
     * threshold before a rejection lands. Making this fully atomic would
     * need a Lua script combining the check and the write; not done here —
     * flagged as a deliberate, documented trade-off, not an oversight.
     */
    public void enqueue(UUID jobId, int priority) {
        long currentDepth = depth();
        if (currentDepth >= properties.getBackpressureThreshold()) {
            throw new QueueBackpressureException(
                    "Queue depth " + currentDepth + " has reached the backpressure threshold of "
                            + properties.getBackpressureThreshold());
        }

        long enqueuedAt = System.currentTimeMillis();
        double score = computeScore(priority, enqueuedAt);
        String member = jobId.toString();

        redis.zadd(QUEUE_KEY, score, member);
        redis.hset(ENQUEUED_AT_KEY, member, String.valueOf(enqueuedAt));
    }

    static double computeScore(int priority, long enqueuedAtMillis) {
        return (double) priority * PRIORITY_WEIGHT + enqueuedAtMillis;
    }

    /**
     * ZPOPMIN is atomic: exactly one caller ever receives a given job, no
     * matter how many workers call this concurrently. Cleanup of the
     * companion hash entry is best-effort (a second command, not part of
     * the same atomic step) — see race #3 in the design writeup for why
     * that's safe: the aging script self-heals any orphan it finds.
     */
    public Optional<QueuedJob> poll() {
        List<ScoredValue<String>> popped = redis.zpopmin(QUEUE_KEY, 1);
        if (popped.isEmpty()) {
            return Optional.empty();
        }
        ScoredValue<String> entry = popped.get(0);
        redis.hdel(ENQUEUED_AT_KEY, entry.getValue());
        return Optional.of(new QueuedJob(UUID.fromString(entry.getValue()), entry.getScore()));
    }

    public long depth() {
        Long cardinality = redis.zcard(QUEUE_KEY);
        return cardinality == null ? 0 : cardinality;
    }

    /**
     * Runs the aging Lua script: identifies jobs that have waited longer
     * than the configured threshold and lowers their score, without ever
     * resurrecting a job a worker already consumed. Returns how many jobs
     * were boosted, for logging.
     */
    public long applyAging() {
        long cutoff = System.currentTimeMillis() - properties.getAgingThreshold().toMillis();
        Long boosted = redis.eval(AGING_SCRIPT, ScriptOutputType.INTEGER,
                new String[]{QUEUE_KEY, ENQUEUED_AT_KEY},
                String.valueOf(cutoff), String.valueOf(properties.getAgingBoost()));
        long result = boosted == null ? 0 : boosted;
        if (result > 0) {
            log.info("Aging pass boosted {} stale job(s)", result);
        }
        return result;
    }
}
