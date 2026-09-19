package com.example.scheduler.queue;

import com.example.scheduler.queue.exception.QueueBackpressureException;
import io.lettuce.core.Range;
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

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Wraps every Redis structure involved in job delivery:
 *  - jobs:queue          sorted set, priority-ordered, waiting to be claimed
 *  - jobs:queue:enqueued-at  companion hash, true enqueue time (Stage 3, for aging)
 *  - jobs:processing     sorted set, score = claim deadline (visibility timeout)
 *  - jobs:delayed        sorted set, score = readyAt (Stage 4: backoff holding area)
 *  - jobs:dead_letter    list, jobs that exhausted their retry budget
 *
 * Score = priority * PRIORITY_WEIGHT + enqueuedAtMillis, priority 1 most
 * urgent, 10 least (see Stage 3 design writeup for why).
 */
@Service
public class JobQueueService {

    private static final Logger log = LoggerFactory.getLogger(JobQueueService.class);

    static final String QUEUE_KEY = "jobs:queue";
    static final String ENQUEUED_AT_KEY = "jobs:queue:enqueued-at";
    static final String PROCESSING_KEY = "jobs:processing";
    static final String DELAYED_KEY = "jobs:delayed";
    static final String DEAD_LETTER_KEY = "jobs:dead_letter";
    static final long PRIORITY_WEIGHT = 1_000_000_000L;

    // Claim: atomically pop the highest-priority waiting job and move it
    // into the processing set with a visibility deadline. Doing this as one
    // script closes the exact gap Stage 4 exists to prevent — a crash
    // between "popped from queue" and "recorded as processing" would lose
    // the job just as badly as never having a processing set at all.
    private static final String CLAIM_SCRIPT =
            "local popped = redis.call('ZPOPMIN', KEYS[1], 1) "
                    + "if #popped == 0 then return {} end "
                    + "local member = popped[1] "
                    + "local originalScore = popped[2] "
                    + "redis.call('HDEL', KEYS[2], member) "
                    + "redis.call('ZADD', KEYS[3], ARGV[1], member) "
                    + "return {member, originalScore}";

    // Aging (Stage 3): boost stale jobs' scores only if they still exist in
    // the queue (XX) — never re-insert a job a worker already claimed.
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

    // Promote a job whose backoff has elapsed from jobs:delayed back into
    // jobs:queue, re-establishing its enqueued-at metadata for Stage 3's
    // FIFO/aging to keep working correctly on this fresh attempt.
    private static final String PROMOTE_DELAYED_SCRIPT =
            "local removed = redis.call('ZREM', KEYS[1], ARGV[1]) "
                    + "if removed == 1 then "
                    + "  redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1]) "
                    + "  redis.call('HSET', KEYS[3], ARGV[1], ARGV[3]) "
                    + "end "
                    + "return removed";

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

    @PreDestroy
    public void shutdown() {
        agingExecutor.shutdownNow();
    }

    /**
     * Backpressure is a soft limit: ZCARD then the claim script are
     * separate round trips, so concurrent producers can momentarily push
     * depth slightly past the threshold before a rejection lands. A
     * documented trade-off, not an oversight — see Stage 3 design writeup.
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
     * Atomically claims the highest-priority waiting job: removes it from
     * jobs:queue and adds it to jobs:processing with a deadline of
     * now + visibilityTimeout. Nothing about ownership is proven beyond
     * "some worker claims this" — see the Stage 4 design writeup for the
     * race that follows from that.
     */
    public Optional<QueuedJob> claim(Duration visibilityTimeout) {
        long deadline = System.currentTimeMillis() + visibilityTimeout.toMillis();
        List<Object> result = redis.eval(CLAIM_SCRIPT, ScriptOutputType.MULTI,
                new String[]{QUEUE_KEY, ENQUEUED_AT_KEY, PROCESSING_KEY},
                String.valueOf(deadline));

        if (result.isEmpty()) {
            return Optional.empty();
        }
        String jobId = (String) result.get(0);
        long originalScore = (Long) result.get(1);
        return Optional.of(new QueuedJob(UUID.fromString(jobId), originalScore));
    }

    /** Successful completion: the job is done, drop its processing claim. */
    public void completeJob(UUID jobId) {
        removeFromProcessing(jobId);
    }

    public void removeFromProcessing(UUID jobId) {
        redis.zrem(PROCESSING_KEY, jobId.toString());
    }

    public long depth() {
        Long cardinality = redis.zcard(QUEUE_KEY);
        return cardinality == null ? 0 : cardinality;
    }

    public long processingDepth() {
        Long cardinality = redis.zcard(PROCESSING_KEY);
        return cardinality == null ? 0 : cardinality;
    }

    /** ZRANGEBYSCORE jobs:processing 0 {now} — the visibility-timeout expiry check. */
    public List<UUID> findExpiredProcessingJobs(long nowMillis) {
        return redis.zrangebyscore(PROCESSING_KEY, Range.create(0L, nowMillis)).stream()
                .map(UUID::fromString)
                .collect(Collectors.toList());
    }

    /** Stage 4: park a job in jobs:delayed until its backoff elapses. */
    public void scheduleDelayedRequeue(UUID jobId, long readyAtMillis) {
        redis.zadd(DELAYED_KEY, readyAtMillis, jobId.toString());
    }

    public List<UUID> findReadyDelayedJobs(long nowMillis) {
        return redis.zrangebyscore(DELAYED_KEY, Range.create(0L, nowMillis)).stream()
                .map(UUID::fromString)
                .collect(Collectors.toList());
    }

    public void removeFromDelayed(UUID jobId) {
        redis.zrem(DELAYED_KEY, jobId.toString());
    }

    /** Atomically moves a ready job from jobs:delayed into jobs:queue at a fresh priority score. */
    public void promoteDelayedToQueue(UUID jobId, int priority) {
        long enqueuedAt = System.currentTimeMillis();
        double score = computeScore(priority, enqueuedAt);
        redis.eval(PROMOTE_DELAYED_SCRIPT, ScriptOutputType.INTEGER,
                new String[]{DELAYED_KEY, QUEUE_KEY, ENQUEUED_AT_KEY},
                jobId.toString(), String.valueOf(score), String.valueOf(enqueuedAt));
    }

    public void pushToDeadLetter(UUID jobId) {
        redis.rpush(DEAD_LETTER_KEY, jobId.toString());
    }

    public List<UUID> listDeadLetter() {
        return redis.lrange(DEAD_LETTER_KEY, 0, -1).stream()
                .map(UUID::fromString)
                .collect(Collectors.toList());
    }

    /** count=0 removes every occurrence of this value — safe even if it somehow appears more than once. */
    public boolean removeDeadLetterEntry(UUID jobId) {
        Long removed = redis.lrem(DEAD_LETTER_KEY, 0, jobId.toString());
        return removed != null && removed > 0;
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
