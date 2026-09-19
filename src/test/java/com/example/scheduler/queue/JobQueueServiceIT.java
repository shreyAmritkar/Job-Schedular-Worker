package com.example.scheduler.queue;

import com.example.scheduler.queue.exception.QueueBackpressureException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests queue mechanics (ordering, FIFO tie-breaking, backpressure, aging)
 * directly against real Redis. Deliberately no WorkerPool here — that would
 * introduce a consumer racing to drain what this test is trying to inspect.
 * See WorkerPoolConcurrencyIT for the producer+consumer scenarios.
 */
@Testcontainers
class JobQueueServiceIT {

    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7")).withExposedPorts(6379);

    private static RedisClient redisClient;
    private static StatefulRedisConnection<String, String> connection;
    private static RedisCommands<String, String> commands;

    private JobQueueProperties properties;
    private JobQueueService queueService;

    @BeforeAll
    static void startRedis() {
        REDIS.start();
        redisClient = RedisClient.create(RedisURI.create(REDIS.getHost(), REDIS.getMappedPort(6379)));
        connection = redisClient.connect();
        commands = connection.sync();
    }

    @AfterAll
    static void stopRedis() {
        connection.close();
        redisClient.shutdown();
        REDIS.stop();
    }

    @BeforeEach
    void setUp() {
        properties = new JobQueueProperties();
        queueService = new JobQueueService(commands, properties, new SimpleMeterRegistry());
    }

    @AfterEach
    void cleanUp() {
        commands.del(JobQueueService.QUEUE_KEY, JobQueueService.ENQUEUED_AT_KEY);
        queueService.shutdown();
    }

    @Test
    void enqueueAndPoll_roundTripsASingleJob() {
        UUID jobId = UUID.randomUUID();

        queueService.enqueue(jobId, 5);
        var polled = queueService.poll();

        assertThat(polled).isPresent();
        assertThat(polled.get().jobId()).isEqualTo(jobId);
    }

    @Test
    void poll_returnsEmptyWhenQueueIsEmpty() {
        assertThat(queueService.poll()).isEmpty();
    }

    @Test
    void higherPriorityJobPopsBeforeLowerPriorityRegardlessOfInsertionOrder() {
        UUID lowPriorityId = UUID.randomUUID();
        UUID highPriorityId = UUID.randomUUID();

        queueService.enqueue(lowPriorityId, 10); // least urgent, enqueued first
        queueService.enqueue(highPriorityId, 1); // most urgent, enqueued second

        assertThat(queueService.poll()).map(QueuedJob::jobId).contains(highPriorityId);
        assertThat(queueService.poll()).map(QueuedJob::jobId).contains(lowPriorityId);
    }

    @Test
    void fifoIsPreservedWithinTheSamePriority() throws InterruptedException {
        UUID firstId = UUID.randomUUID();
        UUID secondId = UUID.randomUUID();

        queueService.enqueue(firstId, 5);
        Thread.sleep(5); // guarantee a distinct, later millisecond timestamp
        queueService.enqueue(secondId, 5);

        assertThat(queueService.poll()).map(QueuedJob::jobId).contains(firstId);
        assertThat(queueService.poll()).map(QueuedJob::jobId).contains(secondId);
    }

    @Test
    void depthReflectsCurrentQueueSize() {
        assertThat(queueService.depth()).isEqualTo(0);

        queueService.enqueue(UUID.randomUUID(), 5);
        queueService.enqueue(UUID.randomUUID(), 5);

        assertThat(queueService.depth()).isEqualTo(2);

        queueService.poll();

        assertThat(queueService.depth()).isEqualTo(1);
    }

    @Test
    void enqueue_rejectsSubmissionsOnceBackpressureThresholdReached() {
        properties.setBackpressureThreshold(2);

        queueService.enqueue(UUID.randomUUID(), 5);
        queueService.enqueue(UUID.randomUUID(), 5);

        assertThatThrownBy(() -> queueService.enqueue(UUID.randomUUID(), 5))
                .isInstanceOf(QueueBackpressureException.class);
        assertThat(queueService.depth()).isEqualTo(2); // the rejected submission never landed
    }

    @Test
    void applyAging_boostsOnlyJobsThatHaveExceededTheThreshold() throws InterruptedException {
        properties.setAgingThreshold(Duration.ofMillis(50));
        properties.setAgingBoost(1_000_000_000L);

        UUID staleJobId = UUID.randomUUID();
        queueService.enqueue(staleJobId, 5);
        Thread.sleep(100); // let staleJobId exceed the 50ms threshold

        UUID freshJobId = UUID.randomUUID();
        queueService.enqueue(freshJobId, 5);

        Double staleScoreBefore = commands.zscore(JobQueueService.QUEUE_KEY, staleJobId.toString());
        Double freshScoreBefore = commands.zscore(JobQueueService.QUEUE_KEY, freshJobId.toString());

        long boosted = queueService.applyAging();

        Double staleScoreAfter = commands.zscore(JobQueueService.QUEUE_KEY, staleJobId.toString());
        Double freshScoreAfter = commands.zscore(JobQueueService.QUEUE_KEY, freshJobId.toString());

        assertThat(boosted).isEqualTo(1);
        assertThat(staleScoreAfter).isEqualTo(staleScoreBefore - 1_000_000_000L);
        assertThat(freshScoreAfter).isEqualTo(freshScoreBefore); // untouched — not yet stale
    }

    @Test
    void applyAging_doesNotResurrectAJobAWorkerAlreadyPopped() throws InterruptedException {
        properties.setAgingThreshold(Duration.ofMillis(10));

        UUID jobId = UUID.randomUUID();
        queueService.enqueue(jobId, 5);
        Thread.sleep(50);
        queueService.poll(); // a "worker" consumes it before the aging pass runs

        long boosted = queueService.applyAging();

        assertThat(boosted).isEqualTo(0);
        assertThat(queueService.depth()).isEqualTo(0);
        // orphaned metadata is cleaned up as a side effect, not left to leak
        assertThat(commands.hget(JobQueueService.ENQUEUED_AT_KEY, jobId.toString())).isNull();
    }

    @Test
    void computeScore_lowerPriorityNumberProducesLowerScore() {
        double urgentScore = JobQueueService.computeScore(1, 1_000_000L);
        double lessUrgentScore = JobQueueService.computeScore(10, 1_000_000L);

        assertThat(urgentScore).isLessThan(lessUrgentScore);
    }

    @Test
    void computeScore_earlierTimestampProducesLowerScoreAtSamePriority() {
        double earlier = JobQueueService.computeScore(5, 1_000_000L);
        double later = JobQueueService.computeScore(5, 2_000_000L);

        assertThat(earlier).isLessThan(later);
    }
}
