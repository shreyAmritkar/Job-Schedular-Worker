package com.example.scheduler.queue;

import com.example.scheduler.execution.ClaimedJobHandler;
import com.example.scheduler.execution.ExecutionProperties;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * These tests exercise the claim -> handle -> complete loop with a
 * lightweight, Postgres-free ClaimedJobHandler test double — the full
 * production JobExecutionCoordinator (idempotency, job_executions
 * recording, retry-on-failure) is exercised separately in the `execution`
 * package's Spring-context tests. That split keeps these fast, deterministic
 * concurrency scenarios independent of Postgres entirely.
 */
@Testcontainers
class WorkerPoolConcurrencyIT {

    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7")).withExposedPorts(6379);
    private static final ExecutionProperties EXECUTION_PROPERTIES = new ExecutionProperties(); // 30s default visibility timeout, generous for these tests

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
        commands.del(JobQueueService.QUEUE_KEY, JobQueueService.ENQUEUED_AT_KEY, JobQueueService.PROCESSING_KEY);
        queueService.shutdown();
    }

    @Test
    void oneHundredMixedPriorityJobs_allHighPriorityExecuteBeforeAnyLowPriority() throws Exception {
        List<UUID> highPriorityIds = new ArrayList<>();
        List<UUID> lowPriorityIds = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            UUID id = UUID.randomUUID();
            lowPriorityIds.add(id);
            queueService.enqueue(id, 10); // enqueue low-priority FIRST
        }
        for (int i = 0; i < 50; i++) {
            UUID id = UUID.randomUUID();
            highPriorityIds.add(id);
            queueService.enqueue(id, 1); // then high-priority
        }

        RecordingClaimedJobHandler handler = new RecordingClaimedJobHandler(queueService, 0);
        WorkerPoolProperties workerProps = new WorkerPoolProperties();
        workerProps.setSize(1); // single worker gives a strictly deterministic global order
        WorkerPool pool = new WorkerPool(queueService, handler, workerProps, EXECUTION_PROPERTIES, new SimpleMeterRegistry());
        pool.start();

        try {
            awaitExecutionCount(handler, 100, Duration.ofSeconds(15));
        } finally {
            pool.shutdown();
        }

        List<UUID> order = handler.executionOrder();
        int lastHighPriorityIndex = lastIndexOfAny(order, highPriorityIds);
        int firstLowPriorityIndex = firstIndexOfAny(order, lowPriorityIds);

        assertThat(lastHighPriorityIndex)
                .as("every high-priority job should execute before any low-priority job, "
                        + "despite low-priority jobs being enqueued first")
                .isLessThan(firstLowPriorityIndex);
    }

    @Test
    void multipleWorkersConsumeSimultaneously() throws Exception {
        for (int i = 0; i < 50; i++) {
            queueService.enqueue(UUID.randomUUID(), 5);
        }

        // 100ms of simulated work per job widens the window in which
        // multiple workers are genuinely active at once, so the assertion
        // below isn't relying on a lucky sampling instant.
        RecordingClaimedJobHandler handler = new RecordingClaimedJobHandler(queueService, 100);
        WorkerPoolProperties workerProps = new WorkerPoolProperties();
        workerProps.setSize(4);
        WorkerPool pool = new WorkerPool(queueService, handler, workerProps, EXECUTION_PROPERTIES, new SimpleMeterRegistry());
        pool.start();

        int maxObservedActive = 0;
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline && handler.executionOrder().size() < 50) {
            maxObservedActive = Math.max(maxObservedActive, pool.activeWorkerCount());
            Thread.sleep(5);
        }
        pool.shutdown();

        assertThat(handler.executionOrder()).hasSize(50);
        assertThat(maxObservedActive)
                .as("at least two workers should have been observed executing simultaneously")
                .isGreaterThan(1);
    }

    @Test
    void lowPriorityJobEventuallyExecutesThroughAgingDespiteContinuousHighPriorityFlood() throws Exception {
        properties.setAgingThreshold(Duration.ofMillis(150));
        properties.setAgingBoost(1_000_000_000L);

        UUID lowPriorityJobId = UUID.randomUUID();
        queueService.enqueue(lowPriorityJobId, 10);

        AtomicBoolean producing = new AtomicBoolean(true);
        Thread producer = new Thread(() -> {
            while (producing.get()) {
                queueService.enqueue(UUID.randomUUID(), 1); // continuous most-urgent flood
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        });
        producer.setDaemon(true);
        producer.start();

        // 20ms of simulated work per job keeps a genuine backlog alive
        // instead of the flood draining faster than it can build up.
        RecordingClaimedJobHandler handler = new RecordingClaimedJobHandler(queueService, 20);
        WorkerPoolProperties workerProps = new WorkerPoolProperties();
        workerProps.setSize(4);
        workerProps.setPollBackoff(Duration.ofMillis(50));
        WorkerPool pool = new WorkerPool(queueService, handler, workerProps, EXECUTION_PROPERTIES, new SimpleMeterRegistry());
        pool.start();

        ScheduledExecutorService agingDriver = Executors.newSingleThreadScheduledExecutor();
        agingDriver.scheduleWithFixedDelay(queueService::applyAging, 100, 100, TimeUnit.MILLISECONDS);

        try {
            long deadline = System.currentTimeMillis() + Duration.ofSeconds(20).toMillis();
            boolean executed = false;
            while (System.currentTimeMillis() < deadline) {
                if (handler.executionOrder().contains(lowPriorityJobId)) {
                    executed = true;
                    break;
                }
                Thread.sleep(20);
            }

            assertThat(executed)
                    .as("the low-priority job should eventually execute via aging, "
                            + "even under a continuous high-priority flood")
                    .isTrue();
            assertThat(producing.get())
                    .as("the flood should still be actively producing when the low-priority job "
                            + "finally executes -- this proves aging pulled it forward mid-flood, "
                            + "not that the flood simply happened to end first")
                    .isTrue();
        } finally {
            producing.set(false);
            producer.interrupt();
            agingDriver.shutdownNow();
            pool.shutdown();
        }
    }

    private void awaitExecutionCount(RecordingClaimedJobHandler handler, int expectedCount, Duration timeout)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (handler.executionOrder().size() >= expectedCount) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("Expected " + expectedCount + " executions but observed "
                + handler.executionOrder().size() + " within " + timeout);
    }

    private int lastIndexOfAny(List<UUID> order, List<UUID> ids) {
        int last = -1;
        for (int i = 0; i < order.size(); i++) {
            if (ids.contains(order.get(i))) {
                last = i;
            }
        }
        return last;
    }

    private int firstIndexOfAny(List<UUID> order, List<UUID> ids) {
        for (int i = 0; i < order.size(); i++) {
            if (ids.contains(order.get(i))) {
                return i;
            }
        }
        return order.size();
    }

    /**
     * Test double recording execution order and optionally simulating work
     * via a sleep. Also calls completeJob() itself (standing in for what
     * JobExecutionCoordinator does in production) so jobs:processing
     * doesn't accumulate stale claims across these tests.
     */
    private static final class RecordingClaimedJobHandler implements ClaimedJobHandler {

        private final JobQueueService queueService;
        private final List<UUID> executionOrder = Collections.synchronizedList(new ArrayList<>());
        private final long simulatedWorkMillis;

        RecordingClaimedJobHandler(JobQueueService queueService, long simulatedWorkMillis) {
            this.queueService = queueService;
            this.simulatedWorkMillis = simulatedWorkMillis;
        }

        @Override
        public void handle(QueuedJob job) {
            if (simulatedWorkMillis > 0) {
                try {
                    Thread.sleep(simulatedWorkMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            executionOrder.add(job.jobId());
            queueService.completeJob(job.jobId());
        }

        List<UUID> executionOrder() {
            return List.copyOf(executionOrder);
        }
    }
}
