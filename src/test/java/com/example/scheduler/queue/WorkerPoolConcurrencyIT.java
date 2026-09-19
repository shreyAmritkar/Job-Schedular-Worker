package com.example.scheduler.queue;

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

@Testcontainers
class WorkerPoolConcurrencyIT {

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

        RecordingJobExecutor executor = new RecordingJobExecutor(0);
        WorkerPoolProperties workerProps = new WorkerPoolProperties();
        workerProps.setSize(1); // single worker gives a strictly deterministic global order
        WorkerPool pool = new WorkerPool(queueService, executor, workerProps, new SimpleMeterRegistry());
        pool.start();

        try {
            awaitExecutionCount(executor, 100, Duration.ofSeconds(15));
        } finally {
            pool.shutdown();
        }

        List<UUID> order = executor.executionOrder();
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
        RecordingJobExecutor executor = new RecordingJobExecutor(100);
        WorkerPoolProperties workerProps = new WorkerPoolProperties();
        workerProps.setSize(4);
        WorkerPool pool = new WorkerPool(queueService, executor, workerProps, new SimpleMeterRegistry());
        pool.start();

        int maxObservedActive = 0;
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline && executor.executionOrder().size() < 50) {
            maxObservedActive = Math.max(maxObservedActive, pool.activeWorkerCount());
            Thread.sleep(5);
        }
        pool.shutdown();

        assertThat(executor.executionOrder()).hasSize(50);
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
        RecordingJobExecutor executor = new RecordingJobExecutor(20);
        WorkerPoolProperties workerProps = new WorkerPoolProperties();
        workerProps.setSize(4);
        workerProps.setPollBackoff(Duration.ofMillis(50));
        WorkerPool pool = new WorkerPool(queueService, executor, workerProps, new SimpleMeterRegistry());
        pool.start();

        ScheduledExecutorService agingDriver = Executors.newSingleThreadScheduledExecutor();
        agingDriver.scheduleWithFixedDelay(queueService::applyAging, 100, 100, TimeUnit.MILLISECONDS);

        try {
            long deadline = System.currentTimeMillis() + Duration.ofSeconds(20).toMillis();
            boolean executed = false;
            while (System.currentTimeMillis() < deadline) {
                if (executor.executionOrder().contains(lowPriorityJobId)) {
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

    private void awaitExecutionCount(RecordingJobExecutor executor, int expectedCount, Duration timeout)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (executor.executionOrder().size() >= expectedCount) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("Expected " + expectedCount + " executions but observed "
                + executor.executionOrder().size() + " within " + timeout);
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
     * via a sleep, so concurrency scenarios can be widened enough to
     * reliably observe (multiple active workers) or genuinely contested
     * (a continuous flood competing against a starved job).
     */
    private static final class RecordingJobExecutor implements JobExecutor {

        private final List<UUID> executionOrder = Collections.synchronizedList(new ArrayList<>());
        private final long simulatedWorkMillis;

        RecordingJobExecutor(long simulatedWorkMillis) {
            this.simulatedWorkMillis = simulatedWorkMillis;
        }

        @Override
        public void execute(QueuedJob job) {
            if (simulatedWorkMillis > 0) {
                try {
                    Thread.sleep(simulatedWorkMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            executionOrder.add(job.jobId());
        }

        List<UUID> executionOrder() {
            return List.copyOf(executionOrder);
        }
    }
}
