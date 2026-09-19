package com.example.scheduler.queue;

import com.example.scheduler.execution.ClaimedJobHandler;
import com.example.scheduler.execution.ExecutionProperties;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A fixed pool of worker threads, each independently looping:
 * claim -> handle -> repeat. "handle" (ClaimedJobHandler) is where Stage 4's
 * idempotency/recording/retry logic lives — WorkerPool itself only owns the
 * thread-pool mechanics and the claim/backoff loop, same separation of
 * concerns as Stage 2's LeaderElectionService vs RedisLeaderRepository.
 */
@Service
public class WorkerPool {

    private static final Logger log = LoggerFactory.getLogger(WorkerPool.class);

    private final JobQueueService queueService;
    private final ClaimedJobHandler claimedJobHandler;
    private final WorkerPoolProperties properties;
    private final ExecutionProperties executionProperties;

    private final AtomicInteger activeWorkers = new AtomicInteger(0);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService workerExecutor;

    public WorkerPool(JobQueueService queueService, ClaimedJobHandler claimedJobHandler,
                       WorkerPoolProperties properties, ExecutionProperties executionProperties,
                       MeterRegistry meterRegistry) {
        this.queueService = queueService;
        this.claimedJobHandler = claimedJobHandler;
        this.properties = properties;
        this.executionProperties = executionProperties;
        Gauge.builder("scheduler.worker.active", activeWorkers, AtomicInteger::get)
                .description("Number of worker threads currently executing a job")
                .register(meterRegistry);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return; // already started
        }
        int size = properties.getSize();
        workerExecutor = Executors.newFixedThreadPool(size, this::newWorkerThread);
        for (int i = 0; i < size; i++) {
            int workerId = i;
            workerExecutor.submit(() -> workerLoop(workerId));
        }
        log.info("WorkerPool started with {} workers", size);
    }

    private Thread newWorkerThread(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setDaemon(true);
        return thread;
    }

    private void workerLoop(int workerId) {
        Thread.currentThread().setName("worker-" + workerId);
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                Optional<QueuedJob> next = queueService.claim(executionProperties.getVisibilityTimeout());
                if (next.isPresent()) {
                    processJob(workerId, next.get());
                } else {
                    sleepBeforeRetry();
                }
            } catch (RuntimeException e) {
                log.error("Worker {} hit an unexpected error", workerId, e);
            }
        }
        log.info("Worker {} stopped", workerId);
    }

    private void processJob(int workerId, QueuedJob job) {
        activeWorkers.incrementAndGet();
        try {
            claimedJobHandler.handle(job);
            log.debug("Worker {} finished handling job {}", workerId, job.jobId());
        } catch (RuntimeException e) {
            // ClaimedJobHandler implementations are expected to handle their
            // own failure/retry bookkeeping internally. Reaching here means
            // something went wrong in that bookkeeping itself (e.g. Postgres
            // unavailable) — the job stays claimed, and the recovery loop's
            // visibility-timeout sweep remains the ultimate safety net.
            log.error("Worker {} hit an unexpected error handling job {}", workerId, job.jobId(), e);
        } finally {
            activeWorkers.decrementAndGet();
        }
    }

    /**
     * "Wait approximately 500ms" — a small +/-50ms jitter is added so that
     * multiple idle workers waking up don't all hit Redis in the same
     * instant, the same thundering-herd reasoning as Stage 2's standby
     * retry jitter.
     */
    private void sleepBeforeRetry() {
        long base = properties.getPollBackoff().toMillis();
        long jitter = ThreadLocalRandom.current().nextLong(-50, 51);
        long delay = Math.max(0, base + jitter);
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public int activeWorkerCount() {
        return activeWorkers.get();
    }

    @PreDestroy
    public void shutdown() {
        running.set(false);
        if (workerExecutor != null) {
            workerExecutor.shutdownNow();
        }
    }
}
