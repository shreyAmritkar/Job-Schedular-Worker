package com.example.scheduler.queue;

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
 * ZPOPMIN -> execute -> record -> repeat. See design writeup for why this
 * is a producer-consumer / counting-semaphore shape.
 */
@Service
public class WorkerPool {

    private static final Logger log = LoggerFactory.getLogger(WorkerPool.class);

    private final JobQueueService queueService;
    private final JobExecutor jobExecutor;
    private final WorkerPoolProperties properties;

    private final AtomicInteger activeWorkers = new AtomicInteger(0);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService workerExecutor;

    public WorkerPool(JobQueueService queueService, JobExecutor jobExecutor,
                       WorkerPoolProperties properties, MeterRegistry meterRegistry) {
        this.queueService = queueService;
        this.jobExecutor = jobExecutor;
        this.properties = properties;
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
                Optional<QueuedJob> next = queueService.poll();
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
            jobExecutor.execute(job);
            log.debug("Worker {} finished job {}", workerId, job.jobId());
        } catch (RuntimeException e) {
            // Stage 3 has no retry logic yet — a failed execution is just
            // logged and dropped. That's a real gap, called out in the review.
            log.error("Worker {} failed to execute job {}", workerId, job.jobId(), e);
        } finally {
            activeWorkers.decrementAndGet();
        }
    }

    /**
     * "Wait approximately 500ms" — a small +/-50ms jitter is added so that
     * multiple idle workers waking up don't all hit Redis in the same
     * instant, the same thundering-herd reasoning as Stage 2's standby
     * retry jitter. Not explicitly requested for this spec line, but cheap
     * and consistent with the rest of the codebase; flagged as a value-add.
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
