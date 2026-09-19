package com.example.scheduler.execution;

import com.example.scheduler.job.Job;
import com.example.scheduler.job.JobRepository;
import com.example.scheduler.leader.LeaderState;
import com.example.scheduler.queue.JobQueueService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The safety net for crashed workers. Leader-only (per spec) — both to
 * avoid every instance redundantly hammering Redis with the same scan, and
 * to keep "who makes scheduling decisions" centralized the same way the
 * rest of this system already does (Stage 2's leader election exists for
 * exactly this kind of responsibility).
 *
 * Also sweeps jobs:delayed on the same tick (see design writeup for why
 * that structure exists) — a job whose backoff elapses may sit up to one
 * recovery interval longer than its computed backoff before being promoted
 * back into jobs:queue. A deliberate simplification: a separate faster
 * sweep loop would reduce that latency but adds scheduling complexity for
 * marginal benefit at this stage.
 */
@Service
public class JobRecoveryService {

    private static final Logger log = LoggerFactory.getLogger(JobRecoveryService.class);

    private final LeaderState leaderState;
    private final JobQueueService queueService;
    private final JobRepository jobRepository;
    private final RetryCoordinator retryCoordinator;
    private final ExecutionProperties properties;
    private final Counter recoveryRunsCounter;
    private final Counter recoveryRecoveredCounter;

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "job-recovery");
        thread.setDaemon(true);
        return thread;
    });

    public JobRecoveryService(LeaderState leaderState, JobQueueService queueService, JobRepository jobRepository,
                               RetryCoordinator retryCoordinator, ExecutionProperties properties,
                               MeterRegistry meterRegistry) {
        this.leaderState = leaderState;
        this.queueService = queueService;
        this.jobRepository = jobRepository;
        this.retryCoordinator = retryCoordinator;
        this.properties = properties;
        this.recoveryRunsCounter = meterRegistry.counter("recovery.runs");
        this.recoveryRecoveredCounter = meterRegistry.counter("recovery.recovered");
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        long intervalMillis = properties.getRecoveryInterval().toMillis();
        executor.scheduleWithFixedDelay(this::runSafely, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    private void runSafely() {
        try {
            runRecoveryPass();
        } catch (RuntimeException e) {
            log.error("Recovery pass failed; will retry on the next interval", e);
        }
    }

    /** Package-visible so tests can drive a single pass deterministically instead of waiting on the real timer. */
    void runRecoveryPass() {
        if (!leaderState.isLeader()) {
            return;
        }
        recoveryRunsCounter.increment();

        long now = System.currentTimeMillis();
        reclaimExpiredJobs(now);
        sweepDelayedIntoQueue(now);
    }

    private void reclaimExpiredJobs(long now) {
        List<UUID> expired = queueService.findExpiredProcessingJobs(now);
        for (UUID jobId : expired) {
            queueService.removeFromProcessing(jobId);
            retryCoordinator.retryOrDeadLetter(jobId);
            recoveryRecoveredCounter.increment();
        }
        if (!expired.isEmpty()) {
            log.info("Recovery pass reclaimed {} expired job(s)", expired.size());
        }
    }

    private void sweepDelayedIntoQueue(long now) {
        List<UUID> ready = queueService.findReadyDelayedJobs(now);
        for (UUID jobId : ready) {
            Job job = jobRepository.findById(jobId).orElse(null);
            if (job == null) {
                queueService.removeFromDelayed(jobId);
                continue;
            }
            queueService.promoteDelayedToQueue(jobId, job.getPriority());
        }
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdownNow();
    }
}
