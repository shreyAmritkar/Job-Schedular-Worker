package com.example.scheduler.execution;

import com.example.scheduler.job.Job;
import com.example.scheduler.job.JobRepository;
import com.example.scheduler.queue.JobQueueService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * The retry-or-dead-letter decision, shared by two callers:
 *  - JobRecoveryService, for jobs whose visibility timeout expired (the
 *    worker may have crashed, or may just be slow — see the race writeup)
 *  - JobExecutionCoordinator, for jobs whose executor threw synchronously
 *    (the worker is definitely still alive and knows about the failure
 *    immediately, so there's no reason to wait up to 10s for the recovery
 *    loop to notice something it could react to right now)
 *
 * Both paths converge here rather than duplicating the retry_count/backoff/
 * dead-letter logic twice.
 */
@Component
public class RetryCoordinator {

    private static final Logger log = LoggerFactory.getLogger(RetryCoordinator.class);

    private final JobRepository jobRepository;
    private final JobQueueService queueService;
    private final ExecutionProperties properties;
    private final Counter retriedCounter;
    private final Counter deadLetteredCounter;

    public RetryCoordinator(JobRepository jobRepository, JobQueueService queueService,
                             ExecutionProperties properties, MeterRegistry meterRegistry) {
        this.jobRepository = jobRepository;
        this.queueService = queueService;
        this.properties = properties;
        this.retriedCounter = meterRegistry.counter("jobs.retried");
        this.deadLetteredCounter = meterRegistry.counter("jobs.dead_lettered");
    }

    /**
     * Assumes the job has ALREADY been removed from jobs:processing by the
     * caller — this method only decides what happens next: another attempt
     * (via jobs:delayed, after backoff) or dead-letter.
     */
    @Transactional
    public void retryOrDeadLetter(UUID jobId) {
        Job job = jobRepository.findById(jobId).orElse(null);
        if (job == null) {
            log.warn("Job {} no longer exists in Postgres; nothing to retry", jobId);
            return;
        }

        jobRepository.incrementRetryCount(jobId);
        int newRetryCount = jobRepository.findById(jobId).map(Job::getRetryCount).orElse(job.getRetryCount() + 1);

        if (newRetryCount > job.getMaxRetries()) {
            jobRepository.markFailed(jobId);
            queueService.pushToDeadLetter(jobId);
            deadLetteredCounter.increment();
            log.warn("Job {} exceeded max retries ({} > {}); moved to dead-letter",
                    jobId, newRetryCount, job.getMaxRetries());
            return;
        }

        long backoffMillis = RetryPolicy.computeBackoffMillis(
                newRetryCount, properties.getInitialBackoffMillis(), properties.getMaxBackoff().toMillis());
        long readyAt = System.currentTimeMillis() + backoffMillis;
        queueService.scheduleDelayedRequeue(jobId, readyAt);
        retriedCounter.increment();
        log.info("Job {} scheduled for retry #{} in {}ms", jobId, newRetryCount, backoffMillis);
    }
}
