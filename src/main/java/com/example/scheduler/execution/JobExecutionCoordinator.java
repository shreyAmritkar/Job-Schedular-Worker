package com.example.scheduler.execution;

import com.example.scheduler.job.Job;
import com.example.scheduler.job.JobExecution;
import com.example.scheduler.job.JobExecutionRepository;
import com.example.scheduler.job.JobRepository;
import com.example.scheduler.leader.InstanceIdProvider;
import com.example.scheduler.queue.JobExecutor;
import com.example.scheduler.queue.JobQueueService;
import com.example.scheduler.queue.QueuedJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Everything that happens to a job between "a worker claimed it" and
 * "it's done, one way or another": look up its details, deduplicate via
 * idempotency key, execute, record the attempt in job_executions, then
 * either complete (remove from jobs:processing) or hand off to
 * RetryCoordinator on failure.
 */
@Component
public class JobExecutionCoordinator implements ClaimedJobHandler {

    private static final Logger log = LoggerFactory.getLogger(JobExecutionCoordinator.class);

    private final JobRepository jobRepository;
    private final JobExecutionRepository jobExecutionRepository;
    private final JobQueueService queueService;
    private final IdempotencyGuard idempotencyGuard;
    private final JobExecutor jobExecutor;
    private final RetryCoordinator retryCoordinator;
    private final String instanceId;

    public JobExecutionCoordinator(JobRepository jobRepository, JobExecutionRepository jobExecutionRepository,
                                    JobQueueService queueService, IdempotencyGuard idempotencyGuard,
                                    JobExecutor jobExecutor, RetryCoordinator retryCoordinator,
                                    InstanceIdProvider instanceIdProvider) {
        this.jobRepository = jobRepository;
        this.jobExecutionRepository = jobExecutionRepository;
        this.queueService = queueService;
        this.idempotencyGuard = idempotencyGuard;
        this.jobExecutor = jobExecutor;
        this.retryCoordinator = retryCoordinator;
        this.instanceId = instanceIdProvider.getInstanceId();
    }

    @Override
    public void handle(QueuedJob queuedJob) {
        UUID jobId = queuedJob.jobId();
        Job job = jobRepository.findById(jobId).orElse(null);
        if (job == null) {
            // The job row is gone entirely (shouldn't normally happen — jobs
            // are only soft-deleted). Nothing sensible to execute; drop the
            // claim so it doesn't sit in jobs:processing until visibility
            // timeout expires for no reason.
            log.warn("Claimed job {} has no corresponding row in Postgres; dropping claim", jobId);
            queueService.removeFromProcessing(jobId);
            return;
        }

        JobExecution execution = JobExecution.start(jobId, instanceId);
        jobExecutionRepository.save(execution);

        boolean isOriginalAttempt = idempotencyGuard.tryClaim(job.getIdempotencyKey().toString());

        try {
            if (isOriginalAttempt) {
                jobExecutor.execute(queuedJob);
                execution.succeed(null);
            } else {
                log.info("Skipping duplicate execution of job {} — idempotency key {} already processed",
                        jobId, job.getIdempotencyKey());
                execution.succeed("Skipped: duplicate execution (idempotency key already processed)");
            }
            jobExecutionRepository.save(execution);
            queueService.completeJob(jobId);
        } catch (RuntimeException e) {
            execution.fail(e.getMessage());
            jobExecutionRepository.save(execution);
            queueService.removeFromProcessing(jobId);
            retryCoordinator.retryOrDeadLetter(jobId);
        }
    }
}
