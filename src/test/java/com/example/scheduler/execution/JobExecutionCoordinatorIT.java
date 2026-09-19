package com.example.scheduler.execution;

import com.example.scheduler.AbstractIntegrationTest;
import com.example.scheduler.job.Job;
import com.example.scheduler.job.JobExecution;
import com.example.scheduler.job.JobExecutionRepository;
import com.example.scheduler.job.JobExecutionStatus;
import com.example.scheduler.job.JobRepository;
import com.example.scheduler.job.JobStatus;
import com.example.scheduler.job.ScheduleType;
import com.example.scheduler.job.dto.JobResponse;
import com.example.scheduler.job.exception.JobConflictException;
import com.example.scheduler.job.exception.JobNotFoundException;
import com.example.scheduler.queue.JobExecutor;
import com.example.scheduler.queue.JobQueueService;
import com.example.scheduler.queue.QueuedJob;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A distinct Spring context from the rest of the IT suite (the @Primary
 * ControllableJobExecutor below changes bean wiring), started once and
 * reused across these tests. Leadership is acquired automatically by
 * LeaderElectionService before any @Test method runs — recovery.interval is
 * set to 1h in the test profile specifically so only the explicit
 * jobRecoveryService.runRecoveryPass() calls in these tests matter, not a
 * racing background timer.
 */
class JobExecutionCoordinatorIT extends AbstractIntegrationTest {

    @TestConfiguration
    static class ControllableExecutorConfig {
        @Bean
        @Primary
        ControllableJobExecutor controllableJobExecutor() {
            return new ControllableJobExecutor();
        }
    }

    @Autowired
    private JobExecutionCoordinator coordinator;

    @Autowired
    private JobRecoveryService recoveryService;

    @Autowired
    private DeadLetterService deadLetterService;

    @Autowired
    private JobQueueService queueService;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobExecutionRepository jobExecutionRepository;

    @Autowired
    private IdempotencyGuard idempotencyGuard;

    @Autowired
    private ControllableJobExecutor controllableJobExecutor;

    private static final Duration VISIBILITY_TIMEOUT = Duration.ofSeconds(2); // matches test profile

    @BeforeEach
    void resetExecutor() {
        controllableJobExecutor.reset();
    }

    // ---- 1. Normal completion ----

    @Test
    void normalCompletion_recordsSuccessAndRemovesFromProcessing() {
        Job job = persistJob(5, 3);
        QueuedJob queued = claimJob(job);

        coordinator.handle(queued);

        assertThat(queueService.processingDepth()).isEqualTo(0);
        assertThat(controllableJobExecutor.invocationCount()).isEqualTo(1);
        List<JobExecution> executions = jobExecutionRepository.findByJobId(job.getId());
        assertThat(executions).hasSize(1);
        assertThat(executions.get(0).getStatus()).isEqualTo(JobExecutionStatus.SUCCESS);
    }

    // ---- 2/3. Worker crash simulation == visibility timeout expiry ----
    // A "crash" is indistinguishable from the system's point of view from a
    // claim that's simply never completed — that's the whole premise of
    // visibility timeout. We simulate it by claiming a job and deliberately
    // never calling the coordinator at all.

    @Test
    void workerCrashSimulation_jobStaysClaimedUntilVisibilityTimeoutExpires() throws InterruptedException {
        Job job = persistJob(5, 3);
        claimJob(job); // claimed, then "the worker crashes" -- nothing more happens with it

        assertThat(queueService.processingDepth()).isEqualTo(1);

        // Immediately after claiming, it should NOT show up as expired yet.
        assertThat(queueService.findExpiredProcessingJobs(System.currentTimeMillis())).isEmpty();

        Thread.sleep(VISIBILITY_TIMEOUT.toMillis() + 200);

        assertThat(queueService.findExpiredProcessingJobs(System.currentTimeMillis())).contains(job.getId());
    }

    // ---- 4/5. Re-enqueue after backoff, exponential backoff ----

    @Test
    void recoveryPass_reclaimsExpiredJobAndSchedulesRetryWithBackoff() throws InterruptedException {
        Job job = persistJob(5, 3); // maxRetries=3, plenty of budget left
        claimJob(job);
        Thread.sleep(VISIBILITY_TIMEOUT.toMillis() + 200);

        recoveryService.runRecoveryPass();

        assertThat(queueService.processingDepth()).isEqualTo(0);
        Job reloaded = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(reloaded.getRetryCount()).isEqualTo(1);
        assertThat(reloaded.getStatus()).isEqualTo(JobStatus.ACTIVE); // not dead-lettered, retries remain

        // retryCount=1 -> backoff = min(100 * 2^1, 2000) = 200ms (test profile values)
        List<UUID> readyImmediately = queueService.findReadyDelayedJobs(System.currentTimeMillis());
        assertThat(readyImmediately).doesNotContain(job.getId()); // backoff hasn't elapsed yet

        Thread.sleep(300);
        List<UUID> readyAfterBackoff = queueService.findReadyDelayedJobs(System.currentTimeMillis());
        assertThat(readyAfterBackoff).contains(job.getId());

        // The next recovery pass promotes it back into jobs:queue.
        recoveryService.runRecoveryPass();
        assertThat(queueService.depth()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void exponentialBackoff_eachSuccessiveRetryWaitsLonger() {
        // initialBackoffMillis=100 in the test profile: retry #1 -> 200ms, retry #2 -> 400ms
        assertThat(RetryPolicy.computeBackoffMillis(1, 100, 2000)).isEqualTo(200);
        assertThat(RetryPolicy.computeBackoffMillis(2, 100, 2000)).isEqualTo(400);
    }

    // ---- 6/7. Maximum retries, dead-letter ----

    @Test
    void exceedingMaxRetries_movesJobToDeadLetterAndSetsFailedStatus() throws InterruptedException {
        Job job = persistJob(5, 0); // maxRetries=0: the very first expiry already exceeds budget
        claimJob(job);
        Thread.sleep(VISIBILITY_TIMEOUT.toMillis() + 200);

        recoveryService.runRecoveryPass();

        Job reloaded = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(reloaded.getStatus()).isEqualTo(JobStatus.FAILED);
        assertThat(queueService.listDeadLetter()).contains(job.getId());
    }

    @Test
    void deadLetterService_listsDeadLetteredJobs() throws InterruptedException {
        Job job = persistJob(5, 0);
        claimJob(job);
        Thread.sleep(VISIBILITY_TIMEOUT.toMillis() + 200);
        recoveryService.runRecoveryPass();

        List<JobResponse> deadLettered = deadLetterService.listDeadLetterJobs();

        assertThat(deadLettered).extracting(JobResponse::id).contains(job.getId());
    }

    // ---- 8. Manual DLQ requeue ----

    @Test
    void manualRequeue_resetsStatusAndRetryCountAndReturnsJobToTheQueue() throws InterruptedException {
        Job job = persistJob(5, 0);
        claimJob(job);
        Thread.sleep(VISIBILITY_TIMEOUT.toMillis() + 200);
        recoveryService.runRecoveryPass();
        assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus()).isEqualTo(JobStatus.FAILED);

        JobResponse response = deadLetterService.requeue(job.getId());

        assertThat(response.status()).isEqualTo(JobStatus.ACTIVE);
        assertThat(response.retryCount()).isEqualTo(0);
        assertThat(queueService.listDeadLetter()).doesNotContain(job.getId());
        assertThat(queueService.depth()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void manualRequeue_rejectsJobNotCurrentlyInDeadLetter() {
        Job job = persistJob(5, 3); // never dead-lettered

        assertThatThrownBy(() -> deadLetterService.requeue(job.getId()))
                .isInstanceOf(JobConflictException.class);
    }

    @Test
    void manualRequeue_rejectsUnknownJobId() {
        assertThatThrownBy(() -> deadLetterService.requeue(UUID.randomUUID()))
                .isInstanceOf(JobNotFoundException.class);
    }

    // ---- 9/10. Duplicate execution + idempotency ----

    @Test
    void duplicateDelivery_onlyTheFirstAttemptRunsTheRealSideEffect() {
        Job job = persistJob(5, 3);

        // Simulates the race from the design writeup: the SAME job gets
        // handled twice (e.g. Worker A's slow-but-alive original attempt,
        // plus Worker B's re-claim after visibility timeout expired).
        QueuedJob firstDelivery = new QueuedJob(job.getId(), 0);
        QueuedJob secondDelivery = new QueuedJob(job.getId(), 0);

        coordinator.handle(firstDelivery);
        coordinator.handle(secondDelivery);

        assertThat(controllableJobExecutor.invocationCount())
                .as("the real executor should only run once despite two deliveries")
                .isEqualTo(1);

        List<JobExecution> executions = jobExecutionRepository.findByJobId(job.getId());
        assertThat(executions).hasSize(2); // both attempts are recorded...
        long successCount = executions.stream().filter(e -> e.getStatus() == JobExecutionStatus.SUCCESS).count();
        assertThat(successCount).isEqualTo(2); // ...but both are SUCCESS: the dedup skip is treated as a no-op success
        assertThat(executions.stream().anyMatch(e ->
                e.getErrorMessage() != null && e.getErrorMessage().contains("duplicate"))).isTrue();
    }

    @Test
    void idempotencyGuard_secondClaimOfSameKeyReturnsFalse() {
        String key = UUID.randomUUID().toString();

        boolean first = idempotencyGuard.tryClaim(key);
        boolean second = idempotencyGuard.tryClaim(key);

        assertThat(first).isTrue();
        assertThat(second).isFalse();
    }

    @Test
    void idempotencyGuard_differentKeysAreIndependent() {
        boolean a = idempotencyGuard.tryClaim(UUID.randomUUID().toString());
        boolean b = idempotencyGuard.tryClaim(UUID.randomUUID().toString());

        assertThat(a).isTrue();
        assertThat(b).isTrue();
    }

    // ---- Synchronous executor failure (worker alive, job itself threw) ----
    // Distinct from the crash-simulation tests above: here the worker is
    // fine and knows about the failure immediately, so RetryCoordinator is
    // invoked synchronously rather than waiting for the recovery loop.

    @Test
    void synchronousExecutorFailure_incrementsRetryCountImmediatelyWithoutWaitingForRecovery() {
        Job job = persistJob(5, 3);
        QueuedJob queued = claimJob(job);
        controllableJobExecutor.failNextExecutionWith(new RuntimeException("simulated job failure"));

        coordinator.handle(queued);

        assertThat(queueService.processingDepth()).isEqualTo(0); // removed from processing immediately
        Job reloaded = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(reloaded.getRetryCount()).isEqualTo(1);
        List<JobExecution> executions = jobExecutionRepository.findByJobId(job.getId());
        assertThat(executions).hasSize(1);
        assertThat(executions.get(0).getStatus()).isEqualTo(JobExecutionStatus.FAILED);
        assertThat(executions.get(0).getErrorMessage()).isEqualTo("simulated job failure");
    }

    @Test
    void synchronousExecutorFailure_exceedingMaxRetriesDeadLettersImmediately() {
        Job job = persistJob(5, 0); // no retry budget at all
        QueuedJob queued = claimJob(job);
        controllableJobExecutor.failNextExecutionWith(new RuntimeException("simulated job failure"));

        coordinator.handle(queued);

        Job reloaded = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(reloaded.getStatus()).isEqualTo(JobStatus.FAILED);
        assertThat(queueService.listDeadLetter()).contains(job.getId());
    }

    // ---- helpers ----

    private Job persistJob(int priority, int maxRetries) {
        Job job = Job.create("job-" + UUID.randomUUID(), ScheduleType.CRON, "0 0 * * * *",
                null, null, priority, maxRetries, null);
        return jobRepository.saveAndFlush(job);
    }

    private QueuedJob claimJob(Job job) {
        queueService.enqueue(job.getId(), job.getPriority());
        return queueService.claim(VISIBILITY_TIMEOUT).orElseThrow();
    }

    /**
     * Controllable JobExecutor test double: counts invocations and can be
     * made to throw on demand, so failure/retry paths are testable without
     * needing LoggingJobExecutor to have any test hooks of its own.
     */
    static class ControllableJobExecutor implements JobExecutor {

        private final AtomicInteger invocations = new AtomicInteger(0);
        private final AtomicReference<RuntimeException> nextFailure = new AtomicReference<>();
        private final List<UUID> invokedJobIds = java.util.Collections.synchronizedList(new ArrayList<>());

        @Override
        public void execute(QueuedJob job) {
            invocations.incrementAndGet();
            invokedJobIds.add(job.jobId());
            RuntimeException failure = nextFailure.getAndSet(null);
            if (failure != null) {
                throw failure;
            }
        }

        void reset() {
            invocations.set(0);
            invokedJobIds.clear();
            nextFailure.set(null);
        }

        void failNextExecutionWith(RuntimeException exception) {
            nextFailure.set(exception);
        }

        int invocationCount() {
            return invocations.get();
        }
    }
}
