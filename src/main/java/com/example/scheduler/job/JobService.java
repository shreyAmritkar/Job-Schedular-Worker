package com.example.scheduler.job;

import com.example.scheduler.common.PageResponse;
import com.example.scheduler.job.dto.CreateJobRequest;
import com.example.scheduler.job.dto.JobResponse;
import com.example.scheduler.job.exception.JobConflictException;
import com.example.scheduler.job.exception.JobNotFoundException;
import com.example.scheduler.queue.JobQueueService;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.UUID;

@Service
public class JobService {

    private static final int MAX_PAGE_SIZE = 100;

    private final JobRepository jobRepository;
    private final JobValidator jobValidator;
    private final JobQueueService jobQueueService;

    public JobService(JobRepository jobRepository, JobValidator jobValidator, JobQueueService jobQueueService) {
        this.jobRepository = jobRepository;
        this.jobValidator = jobValidator;
        this.jobQueueService = jobQueueService;
    }

    @Transactional
    public JobResponse createJob(CreateJobRequest request) {
        ScheduleType scheduleType = jobValidator.validateAndResolveScheduleType(request);

        Job job = Job.create(
                request.name(),
                scheduleType,
                request.cronExpression(),
                request.runAt(),
                request.payload(),
                request.priority(),
                request.maxRetries(),
                request.idempotencyKey()
        );

        Job saved = jobRepository.save(job);
        return JobMapper.toResponse(saved);
    }

    @Transactional(readOnly = true)
    public PageResponse<JobResponse> listJobs(int page, int size) {
        int safePage = Math.max(page, 0);
        int safeSize = Math.min(Math.max(size, 1), MAX_PAGE_SIZE);

        Pageable pageable = PageRequest.of(safePage, safeSize, Sort.by(Sort.Direction.DESC, "createdAt"));
        Page<Job> jobs = jobRepository.findAll(pageable);

        return PageResponse.from(jobs.map(JobMapper::toResponse));
    }

    @Transactional(readOnly = true)
    public JobResponse getJob(UUID id) {
        return jobRepository.findById(id)
                .map(JobMapper::toResponse)
                .orElseThrow(() -> new JobNotFoundException(id));
    }

    /**
     * Soft delete. Idempotent by design: cancelling an already-cancelled
     * job is a no-op, not an error — DELETE should be safe to retry.
     */
    @Transactional
    public void cancelJob(UUID id) {
        if (!jobRepository.existsById(id)) {
            throw new JobNotFoundException(id);
        }
        jobRepository.cancelIfNotAlreadyCancelled(id);
    }

    /**
     * Marks a job as manually triggered by advancing next_run_at to now.
     * Does NOT execute anything — Stage 2+ scheduling logic is what reads
     * next_run_at and actually does something with it.
     *
     * The atomic UPDATE runs first; only if it affects 0 rows do we do a
     * second read to figure out whether that's a 404 or a 409. This avoids
     * a check-then-act race between reading the job's status and updating it.
     */
    @Transactional
    public JobResponse triggerJob(UUID id) {
        OffsetDateTime triggeredAt = OffsetDateTime.now();
        int updated = jobRepository.triggerIfActive(id, triggeredAt);

        if (updated == 0) {
            Job job = jobRepository.findById(id)
                    .orElseThrow(() -> new JobNotFoundException(id));
            throw new JobConflictException(
                    "Cannot trigger job in status " + job.getStatus() + "; only ACTIVE jobs can be triggered");
        }

        Job job = jobRepository.findById(id)
                .orElseThrow(() -> new JobNotFoundException(id));
        return JobMapper.toResponse(job);
    }

    /**
     * Pushes an existing job into the Redis execution queue. Only ACTIVE
     * jobs are eligible — same reasoning as triggerJob: a paused/disabled/
     * cancelled job shouldn't enter the execution pipeline just because
     * someone calls this endpoint. Backpressure (queue depth over
     * threshold) is enforced inside JobQueueService and surfaces as
     * QueueBackpressureException -> 429, not re-implemented here.
     *
     * Deliberately does NOT change the job's row in Postgres — Stage 3
     * keeps "job definitions live in Postgres" and "the execution queue
     * lives in Redis" as separate concerns; tracking that a job was queued
     * is an execution-tracking feature for a later stage.
     */
    @Transactional(readOnly = true)
    public JobResponse enqueueJob(UUID id) {
        Job job = jobRepository.findById(id)
                .orElseThrow(() -> new JobNotFoundException(id));

        if (job.getStatus() != JobStatus.ACTIVE) {
            throw new JobConflictException(
                    "Cannot enqueue job in status " + job.getStatus() + "; only ACTIVE jobs can be enqueued");
        }

        jobQueueService.enqueue(job.getId(), job.getPriority());
        return JobMapper.toResponse(job);
    }
}
