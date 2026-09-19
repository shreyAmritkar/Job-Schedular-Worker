package com.example.scheduler.execution;

import com.example.scheduler.job.Job;
import com.example.scheduler.job.JobMapper;
import com.example.scheduler.job.JobRepository;
import com.example.scheduler.job.dto.JobResponse;
import com.example.scheduler.job.exception.JobConflictException;
import com.example.scheduler.job.exception.JobNotFoundException;
import com.example.scheduler.queue.JobQueueService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class DeadLetterService {

    private final JobQueueService queueService;
    private final JobRepository jobRepository;

    public DeadLetterService(JobQueueService queueService, JobRepository jobRepository) {
        this.queueService = queueService;
        this.jobRepository = jobRepository;
    }

    @Transactional(readOnly = true)
    public List<JobResponse> listDeadLetterJobs() {
        List<UUID> ids = queueService.listDeadLetter();
        return ids.stream()
                .map(id -> jobRepository.findById(id).orElse(null))
                .filter(job -> job != null)
                .map(JobMapper::toResponse)
                .collect(Collectors.toList());
    }

    /**
     * The only sanctioned path back to ACTIVE from FAILED: removes the job
     * from jobs:dead_letter, resets its retry budget, and puts it straight
     * back into jobs:queue at its normal priority.
     */
    @Transactional
    public JobResponse requeue(UUID jobId) {
        Job job = jobRepository.findById(jobId)
                .orElseThrow(() -> new JobNotFoundException(jobId));

        boolean removed = queueService.removeDeadLetterEntry(jobId);
        if (!removed) {
            throw new JobConflictException("Job " + jobId + " is not currently in the dead-letter queue");
        }

        jobRepository.resetForManualRequeue(jobId);
        queueService.enqueue(jobId, job.getPriority());

        return jobRepository.findById(jobId)
                .map(JobMapper::toResponse)
                .orElseThrow(() -> new JobNotFoundException(jobId));
    }
}
