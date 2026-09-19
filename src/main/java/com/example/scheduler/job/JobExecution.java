package com.example.scheduler.job;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.UuidGenerator;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * A single attempt at executing a job. Deliberately a plain jobId column,
 * not a @ManyToOne to Job — nothing here needs to navigate back to the full
 * Job entity, and a foreign key column is enough to answer "which job was
 * this an attempt at."
 */
@Entity
@Table(name = "job_executions")
public class JobExecution {

    @Id
    @UuidGenerator
    @Column(name = "id", updatable = false, nullable = false)
    private UUID id;

    @Column(name = "job_id", nullable = false)
    private UUID jobId;

    @Column(name = "worker_id")
    private String workerId;

    @Column(name = "started_at")
    private OffsetDateTime startedAt;

    @Column(name = "finished_at")
    private OffsetDateTime finishedAt;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private JobExecutionStatus status;

    @Column(name = "error_message")
    private String errorMessage;

    protected JobExecution() {
        // required by JPA
    }

    public static JobExecution start(UUID jobId, String workerId) {
        JobExecution execution = new JobExecution();
        execution.jobId = jobId;
        execution.workerId = workerId;
        execution.startedAt = OffsetDateTime.now();
        execution.status = JobExecutionStatus.RUNNING;
        return execution;
    }

    /**
     * note is non-null only for the "this was a duplicate delivery, skipped
     * via the idempotency key" case — repurposing error_message as a
     * diagnostic note here rather than adding a schema column solely for
     * that one niche case.
     */
    public void succeed(String note) {
        this.status = JobExecutionStatus.SUCCESS;
        this.finishedAt = OffsetDateTime.now();
        this.errorMessage = note;
    }

    public void fail(String errorMessage) {
        this.status = JobExecutionStatus.FAILED;
        this.finishedAt = OffsetDateTime.now();
        this.errorMessage = errorMessage;
    }

    public UUID getId() {
        return id;
    }

    public UUID getJobId() {
        return jobId;
    }

    public String getWorkerId() {
        return workerId;
    }

    public OffsetDateTime getStartedAt() {
        return startedAt;
    }

    public OffsetDateTime getFinishedAt() {
        return finishedAt;
    }

    public JobExecutionStatus getStatus() {
        return status;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
