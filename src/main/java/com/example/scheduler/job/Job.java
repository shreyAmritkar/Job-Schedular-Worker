package com.example.scheduler.job;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.annotations.UuidGenerator;
import org.hibernate.type.SqlTypes;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "jobs")
public class Job {

    @Id
    @UuidGenerator
    @Column(name = "id", updatable = false, nullable = false)
    private UUID id;

    @Column(name = "name", nullable = false)
    private String name;

    @Enumerated(EnumType.STRING)
    @Column(name = "schedule_type", nullable = false)
    private ScheduleType scheduleType;

    @Column(name = "cron_expression")
    private String cronExpression;

    @Column(name = "run_at")
    private OffsetDateTime runAt;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", columnDefinition = "jsonb")
    private JsonNode payload;

    @Column(name = "priority", nullable = false)
    private int priority;

    @Column(name = "max_retries", nullable = false)
    private int maxRetries;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private JobStatus status;

    @Column(name = "next_run_at")
    private OffsetDateTime nextRunAt;

    @Column(name = "idempotency_key", nullable = false, updatable = false)
    private UUID idempotencyKey;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    protected Job() {
        // required by JPA
    }

    /**
     * Factory instead of a public no-arg constructor + setters: a Job is
     * either fully-formed and valid, or it doesn't exist. Callers (the
     * service layer) must go through JobValidator first — this method
     * doesn't re-validate, it just assembles what's already been checked.
     */
    public static Job create(String name, ScheduleType scheduleType, String cronExpression,
                              OffsetDateTime runAt, JsonNode payload, int priority, int maxRetries,
                              UUID idempotencyKey) {
        Job job = new Job();
        job.name = name;
        job.scheduleType = scheduleType;
        job.cronExpression = cronExpression;
        job.runAt = runAt;
        job.payload = payload;
        job.priority = priority;
        job.maxRetries = maxRetries;
        job.retryCount = 0;
        job.status = JobStatus.ACTIVE;
        job.idempotencyKey = idempotencyKey != null ? idempotencyKey : UUID.randomUUID();
        // For CRON jobs, the next fire time is computed by the Stage 2+
        // scheduler, which doesn't exist yet — left null deliberately.
        // For ONE_TIME jobs, we already know exactly when it should run.
        job.nextRunAt = scheduleType == ScheduleType.ONE_TIME ? runAt : null;
        return job;
    }

    public UUID getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public ScheduleType getScheduleType() {
        return scheduleType;
    }

    public String getCronExpression() {
        return cronExpression;
    }

    public OffsetDateTime getRunAt() {
        return runAt;
    }

    public JsonNode getPayload() {
        return payload;
    }

    public int getPriority() {
        return priority;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public JobStatus getStatus() {
        return status;
    }

    public OffsetDateTime getNextRunAt() {
        return nextRunAt;
    }

    public UUID getIdempotencyKey() {
        return idempotencyKey;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    public OffsetDateTime getUpdatedAt() {
        return updatedAt;
    }
}
