package com.example.scheduler.job;

/**
 * Lifecycle status of a job definition. Distinct from a job *execution's*
 * status (which will live on job_executions once Stage 2+ introduces it).
 */
public enum JobStatus {
    ACTIVE,
    PAUSED,
    DISABLED,
    CANCELLED
}
