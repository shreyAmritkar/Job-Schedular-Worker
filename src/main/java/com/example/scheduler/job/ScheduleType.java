package com.example.scheduler.job;

/**
 * How a job's execution time is determined. Exactly one of the two
 * corresponding fields (cronExpression / runAt) is populated, enforced both
 * by JobValidator and by a DB check constraint (jobs_schedule_exclusive_check).
 */
public enum ScheduleType {
    CRON,
    ONE_TIME
}
