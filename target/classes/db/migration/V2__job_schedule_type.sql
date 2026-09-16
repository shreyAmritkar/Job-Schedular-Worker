-- Stage 1: support cron OR one-time (run_at) scheduling, CANCELLED status,
-- and DB-level guardrails matching Stage 1 application-layer validation.

ALTER TABLE jobs
    ADD COLUMN schedule_type VARCHAR(20) NOT NULL DEFAULT 'CRON';

ALTER TABLE jobs
    ADD CONSTRAINT jobs_schedule_type_check
        CHECK (schedule_type IN ('CRON', 'ONE_TIME'));

ALTER TABLE jobs
    ALTER COLUMN cron_expression DROP NOT NULL;

ALTER TABLE jobs
    ADD COLUMN run_at TIMESTAMPTZ;

-- Defense in depth: enforce "exactly one scheduling mechanism" at the DB
-- level too, not just in the service layer.
ALTER TABLE jobs
    ADD CONSTRAINT jobs_schedule_exclusive_check CHECK (
        (schedule_type = 'CRON' AND cron_expression IS NOT NULL AND run_at IS NULL)
        OR
        (schedule_type = 'ONE_TIME' AND run_at IS NOT NULL AND cron_expression IS NULL)
    );

-- Replace the Stage 0 status check to allow soft-deleted (cancelled) jobs.
ALTER TABLE jobs
    DROP CONSTRAINT jobs_status_check;

ALTER TABLE jobs
    ADD CONSTRAINT jobs_status_check
        CHECK (status IN ('ACTIVE', 'PAUSED', 'DISABLED', 'CANCELLED'));

ALTER TABLE jobs
    ALTER COLUMN priority SET DEFAULT 5;

ALTER TABLE jobs
    ADD CONSTRAINT jobs_priority_range_check
        CHECK (priority BETWEEN 1 AND 10);

ALTER TABLE jobs
    ADD CONSTRAINT jobs_max_retries_nonnegative_check
        CHECK (max_retries >= 0);
