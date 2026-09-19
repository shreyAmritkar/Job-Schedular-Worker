-- Stage 4: a job that exhausts its retry budget and is moved to the
-- dead-letter queue needs a terminal status distinct from ACTIVE, so it
-- can't be silently re-submitted through the normal /enqueue path.

ALTER TABLE jobs
    DROP CONSTRAINT jobs_status_check;

ALTER TABLE jobs
    ADD CONSTRAINT jobs_status_check
        CHECK (status IN ('ACTIVE', 'PAUSED', 'DISABLED', 'CANCELLED', 'FAILED'));
