-- Stage 4: every job needs a stable idempotency key so executors can
-- deduplicate side effects when the same job is delivered more than once
-- (at-least-once delivery is the whole point of visibility timeout).

ALTER TABLE jobs
    ADD COLUMN idempotency_key UUID NOT NULL DEFAULT gen_random_uuid();

ALTER TABLE jobs
    ADD CONSTRAINT jobs_idempotency_key_unique UNIQUE (idempotency_key);
