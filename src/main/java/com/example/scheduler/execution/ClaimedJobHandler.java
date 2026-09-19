package com.example.scheduler.execution;

import com.example.scheduler.queue.QueuedJob;

/**
 * What WorkerPool calls once it has claimed a job. JobExecutionCoordinator
 * is the production implementation (idempotency check, execution, DB
 * recording, retry-on-failure). Kept as a separate interface rather than
 * having WorkerPool depend on JobExecutionCoordinator directly, so Stage 3's
 * pure-Redis concurrency tests (no Postgres, no idempotency machinery) can
 * keep using a trivial test double instead of paying for the full stack.
 */
public interface ClaimedJobHandler {

    void handle(QueuedJob job);
}
