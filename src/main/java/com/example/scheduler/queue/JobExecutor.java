package com.example.scheduler.queue;

/**
 * What a worker does with a job it pulled off the queue. Stage 3
 * deliberately has no real execution logic yet — no visibility timeout, no
 * per-job locking, no retries. LoggingJobExecutor is the only
 * implementation for now; it exists to prove the worker pool -> executor
 * wiring works end to end.
 */
public interface JobExecutor {

    void execute(QueuedJob job);
}
