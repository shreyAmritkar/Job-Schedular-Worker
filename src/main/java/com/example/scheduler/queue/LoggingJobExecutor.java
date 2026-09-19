package com.example.scheduler.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class LoggingJobExecutor implements JobExecutor {

    private static final Logger log = LoggerFactory.getLogger(LoggingJobExecutor.class);

    @Override
    public void execute(QueuedJob job) {
        log.info("Executing job {} (dequeued with score {})", job.jobId(), job.score());
        // Stage 3 stops here on purpose: no real execution, no retries, no
        // visibility timeout, no per-job distributed locking. Those are
        // later-stage concerns.
    }
}
