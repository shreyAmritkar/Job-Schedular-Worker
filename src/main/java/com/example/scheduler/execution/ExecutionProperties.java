package com.example.scheduler.execution;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@ConfigurationProperties(prefix = "scheduler.execution")
public class ExecutionProperties {

    /** How long a worker's claim on a job is trusted before recovery treats it as abandoned. */
    private Duration visibilityTimeout = Duration.ofSeconds(30);

    /** Leader-only recovery sweep cadence. Spec: every 10 seconds. */
    private Duration recoveryInterval = Duration.ofSeconds(10);

    /** How long an idempotency key claim is remembered in Redis. */
    private Duration idempotencyKeyTtl = Duration.ofHours(24);

    /** Base of the exponential backoff formula: min(initialBackoff * 2^retryCount, maxBackoff). Spec: 1000ms. */
    private long initialBackoffMillis = 1000;

    /** Cap on backoff duration. Spec: 5 minutes. */
    private Duration maxBackoff = Duration.ofMinutes(5);

    public Duration getVisibilityTimeout() {
        return visibilityTimeout;
    }

    public void setVisibilityTimeout(Duration visibilityTimeout) {
        this.visibilityTimeout = visibilityTimeout;
    }

    public Duration getRecoveryInterval() {
        return recoveryInterval;
    }

    public void setRecoveryInterval(Duration recoveryInterval) {
        this.recoveryInterval = recoveryInterval;
    }

    public Duration getIdempotencyKeyTtl() {
        return idempotencyKeyTtl;
    }

    public void setIdempotencyKeyTtl(Duration idempotencyKeyTtl) {
        this.idempotencyKeyTtl = idempotencyKeyTtl;
    }

    public long getInitialBackoffMillis() {
        return initialBackoffMillis;
    }

    public void setInitialBackoffMillis(long initialBackoffMillis) {
        this.initialBackoffMillis = initialBackoffMillis;
    }

    public Duration getMaxBackoff() {
        return maxBackoff;
    }

    public void setMaxBackoff(Duration maxBackoff) {
        this.maxBackoff = maxBackoff;
    }
}
