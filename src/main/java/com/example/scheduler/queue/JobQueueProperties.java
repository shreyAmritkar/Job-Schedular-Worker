package com.example.scheduler.queue;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@ConfigurationProperties(prefix = "scheduler.queue")
public class JobQueueProperties {

    /** Reject new submissions once queue depth reaches this. Spec: 10,000. */
    private long backpressureThreshold = 10_000;

    /** How often the aging sweep runs. Spec: 60s. */
    private Duration agingInterval = Duration.ofSeconds(60);

    /** How long a job must have waited before aging considers it stale. Spec: 5 minutes. */
    private Duration agingThreshold = Duration.ofMinutes(5);

    /** How much to subtract from a stale job's score per aging pass — same magnitude as one full priority level. */
    private long agingBoost = 1_000_000_000L;

    public long getBackpressureThreshold() {
        return backpressureThreshold;
    }

    public void setBackpressureThreshold(long backpressureThreshold) {
        this.backpressureThreshold = backpressureThreshold;
    }

    public Duration getAgingInterval() {
        return agingInterval;
    }

    public void setAgingInterval(Duration agingInterval) {
        this.agingInterval = agingInterval;
    }

    public Duration getAgingThreshold() {
        return agingThreshold;
    }

    public void setAgingThreshold(Duration agingThreshold) {
        this.agingThreshold = agingThreshold;
    }

    public long getAgingBoost() {
        return agingBoost;
    }

    public void setAgingBoost(long agingBoost) {
        this.agingBoost = agingBoost;
    }
}
