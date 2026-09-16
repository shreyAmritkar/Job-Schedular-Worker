package com.example.scheduler.leader;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@ConfigurationProperties(prefix = "scheduler.leader-election")
public class LeaderElectionProperties {

    /** How long a lease lasts before Redis expires it if not renewed. Spec: 30s. */
    private Duration leaseTtl = Duration.ofSeconds(30);

    /** How often the leader renews its lease. Spec: 10s. */
    private Duration renewalInterval = Duration.ofSeconds(10);

    /** Base delay between standby acquisition attempts. */
    private Duration retryInterval = Duration.ofSeconds(5);

    /** Upper bound of random jitter added on top of retryInterval, to avoid a thundering herd of standbys hitting Redis in lockstep. */
    private Duration retryJitter = Duration.ofSeconds(2);

    public Duration getLeaseTtl() {
        return leaseTtl;
    }

    public void setLeaseTtl(Duration leaseTtl) {
        this.leaseTtl = leaseTtl;
    }

    public Duration getRenewalInterval() {
        return renewalInterval;
    }

    public void setRenewalInterval(Duration renewalInterval) {
        this.renewalInterval = renewalInterval;
    }

    public Duration getRetryInterval() {
        return retryInterval;
    }

    public void setRetryInterval(Duration retryInterval) {
        this.retryInterval = retryInterval;
    }

    public Duration getRetryJitter() {
        return retryJitter;
    }

    public void setRetryJitter(Duration retryJitter) {
        this.retryJitter = retryJitter;
    }
}
