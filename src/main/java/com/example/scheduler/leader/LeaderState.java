package com.example.scheduler.leader;

import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Mutators are package-private: only LeaderElectionService (same package)
 * may change state. Everyone else — the controller, tests reading status —
 * only gets read access. volatile fields are sufficient here (not
 * AtomicBoolean/AtomicReference): every write happens on the single
 * leader-election executor thread, so there's no read-modify-write race to
 * protect against, only the need for other threads (the HTTP thread serving
 * /api/scheduler/status) to see the latest value — which volatile guarantees.
 */
@Component
public class LeaderState {

    private final String instanceId;
    private volatile boolean leader = false;
    private volatile Instant lastRenewalAt;

    public LeaderState(InstanceIdProvider instanceIdProvider) {
        this.instanceId = instanceIdProvider.getInstanceId();
    }

    void markLeader(Instant at) {
        this.leader = true;
        this.lastRenewalAt = at;
    }

    void markRenewed(Instant at) {
        this.lastRenewalAt = at;
    }

    void markStandby() {
        this.leader = false;
        // lastRenewalAt is deliberately NOT cleared: it's useful observability
        // ("when did this instance last actually hold the lease") even after
        // stepping down, rather than snapping back to null.
    }

    public String getInstanceId() {
        return instanceId;
    }

    public boolean isLeader() {
        return leader;
    }

    public Instant getLastRenewalAt() {
        return lastRenewalAt;
    }
}
