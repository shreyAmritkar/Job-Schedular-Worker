package com.example.scheduler.leader.dto;

import java.time.Instant;

public record SchedulerStatusResponse(
        String instanceId,
        boolean isLeader,
        Instant lastRenewalAt
) {
}
