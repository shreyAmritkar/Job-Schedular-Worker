package com.example.scheduler.job.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.example.scheduler.job.JobStatus;
import com.example.scheduler.job.ScheduleType;

import java.time.OffsetDateTime;
import java.util.UUID;

public record JobResponse(
        UUID id,
        String name,
        ScheduleType scheduleType,
        String cronExpression,
        OffsetDateTime runAt,
        JsonNode payload,
        int priority,
        int maxRetries,
        int retryCount,
        JobStatus status,
        OffsetDateTime nextRunAt,
        UUID idempotencyKey,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt
) {
}
