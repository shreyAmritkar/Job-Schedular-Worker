package com.example.scheduler.job.dto;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Size;

import java.time.OffsetDateTime;

/**
 * Structural/transport validation only (blank checks, ranges, sizes) lives
 * here as bean validation annotations. Anything requiring domain knowledge —
 * "exactly one of cronExpression/runAt", cron syntax correctness, runAt
 * being in the future — is deliberately NOT here. That's JobValidator's job,
 * in the service layer, so it's framework-independent and unit-testable
 * with plain Mockito rather than a full web-layer test.
 */
public record CreateJobRequest(

        @NotBlank(message = "name must not be blank")
        @Size(max = 255, message = "name must be at most 255 characters")
        String name,

        String cronExpression,

        OffsetDateTime runAt,

        JsonNode payload,

        @NotNull(message = "priority is required")
        @Min(value = 1, message = "priority must be between 1 and 10")
        @Max(value = 10, message = "priority must be between 1 and 10")
        Integer priority,

        @NotNull(message = "maxRetries is required")
        @PositiveOrZero(message = "maxRetries must be zero or greater")
        Integer maxRetries
) {
}
