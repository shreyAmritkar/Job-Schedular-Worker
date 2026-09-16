package com.example.scheduler.job;

import com.example.scheduler.job.dto.CreateJobRequest;
import com.example.scheduler.job.exception.InvalidJobScheduleException;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.time.OffsetDateTime;

/**
 * Validates domain rules that a bean-validation annotation on a DTO can't
 * express cleanly: cross-field exclusivity, cron grammar correctness, and
 * "is this timestamp actually usable". Kept out of the web layer so it's
 * testable in plain JUnit/Mockito without a Spring context.
 *
 * Note on CronExpression: this uses
 * org.springframework.scheduling.support.CronExpression#isValidExpression,
 * which is a pure syntax-validation utility already on the classpath via
 * spring-context (a transitive dependency of spring-boot-starter-web) — no
 * new dependency added. Nothing here registers a scheduled task; it's used
 * exactly like a regex validator would be.
 */
@Component
public class JobValidator {

    public ScheduleType validateAndResolveScheduleType(CreateJobRequest request) {
        boolean hasCron = StringUtils.hasText(request.cronExpression());
        boolean hasRunAt = request.runAt() != null;

        if (hasCron == hasRunAt) {
            throw new InvalidJobScheduleException(
                    "Exactly one of cronExpression or runAt must be supplied");
        }

        if (hasCron) {
            if (!CronExpression.isValidExpression(request.cronExpression())) {
                throw new InvalidJobScheduleException(
                        "cronExpression is not a valid cron expression: " + request.cronExpression());
            }
            return ScheduleType.CRON;
        }

        if (!request.runAt().isAfter(OffsetDateTime.now())) {
            throw new InvalidJobScheduleException("runAt must be in the future");
        }
        return ScheduleType.ONE_TIME;
    }
}
