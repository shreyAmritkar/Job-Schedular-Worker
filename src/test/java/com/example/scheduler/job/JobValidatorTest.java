package com.example.scheduler.job;

import com.example.scheduler.job.dto.CreateJobRequest;
import com.example.scheduler.job.exception.InvalidJobScheduleException;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JobValidatorTest {

    private final JobValidator validator = new JobValidator();

    @Test
    void rejectsWhenBothCronAndRunAtSupplied() {
        CreateJobRequest request = new CreateJobRequest(
                "job", "0 0 * * * *", OffsetDateTime.now().plusHours(1), null, 5, 0, null);

        assertThatThrownBy(() -> validator.validateAndResolveScheduleType(request))
                .isInstanceOf(InvalidJobScheduleException.class)
                .hasMessageContaining("Exactly one");
    }

    @Test
    void rejectsWhenNeitherCronNorRunAtSupplied() {
        CreateJobRequest request = new CreateJobRequest("job", null, null, null, 5, 0, null);

        assertThatThrownBy(() -> validator.validateAndResolveScheduleType(request))
                .isInstanceOf(InvalidJobScheduleException.class)
                .hasMessageContaining("Exactly one");
    }

    @Test
    void rejectsBlankCronAsEquivalentToMissing() {
        CreateJobRequest request = new CreateJobRequest("job", "   ", null, null, 5, 0, null);

        assertThatThrownBy(() -> validator.validateAndResolveScheduleType(request))
                .isInstanceOf(InvalidJobScheduleException.class)
                .hasMessageContaining("Exactly one");
    }

    @Test
    void rejectsInvalidCronSyntax() {
        CreateJobRequest request = new CreateJobRequest("job", "not a cron", null, null, 5, 0, null);

        assertThatThrownBy(() -> validator.validateAndResolveScheduleType(request))
                .isInstanceOf(InvalidJobScheduleException.class)
                .hasMessageContaining("not a valid cron expression");
    }

    @Test
    void acceptsValidCronExpression() {
        CreateJobRequest request = new CreateJobRequest("job", "0 0 * * * *", null, null, 5, 0, null);

        ScheduleType type = validator.validateAndResolveScheduleType(request);

        assertThat(type).isEqualTo(ScheduleType.CRON);
    }

    @Test
    void rejectsRunAtInThePast() {
        CreateJobRequest request = new CreateJobRequest(
                "job", null, OffsetDateTime.now().minusMinutes(5), null, 5, 0, null);

        assertThatThrownBy(() -> validator.validateAndResolveScheduleType(request))
                .isInstanceOf(InvalidJobScheduleException.class)
                .hasMessageContaining("must be in the future");
    }

    @Test
    void acceptsRunAtInTheFuture() {
        CreateJobRequest request = new CreateJobRequest(
                "job", null, OffsetDateTime.now().plusMinutes(5), null, 5, 0, null);

        ScheduleType type = validator.validateAndResolveScheduleType(request);

        assertThat(type).isEqualTo(ScheduleType.ONE_TIME);
    }
}
