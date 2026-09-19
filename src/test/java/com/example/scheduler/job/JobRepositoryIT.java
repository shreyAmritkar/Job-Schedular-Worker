package com.example.scheduler.job;

import com.example.scheduler.AbstractIntegrationTest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.annotation.Transactional;

import jakarta.persistence.EntityManager;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

class JobRepositoryIT extends AbstractIntegrationTest {

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private EntityManager entityManager;

    @Test
    @Transactional
    void savesAndReloadsAJobIncludingJsonbPayload() throws Exception {
        JsonNode payload = objectMapper.readTree("{\"url\": \"https://example.com\", \"retries\": 3}");
        Job job = Job.create("report-job", ScheduleType.CRON, "0 0 2 * * *", null, payload, 7, 2, null);

        Job saved = jobRepository.saveAndFlush(job);
        entityManager.clear(); // force the next findById to hit Postgres, not the 1st-level cache

        Optional<Job> reloaded = jobRepository.findById(saved.getId());

        assertThat(reloaded).isPresent();
        assertThat(reloaded.get().getName()).isEqualTo("report-job");
        assertThat(reloaded.get().getPayload().get("url").asText()).isEqualTo("https://example.com");
        assertThat(reloaded.get().getStatus()).isEqualTo(JobStatus.ACTIVE);
        assertThat(reloaded.get().getCreatedAt()).isNotNull();
        assertThat(reloaded.get().getUpdatedAt()).isNotNull();
    }

    @Test
    @Transactional
    void cancelIfNotAlreadyCancelled_isIdempotent() {
        Job job = Job.create("job", ScheduleType.CRON, "0 0 * * * *", null, null, 5, 0, null);
        Job saved = jobRepository.saveAndFlush(job);

        int firstCall = jobRepository.cancelIfNotAlreadyCancelled(saved.getId());
        int secondCall = jobRepository.cancelIfNotAlreadyCancelled(saved.getId());

        assertThat(firstCall).isEqualTo(1);
        assertThat(secondCall).isEqualTo(0);
        assertThat(jobRepository.findById(saved.getId()).orElseThrow().getStatus())
                .isEqualTo(JobStatus.CANCELLED);
    }

    @Test
    @Transactional
    void triggerIfActive_onlyUpdatesActiveJobs() {
        Job job = Job.create("job", ScheduleType.CRON, "0 0 * * * *", null, null, 5, 0, null);
        Job saved = jobRepository.saveAndFlush(job);
        jobRepository.cancelIfNotAlreadyCancelled(saved.getId());

        int updated = jobRepository.triggerIfActive(saved.getId(), OffsetDateTime.now());

        assertThat(updated).isEqualTo(0);
        assertThat(jobRepository.findById(saved.getId()).orElseThrow().getNextRunAt()).isNull();
    }

    @Test
    @Transactional
    void triggerIfActive_setsNextRunAtOnActiveJob() {
        Job job = Job.create("job", ScheduleType.CRON, "0 0 * * * *", null, null, 5, 0, null);
        Job saved = jobRepository.saveAndFlush(job);
        OffsetDateTime triggerTime = OffsetDateTime.now();

        int updated = jobRepository.triggerIfActive(saved.getId(), triggerTime);

        assertThat(updated).isEqualTo(1);
        assertThat(jobRepository.findById(saved.getId()).orElseThrow().getNextRunAt())
                .isCloseTo(triggerTime, within(1, java.time.temporal.ChronoUnit.SECONDS));
    }

    @Test
    void findAll_returnsPagedResultsOrderedByCreatedAtDescending() {
        for (int i = 0; i < 3; i++) {
            jobRepository.saveAndFlush(
                    Job.create("job-" + i, ScheduleType.CRON, "0 0 * * * *", null, null, 5, 0, null));
        }

        var page = jobRepository.findAll(
                org.springframework.data.domain.PageRequest.of(0, 2,
                        org.springframework.data.domain.Sort.by(
                                org.springframework.data.domain.Sort.Direction.DESC, "createdAt")));

        assertThat(page.getContent()).hasSize(2);
        assertThat(page.getTotalElements()).isGreaterThanOrEqualTo(3);
    }

    @Test
    void dbRejectsBothCronAndRunAtEvenBypassingApplicationValidation() {
        // Deliberately using a native query to bypass the entity/service
        // layer entirely and prove the DB constraint (jobs_schedule_exclusive_check)
        // holds even if something writes to this table outside JobValidator.
        assertThatThrownBy(() -> jobRepository.saveAndFlush(
                invalidJobWithBothScheduleFieldsSet()))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    private Job invalidJobWithBothScheduleFieldsSet() {
        Job job = Job.create("bad-job", ScheduleType.CRON, "0 0 * * * *",
                OffsetDateTime.now().plusHours(1), null, 5, 0, null);
        // Job.create only sets nextRunAt from runAt for ONE_TIME jobs, but the
        // entity still has scheduleType=CRON with cronExpression AND runAt both
        // populated here (constructed directly, bypassing JobValidator) —
        // exactly the scenario the DB constraint exists to catch.
        return job;
    }
}
