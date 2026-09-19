package com.example.scheduler.job;

import com.example.scheduler.AbstractIntegrationTest;
import com.example.scheduler.common.exception.ApiError;
import com.example.scheduler.job.dto.CreateJobRequest;
import com.example.scheduler.job.dto.JobResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.boot.test.web.client.TestRestTemplate;

import java.time.OffsetDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class JobControllerIT extends AbstractIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void createJob_returns201WithLocationAndBody() {
        CreateJobRequest request = new CreateJobRequest(
                "nightly-backup", "0 0 3 * * *", null, null, 8, 3 , null);

        ResponseEntity<JobResponse> response = restTemplate.postForEntity("/api/jobs", request, JobResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getHeaders().getLocation()).isNotNull();
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().name()).isEqualTo("nightly-backup");
        assertThat(response.getBody().status()).isEqualTo(JobStatus.ACTIVE);
        assertThat(response.getBody().scheduleType()).isEqualTo(ScheduleType.CRON);
    }

    @Test
    void createJob_rejectsBothCronAndRunAtWith400() {
        CreateJobRequest request = new CreateJobRequest(
                "bad-job", "0 0 3 * * *", OffsetDateTime.now().plusHours(1), null, 5, 0 , null);

        ResponseEntity<ApiError> response = restTemplate.postForEntity("/api/jobs", request, ApiError.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void createJob_rejectsPriorityOutOfRangeWith400() {
        CreateJobRequest request = new CreateJobRequest("bad-job", "0 0 3 * * *", null, null, 99, 0 , null);

        ResponseEntity<ApiError> response = restTemplate.postForEntity("/api/jobs", request, ApiError.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().fieldErrors()).containsKey("priority");
    }

    @Test
    void getJob_returns404ForUnknownId() {
        ResponseEntity<ApiError> response = restTemplate.getForEntity(
                "/api/jobs/" + UUID.randomUUID(), ApiError.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void fullLifecycle_createGetCancelTrigger() {
        CreateJobRequest createRequest = new CreateJobRequest(
                "lifecycle-job", "0 0 4 * * *", null, null, 5, 1 , null);
        JobResponse created = restTemplate.postForEntity("/api/jobs", createRequest, JobResponse.class).getBody();
        assertThat(created).isNotNull();

        ResponseEntity<JobResponse> getResponse = restTemplate.getForEntity(
                "/api/jobs/" + created.id(), JobResponse.class);
        assertThat(getResponse.getStatusCode()).isEqualTo(HttpStatus.OK);

        ResponseEntity<JobResponse> triggerResponse = restTemplate.postForEntity(
                "/api/jobs/" + created.id() + "/trigger", null, JobResponse.class);
        assertThat(triggerResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(triggerResponse.getBody()).isNotNull();
        assertThat(triggerResponse.getBody().nextRunAt()).isNotNull();

        ResponseEntity<Void> deleteResponse = restTemplate.exchange(
                "/api/jobs/" + created.id(), HttpMethod.DELETE, null, Void.class);
        assertThat(deleteResponse.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);

        ResponseEntity<JobResponse> afterCancel = restTemplate.getForEntity(
                "/api/jobs/" + created.id(), JobResponse.class);
        assertThat(afterCancel.getBody()).isNotNull();
        assertThat(afterCancel.getBody().status()).isEqualTo(JobStatus.CANCELLED);

        // A cancelled job can no longer be triggered.
        ResponseEntity<ApiError> triggerAfterCancel = restTemplate.postForEntity(
                "/api/jobs/" + created.id() + "/trigger", null, ApiError.class);
        assertThat(triggerAfterCancel.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);

        // Deleting an already-cancelled job is idempotent, not an error.
        ResponseEntity<Void> secondDelete = restTemplate.exchange(
                "/api/jobs/" + created.id(), HttpMethod.DELETE, null, Void.class);
        assertThat(secondDelete.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
    }

    @Test
    void listJobs_returnsPaginatedResults() {
        for (int i = 0; i < 3; i++) {
            restTemplate.postForEntity("/api/jobs",
                    new CreateJobRequest("job-" + i, "0 0 * * * *", null, null, 5, 0 , null),
                    JobResponse.class);
        }

        ResponseEntity<Object> response = restTemplate.getForEntity("/api/jobs?page=0&size=2", Object.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }
}
