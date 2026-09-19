package com.example.scheduler.job;

import com.example.scheduler.common.PageResponse;
import com.example.scheduler.job.dto.CreateJobRequest;
import com.example.scheduler.job.dto.JobResponse;
import com.example.scheduler.job.exception.JobConflictException;
import com.example.scheduler.job.exception.JobNotFoundException;
import com.example.scheduler.queue.JobQueueService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JobServiceTest {

    @Mock
    private JobRepository jobRepository;

    @Mock
    private JobValidator jobValidator;

    @Mock
    private JobQueueService jobQueueService;

    @InjectMocks
    private JobService jobService;

    @Captor
    private ArgumentCaptor<Job> jobCaptor;

    private UUID jobId;

    @BeforeEach
    void setUp() {
        jobId = UUID.randomUUID();
    }

    @Test
    void createJob_persistsValidatedJob() {
        CreateJobRequest request = new CreateJobRequest(
                "nightly-report", "0 0 2 * * *", null, null, 5, 3);
        when(jobValidator.validateAndResolveScheduleType(request)).thenReturn(ScheduleType.CRON);
        when(jobRepository.save(any(Job.class))).thenAnswer(invocation -> invocation.getArgument(0));

        JobResponse response = jobService.createJob(request);

        verify(jobValidator).validateAndResolveScheduleType(request);
        verify(jobRepository).save(jobCaptor.capture());
        Job saved = jobCaptor.getValue();
        assertThat(saved.getName()).isEqualTo("nightly-report");
        assertThat(saved.getScheduleType()).isEqualTo(ScheduleType.CRON);
        assertThat(saved.getStatus()).isEqualTo(JobStatus.ACTIVE);
        assertThat(response.name()).isEqualTo("nightly-report");
    }

    @Test
    void createJob_doesNotPersistWhenValidationFails() {
        CreateJobRequest request = new CreateJobRequest("bad-job", null, null, null, 5, 0);
        when(jobValidator.validateAndResolveScheduleType(request))
                .thenThrow(new com.example.scheduler.job.exception.InvalidJobScheduleException("bad"));

        assertThatThrownBy(() -> jobService.createJob(request))
                .isInstanceOf(com.example.scheduler.job.exception.InvalidJobScheduleException.class);

        verify(jobRepository, never()).save(any());
    }

    @Test
    void getJob_returnsMappedResponseWhenFound() {
        Job job = Job.create("job", ScheduleType.CRON, "0 0 * * * *", null, null, 5, 0);
        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));

        // Job.create doesn't set an id (that's Hibernate's job on persist), so
        // for this test we only assert on fields that don't depend on it.
        JobResponse response = jobService.getJob(jobId);

        assertThat(response.name()).isEqualTo("job");
    }

    @Test
    void getJob_throwsNotFoundWhenMissing() {
        when(jobRepository.findById(jobId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> jobService.getJob(jobId))
                .isInstanceOf(JobNotFoundException.class);
    }

    @Test
    void cancelJob_updatesWhenJobExists() {
        when(jobRepository.existsById(jobId)).thenReturn(true);

        jobService.cancelJob(jobId);

        verify(jobRepository).cancelIfNotAlreadyCancelled(jobId);
    }

    @Test
    void cancelJob_throwsNotFoundWhenMissing() {
        when(jobRepository.existsById(jobId)).thenReturn(false);

        assertThatThrownBy(() -> jobService.cancelJob(jobId))
                .isInstanceOf(JobNotFoundException.class);

        verify(jobRepository, never()).cancelIfNotAlreadyCancelled(any());
    }

    @Test
    void triggerJob_succeedsAndReturnsUpdatedJobWhenActive() {
        when(jobRepository.triggerIfActive(eq(jobId), any(OffsetDateTime.class))).thenReturn(1);
        Job job = Job.create("job", ScheduleType.CRON, "0 0 * * * *", null, null, 5, 0);
        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));

        JobResponse response = jobService.triggerJob(jobId);

        assertThat(response.name()).isEqualTo("job");
    }

    @Test
    void triggerJob_throwsConflictWhenJobNotActive() {
        when(jobRepository.triggerIfActive(eq(jobId), any(OffsetDateTime.class))).thenReturn(0);
        Job cancelledJob = Job.create("job", ScheduleType.CRON, "0 0 * * * *", null, null, 5, 0);
        when(jobRepository.findById(jobId)).thenReturn(Optional.of(cancelledJob));

        assertThatThrownBy(() -> jobService.triggerJob(jobId))
                .isInstanceOf(JobConflictException.class);
    }

    @Test
    void triggerJob_throwsNotFoundWhenJobDoesNotExist() {
        when(jobRepository.triggerIfActive(eq(jobId), any(OffsetDateTime.class))).thenReturn(0);
        when(jobRepository.findById(jobId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> jobService.triggerJob(jobId))
                .isInstanceOf(JobNotFoundException.class);
    }

    @Test
    void listJobs_clampsPageSizeToMaximum() {
        when(jobRepository.findAll(any(Pageable.class)))
                .thenReturn(new PageImpl<>(List.of()));

        jobService.listJobs(0, 10_000);

        org.mockito.ArgumentCaptor<Pageable> pageableCaptor = org.mockito.ArgumentCaptor.forClass(Pageable.class);
        verify(jobRepository).findAll(pageableCaptor.capture());
        assertThat(pageableCaptor.getValue().getPageSize()).isEqualTo(100);
    }

    @Test
    void listJobs_clampsNegativePageToZero() {
        when(jobRepository.findAll(any(Pageable.class)))
                .thenReturn(new PageImpl<>(List.of(), PageRequest.of(0, 20), 0));

        jobService.listJobs(-5, 20);

        verify(jobRepository).findAll(argThatPageIsZero());
    }

    private Pageable argThatPageIsZero() {
        return org.mockito.ArgumentMatchers.argThat(p -> p.getPageNumber() == 0);
    }

    @Test
    void enqueueJob_delegatesToQueueServiceWhenJobIsActive() {
        Job job = Job.create("job", ScheduleType.CRON, "0 0 * * * *", null, null, 5, 0);
        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));

        jobService.enqueueJob(jobId);

        verify(jobQueueService).enqueue(job.getId(), 5);
    }

    @Test
    void enqueueJob_throwsNotFoundWhenJobDoesNotExist() {
        when(jobRepository.findById(jobId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> jobService.enqueueJob(jobId))
                .isInstanceOf(JobNotFoundException.class);

        verify(jobQueueService, never()).enqueue(any(), org.mockito.ArgumentMatchers.anyInt());
    }

    @Test
    void enqueueJob_throwsConflictWhenJobIsCancelled() {
        Job cancelledJob = cancelledJob();
        when(jobRepository.findById(jobId)).thenReturn(Optional.of(cancelledJob));

        assertThatThrownBy(() -> jobService.enqueueJob(jobId))
                .isInstanceOf(JobConflictException.class);

        verify(jobQueueService, never()).enqueue(any(), org.mockito.ArgumentMatchers.anyInt());
    }

    /**
     * Job has no public setter for status (by design — see Job.java); the
     * only production path to CANCELLED is the repository's atomic
     * @Modifying update query, which doesn't mutate a Java object. For this
     * unit test we just need *a* Job instance whose status is CANCELLED,
     * so a small reflection helper is the honest option here rather than
     * adding a test-only setter to production code.
     */
    private Job cancelledJob() {
        Job job = Job.create("job", ScheduleType.CRON, "0 0 * * * *", null, null, 5, 0);
        try {
            var statusField = Job.class.getDeclaredField("status");
            statusField.setAccessible(true);
            statusField.set(job, JobStatus.CANCELLED);
            return job;
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }
}
