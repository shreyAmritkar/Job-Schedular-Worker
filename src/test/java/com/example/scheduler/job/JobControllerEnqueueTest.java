package com.example.scheduler.job;

import com.example.scheduler.job.dto.JobResponse;
import com.example.scheduler.job.exception.JobConflictException;
import com.example.scheduler.job.exception.JobNotFoundException;
import com.example.scheduler.queue.exception.QueueBackpressureException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.UUID;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Tests the HTTP-layer mapping for /api/jobs/{id}/enqueue in isolation from
 * real Redis and a real WorkerPool — a running WorkerPool would race to
 * drain whatever this test enqueues, making backpressure hard to observe
 * deterministically. JobService is mocked here; the actual backpressure
 * THRESHOLD logic is proven separately in JobQueueServiceIT.
 */
@WebMvcTest(JobController.class)
class JobControllerEnqueueTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private JobService jobService;

    @Test
    void enqueue_returns202OnSuccess() throws Exception {
        UUID id = UUID.randomUUID();
        when(jobService.enqueueJob(id)).thenReturn(sampleResponse(id));

        mockMvc.perform(post("/api/jobs/{id}/enqueue", id))
                .andExpect(status().isAccepted());
    }

    @Test
    void enqueue_returns404WhenJobDoesNotExist() throws Exception {
        UUID id = UUID.randomUUID();
        when(jobService.enqueueJob(id)).thenThrow(new JobNotFoundException(id));

        mockMvc.perform(post("/api/jobs/{id}/enqueue", id))
                .andExpect(status().isNotFound());
    }

    @Test
    void enqueue_returns409WhenJobIsNotActive() throws Exception {
        UUID id = UUID.randomUUID();
        when(jobService.enqueueJob(id)).thenThrow(new JobConflictException("not active"));

        mockMvc.perform(post("/api/jobs/{id}/enqueue", id))
                .andExpect(status().isConflict());
    }

    @Test
    void enqueue_returns429WhenQueueIsUnderBackpressure() throws Exception {
        UUID id = UUID.randomUUID();
        when(jobService.enqueueJob(id)).thenThrow(new QueueBackpressureException("queue full"));

        mockMvc.perform(post("/api/jobs/{id}/enqueue", id))
                .andExpect(status().isTooManyRequests());
    }

    private JobResponse sampleResponse(UUID id) {
        return new JobResponse(id, "job", ScheduleType.CRON, "0 0 * * * *", null, null,
                5, 0, 0, JobStatus.ACTIVE, null, null, null);
    }
}
