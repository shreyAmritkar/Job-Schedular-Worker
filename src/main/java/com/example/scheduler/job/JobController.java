package com.example.scheduler.job;

import com.example.scheduler.common.PageResponse;
import com.example.scheduler.job.dto.CreateJobRequest;
import com.example.scheduler.job.dto.JobResponse;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.UUID;

@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobService jobService;

    public JobController(JobService jobService) {
        this.jobService = jobService;
    }

    @PostMapping
    public ResponseEntity<JobResponse> createJob(@Valid @RequestBody CreateJobRequest request) {
        JobResponse response = jobService.createJob(request);
        return ResponseEntity.created(URI.create("/api/jobs/" + response.id())).body(response);
    }

    @GetMapping
    public PageResponse<JobResponse> listJobs(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        return jobService.listJobs(page, size);
    }

    @GetMapping("/{id}")
    public JobResponse getJob(@PathVariable UUID id) {
        return jobService.getJob(id);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> cancelJob(@PathVariable UUID id) {
        jobService.cancelJob(id);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/{id}/trigger")
    public JobResponse triggerJob(@PathVariable UUID id) {
        return jobService.triggerJob(id);
    }

    @PostMapping("/{id}/enqueue")
    public ResponseEntity<JobResponse> enqueueJob(@PathVariable UUID id) {
        JobResponse response = jobService.enqueueJob(id);
        return ResponseEntity.accepted().body(response);
    }
}
