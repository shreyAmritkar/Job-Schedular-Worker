package com.example.scheduler.execution;

import com.example.scheduler.job.dto.JobResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/jobs/dead-letter")
public class DeadLetterController {

    private final DeadLetterService deadLetterService;

    public DeadLetterController(DeadLetterService deadLetterService) {
        this.deadLetterService = deadLetterService;
    }

    @GetMapping
    public List<JobResponse> listDeadLetterJobs() {
        return deadLetterService.listDeadLetterJobs();
    }

    @PostMapping("/{id}/requeue")
    public JobResponse requeue(@PathVariable UUID id) {
        return deadLetterService.requeue(id);
    }
}
