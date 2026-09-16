package com.example.scheduler.leader;

import com.example.scheduler.leader.dto.SchedulerStatusResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/scheduler")
public class SchedulerStatusController {

    private final LeaderState state;

    public SchedulerStatusController(LeaderState state) {
        this.state = state;
    }

    @GetMapping("/status")
    public SchedulerStatusResponse status() {
        return new SchedulerStatusResponse(state.getInstanceId(), state.isLeader(), state.getLastRenewalAt());
    }
}
