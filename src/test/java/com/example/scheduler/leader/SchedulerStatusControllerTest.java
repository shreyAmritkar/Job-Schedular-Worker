package com.example.scheduler.leader;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;

import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(SchedulerStatusController.class)
class SchedulerStatusControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private LeaderState leaderState;

    @Test
    void returnsCurrentLeadershipState() throws Exception {
        Instant renewedAt = Instant.parse("2026-09-16T12:00:00Z");
        when(leaderState.getInstanceId()).thenReturn("host-abc123");
        when(leaderState.isLeader()).thenReturn(true);
        when(leaderState.getLastRenewalAt()).thenReturn(renewedAt);

        mockMvc.perform(get("/api/scheduler/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.instanceId").value("host-abc123"))
                .andExpect(jsonPath("$.isLeader").value(true))
                .andExpect(jsonPath("$.lastRenewalAt").value("2026-09-16T12:00:00Z"));
    }

    @Test
    void reflectsStandbyState() throws Exception {
        when(leaderState.getInstanceId()).thenReturn("host-def456");
        when(leaderState.isLeader()).thenReturn(false);
        when(leaderState.getLastRenewalAt()).thenReturn(null);

        mockMvc.perform(get("/api/scheduler/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.isLeader").value(false))
                .andExpect(jsonPath("$.lastRenewalAt").value(nullValue()));
    }
}
