package com.example.scheduler;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Stage 0 acceptance test:
 *  - Spring context loads (proves JPA/Flyway/Redis auto-config wiring is valid)
 *  - Flyway successfully migrates the schema against a real Postgres container
 *  - /actuator/health reports UP (proves DB + Redis health indicators pass)
 */
class SchedulerApplicationIT extends AbstractIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void contextLoads() {
        // If Flyway migration or datasource/redis config were broken,
        // the Spring context would fail to start before this test even runs.
    }

    @Test
    void healthEndpointReportsUp() {
        ResponseEntity<String> response = restTemplate.getForEntity("/actuator/health", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("\"status\":\"UP\"");
    }
}
