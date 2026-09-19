package com.example.scheduler.job;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface JobExecutionRepository extends JpaRepository<JobExecution, UUID> {

    List<JobExecution> findByJobId(UUID jobId);
}
