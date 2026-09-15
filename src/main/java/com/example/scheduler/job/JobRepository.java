package com.example.scheduler.job;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.OffsetDateTime;
import java.util.UUID;

public interface JobRepository extends JpaRepository<Job, UUID> {

    /**
     * Atomic soft-delete: single UPDATE guarded by the WHERE clause instead
     * of load -> mutate -> save. Returns 0 if the job doesn't exist or is
     * already cancelled (both are fine — cancel is idempotent).
     */
    @Modifying
    @Query("UPDATE Job j SET j.status = com.example.scheduler.job.JobStatus.CANCELLED, "
            + "j.updatedAt = CURRENT_TIMESTAMP "
            + "WHERE j.id = :id AND j.status <> com.example.scheduler.job.JobStatus.CANCELLED")
    int cancelIfNotAlreadyCancelled(@Param("id") UUID id);

    /**
     * Atomic manual-trigger: only succeeds if the job is currently ACTIVE.
     * The service layer uses the returned row count (0 vs 1) to decide
     * between 404 and 409 without a separate read-then-check race.
     */
    @Modifying
    @Query("UPDATE Job j SET j.nextRunAt = :triggeredAt, j.updatedAt = CURRENT_TIMESTAMP "
            + "WHERE j.id = :id AND j.status = com.example.scheduler.job.JobStatus.ACTIVE")
    int triggerIfActive(@Param("id") UUID id, @Param("triggeredAt") OffsetDateTime triggeredAt);
}
