package com.example.scheduler.job;

import com.example.scheduler.job.dto.JobResponse;

public final class JobMapper {

    private JobMapper() {
    }

    public static JobResponse toResponse(Job job) {
        return new JobResponse(
                job.getId(),
                job.getName(),
                job.getScheduleType(),
                job.getCronExpression(),
                job.getRunAt(),
                job.getPayload(),
                job.getPriority(),
                job.getMaxRetries(),
                job.getRetryCount(),
                job.getStatus(),
                job.getNextRunAt(),
                job.getIdempotencyKey(),
                job.getCreatedAt(),
                job.getUpdatedAt()
        );
    }
}
