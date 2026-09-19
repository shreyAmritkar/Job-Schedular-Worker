package com.example.scheduler.queue;

import java.util.UUID;

/**
 * score is the raw ZSET score the job had at the moment it was popped —
 * useful for diagnostics/logging, but NOT reliably decomposable back into
 * "original priority" + "original timestamp" once aging has touched it
 * (aging subtracts a flat boost from whatever the score currently is).
 * Nothing in Stage 3 needs to invert it; it's carried through purely for
 * observability.
 */
public record QueuedJob(UUID jobId, double score) {
}
