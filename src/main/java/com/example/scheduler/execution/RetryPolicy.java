package com.example.scheduler.execution;

public final class RetryPolicy {

    private RetryPolicy() {
    }

    /**
     * min(initialBackoffMillis * 2^retryCount, maxBackoffMillis), per spec.
     * retryCount is clamped before shifting to guard against overflow —
     * max_retries has no enforced upper bound at the API layer (Stage 1
     * only validated >= 0), and 2^30 already vastly exceeds any realistic
     * maxBackoff, so clamping there loses nothing meaningful.
     */
    public static long computeBackoffMillis(int retryCount, long initialBackoffMillis, long maxBackoffMillis) {
        int safeExponent = Math.min(Math.max(retryCount, 0), 30);
        long backoff = initialBackoffMillis * (1L << safeExponent);
        if (backoff < 0 || backoff > maxBackoffMillis) {
            return maxBackoffMillis;
        }
        return backoff;
    }
}
