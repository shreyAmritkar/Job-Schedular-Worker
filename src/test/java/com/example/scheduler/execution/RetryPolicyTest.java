package com.example.scheduler.execution;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class RetryPolicyTest {

    @Test
    void firstRetryUsesBaseBackoffDoubled() {
        // min(1000 * 2^1, cap) = 2000
        long backoff = RetryPolicy.computeBackoffMillis(1, 1000, Long.MAX_VALUE);
        assertThat(backoff).isEqualTo(2000);
    }

    @Test
    void backoffDoublesWithEachRetry() {
        assertThat(RetryPolicy.computeBackoffMillis(0, 1000, Long.MAX_VALUE)).isEqualTo(1000);
        assertThat(RetryPolicy.computeBackoffMillis(1, 1000, Long.MAX_VALUE)).isEqualTo(2000);
        assertThat(RetryPolicy.computeBackoffMillis(2, 1000, Long.MAX_VALUE)).isEqualTo(4000);
        assertThat(RetryPolicy.computeBackoffMillis(3, 1000, Long.MAX_VALUE)).isEqualTo(8000);
    }

    @Test
    void backoffIsCappedAtMaximum() {
        long maxBackoff = Duration.ofMinutes(5).toMillis();
        // 1000 * 2^10 = 1,024,000ms, well past the 5-minute (300,000ms) cap
        long backoff = RetryPolicy.computeBackoffMillis(10, 1000, maxBackoff);

        assertThat(backoff).isEqualTo(maxBackoff);
    }

    @Test
    void veryLargeRetryCountDoesNotOverflowOrGoNegative() {
        long maxBackoff = Duration.ofMinutes(5).toMillis();
        long backoff = RetryPolicy.computeBackoffMillis(Integer.MAX_VALUE, 1000, maxBackoff);

        assertThat(backoff).isEqualTo(maxBackoff);
    }

    @Test
    void negativeRetryCountIsTreatedAsZero() {
        long backoff = RetryPolicy.computeBackoffMillis(-5, 1000, Long.MAX_VALUE);
        assertThat(backoff).isEqualTo(1000);
    }
}
