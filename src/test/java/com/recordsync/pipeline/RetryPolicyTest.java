package com.recordsync.pipeline;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetryPolicyTest {

    private final RetryPolicy policy = new RetryPolicy(3, 100, 1_000);

    @Test
    void retriesUntilMaxAttempts() {
        assertTrue(policy.shouldRetry(1));
        assertTrue(policy.shouldRetry(2));
        assertFalse(policy.shouldRetry(3));
    }

    @Test
    void delayGrowsExponentiallyWithJitter() {
        for (int i = 0; i < 100; i++) {
            long first = policy.delayMillis(1);
            long third = policy.delayMillis(3);
            assertTrue(first >= 50 && first <= 100, "first delay in [50, 100] but was " + first);
            assertTrue(third >= 200 && third <= 400, "third delay in [200, 400] but was " + third);
        }
    }

    @Test
    void delayIsCapped() {
        for (int i = 0; i < 100; i++) {
            assertTrue(policy.delayMillis(30) <= 1_000);
        }
    }
}
