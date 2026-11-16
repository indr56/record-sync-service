package com.recordsync.pipeline;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential backoff with jitter for transient failures.
 *
 * Delay after the n-th failure = min(maxDelay, baseDelay * 2^(n-1)), then randomised to
 * between 50% and 100% of that value. The randomness ("jitter") stops thousands of failed
 * records from retrying at exactly the same moment and hitting a recovering CRM all at once.
 *
 * Throttling (429) is counted separately and does not use up the failure attempts, but it is
 * also bounded: a CRM that keeps throttling one record must not keep it waiting forever.
 */
public class RetryPolicy {

    private final int maxAttempts;
    private final long baseDelayMillis;
    private final long maxDelayMillis;
    private final int maxThrottledAttempts;

    public static final int DEFAULT_MAX_THROTTLED_ATTEMPTS = 100;

    public RetryPolicy(int maxAttempts, long baseDelayMillis, long maxDelayMillis) {
        this(maxAttempts, baseDelayMillis, maxDelayMillis, DEFAULT_MAX_THROTTLED_ATTEMPTS);
    }

    public RetryPolicy(int maxAttempts, long baseDelayMillis, long maxDelayMillis, int maxThrottledAttempts) {
        this.maxAttempts = maxAttempts;
        this.baseDelayMillis = baseDelayMillis;
        this.maxDelayMillis = maxDelayMillis;
        this.maxThrottledAttempts = maxThrottledAttempts;
    }

    /** @return true if the task should be retried after this many failed attempts */
    public boolean shouldRetry(int failedAttempts) {
        return failedAttempts < maxAttempts;
    }

    /** @return true if the task may wait again after the CRM has throttled it this many times */
    public boolean shouldWaitForThrottle(int throttledAttempts) {
        return throttledAttempts < maxThrottledAttempts;
    }

    public long delayMillis(int failedAttempts) {
        long exponential = baseDelayMillis * (1L << Math.min(failedAttempts - 1, 20));
        long capped = Math.min(maxDelayMillis, exponential);
        double jitterFactor = 0.5 + ThreadLocalRandom.current().nextDouble() * 0.5;
        return Math.max(1, (long) (capped * jitterFactor));
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public int maxThrottledAttempts() {
        return maxThrottledAttempts;
    }
}
