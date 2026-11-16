package com.recordsync.ratelimit;

import java.util.function.LongSupplier;

/**
 * Token bucket rate limiter.
 *
 * The bucket holds up to {@code capacity} tokens and refills at {@code refillPerSecond}.
 * Each API call takes one token. Short bursts are allowed up to the capacity; the long-run
 * rate cannot exceed the refill rate.
 *
 * Instead of blocking a thread while waiting, both methods return how long to wait, so the
 * caller can schedule the work for later and keep the thread free for other records.
 * {@link #tryAcquire()} only takes a token if one is free now (used by the mock CRM to reject
 * calls); {@link #reserve()} books the next free slot (used by the pipeline).
 */
public class TokenBucket {

    private final double capacity;
    private final double refillPerSecond;
    private final LongSupplier nanoClock;

    private double tokens;
    private long lastRefillNanos;

    public TokenBucket(double capacity, double refillPerSecond) {
        this(capacity, refillPerSecond, System::nanoTime);
    }

    /** The clock is injectable so tests can control time. */
    public TokenBucket(double capacity, double refillPerSecond, LongSupplier nanoClock) {
        if (capacity < 1 || refillPerSecond <= 0) {
            throw new IllegalArgumentException("capacity must be >= 1 and refillPerSecond > 0");
        }
        this.capacity = capacity;
        this.refillPerSecond = refillPerSecond;
        this.nanoClock = nanoClock;
        this.tokens = capacity;
        this.lastRefillNanos = nanoClock.getAsLong();
    }

    /**
     * Tries to take one token.
     *
     * @return 0 if a token was taken; otherwise the number of milliseconds until one is available
     */
    public synchronized long tryAcquire() {
        refill();
        if (tokens >= 1) {
            tokens -= 1;
            return 0;
        }
        double missing = 1 - tokens;
        long waitMillis = (long) Math.ceil(missing / refillPerSecond * 1000);
        return Math.max(1, waitMillis);
    }

    /**
     * Books the next available token, even if it is in the future, and returns how long to wait
     * for it. The token is taken now (the balance can go below zero), so the caller must not
     * ask again: after waiting, it simply goes ahead. Each caller wakes exactly once, at its
     * turn, which avoids a "thundering herd" of re-checks.
     *
     * @return 0 if the token is available now; otherwise the number of milliseconds until the booked slot
     */
    public synchronized long reserve() {
        refill();
        tokens -= 1;
        if (tokens >= 0) {
            return 0;
        }
        long waitMillis = (long) Math.ceil(-tokens / refillPerSecond * 1000);
        return Math.max(1, waitMillis);
    }

    private void refill() {
        long now = nanoClock.getAsLong();
        double elapsedSeconds = (now - lastRefillNanos) / 1_000_000_000.0;
        if (elapsedSeconds > 0) {
            tokens = Math.min(capacity, tokens + elapsedSeconds * refillPerSecond);
            lastRefillNanos = now;
        }
    }
}
