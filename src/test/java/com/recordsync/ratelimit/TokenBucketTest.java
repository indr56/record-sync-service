package com.recordsync.ratelimit;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TokenBucketTest {

    private static final long MILLIS = 1_000_000L;

    private final AtomicLong fakeClock = new AtomicLong(0);

    @Test
    void allowsBurstUpToCapacityThenAsksToWait() {
        TokenBucket bucket = new TokenBucket(3, 10, fakeClock::get);

        assertEquals(0, bucket.tryAcquire());
        assertEquals(0, bucket.tryAcquire());
        assertEquals(0, bucket.tryAcquire());
        assertEquals(100, bucket.tryAcquire()); // 10 per second = one token every 100 ms
    }

    @Test
    void refillsOverTime() {
        TokenBucket bucket = new TokenBucket(1, 10, fakeClock::get);
        assertEquals(0, bucket.tryAcquire());
        assertTrue(bucket.tryAcquire() > 0);

        fakeClock.addAndGet(100 * MILLIS);

        assertEquals(0, bucket.tryAcquire());
    }

    @Test
    void neverRefillsAboveCapacity() {
        TokenBucket bucket = new TokenBucket(2, 10, fakeClock::get);
        fakeClock.addAndGet(10_000 * MILLIS); // a long idle period

        assertEquals(0, bucket.tryAcquire());
        assertEquals(0, bucket.tryAcquire());
        assertTrue(bucket.tryAcquire() > 0);
    }

    @Test
    void reserveBooksSuccessiveSlotsSoWaitersWakeInTurn() {
        TokenBucket bucket = new TokenBucket(1, 10, fakeClock::get);

        assertEquals(0, bucket.reserve());   // free now
        assertEquals(100, bucket.reserve()); // next slot in 100 ms
        assertEquals(200, bucket.reserve()); // the one after that in 200 ms
        assertEquals(300, bucket.reserve());
    }

    @Test
    void reservedSlotsAreUsedUpBeforeNewCallersGetTokens() {
        TokenBucket bucket = new TokenBucket(1, 10, fakeClock::get);
        bucket.reserve();
        bucket.reserve(); // books the slot at 100 ms

        fakeClock.addAndGet(100 * MILLIS);

        // The token that appeared at 100 ms belongs to the earlier reservation.
        assertEquals(100, bucket.tryAcquire());
    }
}
