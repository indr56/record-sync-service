package com.recordsync.ratelimit;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds one token bucket per (CRM provider, tenant).
 *
 * Why per tenant: CRM limits are usually per customer account (for example, a daily API
 * allowance per Salesforce org). A separate budget per tenant also means one very busy
 * customer cannot use up the capacity that other customers depend on.
 *
 * In production the buckets live in a shared store (for example Redis).
 */
public class RateLimiterRegistry {

    private final Map<String, TokenBucket> buckets = new ConcurrentHashMap<>();
    private final Map<String, RateLimit> limitsByProvider;
    private final RateLimit defaultLimit;

    /** Requests per second allowed, with a burst size. */
    public record RateLimit(double burstCapacity, double requestsPerSecond) {
    }

    public RateLimiterRegistry(Map<String, RateLimit> limitsByProvider, RateLimit defaultLimit) {
        this.limitsByProvider = Map.copyOf(limitsByProvider);
        this.defaultLimit = defaultLimit;
    }

    /**
     * Books a slot for one call in this tenant's budget for this provider.
     *
     * @return 0 if the call may go ahead now, otherwise milliseconds until the booked slot
     */
    public long reserve(String provider, String tenantId) {
        TokenBucket bucket = buckets.computeIfAbsent(provider + "|" + tenantId, key -> {
            RateLimit limit = limitsByProvider.getOrDefault(provider, defaultLimit);
            return new TokenBucket(limit.burstCapacity(), limit.requestsPerSecond());
        });
        return bucket.reserve();
    }
}
