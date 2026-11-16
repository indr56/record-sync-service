package com.recordsync.crm;

import com.recordsync.ratelimit.TokenBucket;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An in-memory stand-in for an external CRM (System B), used by the demo and tests.
 *
 * It behaves like a real CRM API in the ways that matter to the pipeline:
 * - it has its own server-side rate limit and answers "429 rate limited" when exceeded
 * - it can be told to fail the next N calls with transient errors (timeouts, 5xx)
 * - it can simulate a lost response: the create succeeds but the caller sees a timeout
 * - it honours idempotency keys on create, like real CRM APIs that support them
 * - update/delete of an unknown id is a permanent error (404)
 */
public class MockCrmClient implements CrmClient {

    private final String providerName;
    private final String idPrefix;
    private final TokenBucket serverRateLimit; // null means "no server-side limit"

    private final Map<String, Map<String, Object>> records = new ConcurrentHashMap<>();
    private final Map<String, String> idempotencyKeys = new ConcurrentHashMap<>();
    private final Map<String, List<Map<String, Object>>> writeHistory = new ConcurrentHashMap<>();
    private final List<String> callLog = Collections.synchronizedList(new ArrayList<>());

    private final AtomicLong idSequence = new AtomicLong();
    private final AtomicInteger apiCalls = new AtomicInteger();
    private final AtomicInteger transientFailuresToInject = new AtomicInteger();
    private final AtomicBoolean loseNextCreateResponse = new AtomicBoolean();

    public MockCrmClient(String providerName, String idPrefix, TokenBucket serverRateLimit) {
        this.providerName = providerName;
        this.idPrefix = idPrefix;
        this.serverRateLimit = serverRateLimit;
    }

    @Override
    public String providerName() {
        return providerName;
    }

    @Override
    public String create(String tenantId, String objectType, Map<String, Object> payload, String idempotencyKey) {
        beforeCall();
        String existingId = idempotencyKeys.get(idempotencyKey);
        if (existingId != null) {
            callLog.add("CREATE " + objectType + " " + existingId + " (idempotent replay, no duplicate)");
            return existingId;
        }
        String externalId = idPrefix + "-" + idSequence.incrementAndGet();
        records.put(externalId, new HashMap<>(payload));
        idempotencyKeys.put(idempotencyKey, externalId);
        recordWrite(externalId, payload);
        callLog.add("CREATE " + objectType + " " + externalId + " " + payload);

        if (loseNextCreateResponse.compareAndSet(true, false)) {
            // The record was created, but the response never reaches the caller.
            throw new TransientCrmException(providerName + ": timeout waiting for create response");
        }
        return externalId;
    }

    @Override
    public void update(String tenantId, String objectType, String externalId, Map<String, Object> payload) {
        beforeCall();
        Map<String, Object> existing = records.get(externalId);
        if (existing == null) {
            throw new PermanentCrmException(providerName + ": 404 " + objectType + " " + externalId + " not found");
        }
        existing.putAll(payload);
        recordWrite(externalId, payload);
        callLog.add("UPDATE " + objectType + " " + externalId + " " + payload);
    }

    @Override
    public void delete(String tenantId, String objectType, String externalId) {
        beforeCall();
        if (records.remove(externalId) == null) {
            throw new PermanentCrmException(providerName + ": 404 " + objectType + " " + externalId + " not found");
        }
        callLog.add("DELETE " + objectType + " " + externalId);
    }

    private void beforeCall() {
        apiCalls.incrementAndGet();
        if (serverRateLimit != null) {
            long waitMillis = serverRateLimit.tryAcquire();
            if (waitMillis > 0) {
                throw new RateLimitedException(providerName + ": 429 too many requests", waitMillis);
            }
        }
        if (transientFailuresToInject.get() > 0 && transientFailuresToInject.getAndDecrement() > 0) {
            throw new TransientCrmException(providerName + ": 503 service unavailable");
        }
    }

    private void recordWrite(String externalId, Map<String, Object> payload) {
        writeHistory.computeIfAbsent(externalId, id -> Collections.synchronizedList(new ArrayList<>()))
                .add(new HashMap<>(payload));
    }

    /** The next {@code count} calls fail with a transient error. */
    public void injectTransientFailures(int count) {
        transientFailuresToInject.set(count);
    }

    /** The next create succeeds on the CRM side, but the caller gets a timeout. */
    public void loseNextCreateResponse() {
        loseNextCreateResponse.set(true);
    }

    public Map<String, Object> getRecord(String externalId) {
        Map<String, Object> record = records.get(externalId);
        return record == null ? null : new HashMap<>(record);
    }

    public int recordCount() {
        return records.size();
    }

    /** Every payload written to a record, in the order the CRM received them. */
    public List<Map<String, Object>> writesFor(String externalId) {
        List<Map<String, Object>> writes = writeHistory.get(externalId);
        return writes == null ? List.of() : new ArrayList<>(writes);
    }

    public List<String> callLog() {
        synchronized (callLog) {
            return new ArrayList<>(callLog);
        }
    }

    /** Number of API calls that reached the CRM, including rejected ones. */
    public int apiCallCount() {
        return apiCalls.get();
    }
}
