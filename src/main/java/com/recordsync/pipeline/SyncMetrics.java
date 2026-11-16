package com.recordsync.pipeline;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple counters describing what the pipeline did. In production these would be exported
 * to a metrics system (for example Prometheus) and drive dashboards and alerts:
 * dead-letter growth, retry rate, rate-limit deferrals per provider, queue lag.
 */
public class SyncMetrics {

    public static final String SYNCED_CREATE = "synced.create";
    public static final String SYNCED_UPDATE = "synced.update";
    public static final String SYNCED_DELETE = "synced.delete";
    public static final String SKIPPED_RULE = "skipped.no_rule_or_nothing_to_do";
    public static final String SKIPPED_STALE = "skipped.stale_or_duplicate";
    public static final String SKIPPED_NO_CHANGE = "skipped.no_change";
    public static final String SKIPPED_ECHO = "skipped.echo_from_same_crm";
    public static final String COALESCED = "coalesced.superseded_update";
    public static final String DEFERRED_RATE_LIMIT = "deferred.rate_limit";
    public static final String RETRIED_TRANSIENT = "retried.transient_error";
    public static final String DEAD_LETTERED = "dead_lettered";

    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();

    public void increment(String name) {
        counters.computeIfAbsent(name, key -> new AtomicLong()).incrementAndGet();
    }

    public long get(String name) {
        AtomicLong counter = counters.get(name);
        return counter == null ? 0 : counter.get();
    }

    /** All counters, sorted by name. */
    public Map<String, Long> snapshot() {
        Map<String, Long> result = new TreeMap<>();
        for (Map.Entry<String, AtomicLong> entry : counters.entrySet()) {
            result.put(entry.getKey(), entry.getValue().get());
        }
        return result;
    }
}
