package com.recordsync.state;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stores the sync state of each record per CRM: the id mapping (internal id to external id),
 * the last synced version and the last payload fingerprint.
 *
 * This is in memory for the exercise. In production it would be a durable key-value store
 * (for example DynamoDB, Cassandra or a sharded Postgres table) keyed by
 * (tenant, provider, object type, record id).
 */
public class SyncStateStore {

    private final Map<String, SyncState> states = new ConcurrentHashMap<>();

    public SyncState get(String provider, String recordKey) {
        return states.get(key(provider, recordKey));
    }

    public void put(String provider, String recordKey, SyncState state) {
        states.put(key(provider, recordKey), state);
    }

    private static String key(String provider, String recordKey) {
        return provider + "|" + recordKey;
    }
}
