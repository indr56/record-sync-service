package com.recordsync.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A change to one record in the internal system, as published by change data capture
 * (for example a transactional outbox or a database change stream).
 *
 * @param eventId    unique id of this event (used for logging and tracing)
 * @param tenantId   the customer account the record belongs to
 * @param objectType internal object type, for example "contact"
 * @param recordId   internal record id
 * @param changeType created, updated or deleted
 * @param version    version of the record after the change; increases with every change to the same record
 * @param fields     full snapshot of the record's fields after the change (empty for deletes)
 * @param source     who made the change: "internal" for a user or service in System A, or the CRM
 *                   provider name when the change was written by our own inbound sync (used for loop prevention)
 */
public record ChangeEvent(
        String eventId,
        String tenantId,
        String objectType,
        String recordId,
        ChangeType changeType,
        long version,
        Map<String, Object> fields,
        String source) {

    public static final String SOURCE_INTERNAL = "internal";

    public ChangeEvent {
        if (fields == null) {
            fields = Collections.emptyMap();
        } else {
            fields = Collections.unmodifiableMap(new HashMap<>(fields));
        }
        if (source == null) {
            source = SOURCE_INTERNAL;
        }
    }

    /** Key that identifies one record across all its events: tenant + object type + record id. */
    public String recordKey() {
        return tenantId + "/" + objectType + "/" + recordId;
    }
}
