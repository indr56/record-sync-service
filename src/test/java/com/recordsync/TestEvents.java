package com.recordsync;

import com.recordsync.demo.SampleConfig;
import com.recordsync.model.ChangeEvent;
import com.recordsync.model.ChangeType;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** Helpers for building change events in tests. */
public final class TestEvents {

    public static final String TENANT = "acme";
    private static final AtomicLong sequence = new AtomicLong();

    private TestEvents() {
    }

    public static Map<String, Object> contact(String lastName, String email, long revenue, boolean syncEnabled) {
        Map<String, Object> fields = new HashMap<>();
        fields.put("first_name", "Test");
        fields.put("last_name", lastName);
        fields.put("email", email);
        fields.put("company_name", "Acme");
        fields.put("annual_revenue", revenue);
        fields.put("lifecycle_stage", "Lead");
        fields.put("sync_enabled", syncEnabled);
        return fields;
    }

    public static Map<String, Object> contact(String lastName, long revenue) {
        return contact(lastName, lastName.toLowerCase() + "@acme.com", revenue, true);
    }

    public static ChangeEvent event(String recordId, ChangeType type, long version, Map<String, Object> fields) {
        return event(recordId, type, version, fields, ChangeEvent.SOURCE_INTERNAL);
    }

    public static ChangeEvent event(String recordId, ChangeType type, long version, Map<String, Object> fields, String source) {
        return new ChangeEvent("evt-" + sequence.incrementAndGet(), TENANT, SampleConfig.CONTACT, recordId,
                type, version, fields, source);
    }

    public static ChangeEvent delete(String recordId, long version) {
        return event(recordId, ChangeType.DELETED, version, Map.of());
    }

    public static String recordKey(String recordId) {
        return TENANT + "/" + SampleConfig.CONTACT + "/" + recordId;
    }
}
