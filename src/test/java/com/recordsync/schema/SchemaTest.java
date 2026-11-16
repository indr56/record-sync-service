package com.recordsync.schema;

import com.recordsync.demo.SampleConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.recordsync.TestEvents.contact;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaTest {

    private final Schema schema = SampleConfig.internalContactSchema();

    @Test
    void validRecordHasNoErrors() {
        assertTrue(schema.validate(contact("Lovelace", 100)).isEmpty());
    }

    @Test
    void missingRequiredFieldIsReported() {
        Map<String, Object> record = new HashMap<>(contact("Lovelace", 100));
        record.remove("last_name");

        List<String> errors = schema.validate(record);

        assertEquals(List.of("internal.contact.last_name is required"), errors);
    }

    @Test
    void wrongTypeIsReported() {
        Map<String, Object> record = new HashMap<>(contact("Lovelace", 100));
        record.put("annual_revenue", "a lot");

        List<String> errors = schema.validate(record);

        assertEquals(1, errors.size());
        assertTrue(errors.get(0).contains("annual_revenue must be NUMBER"));
    }

    @Test
    void invalidEmailIsReported() {
        List<String> errors = schema.validate(contact("Lovelace", "not-an-email", 1, true));

        assertEquals(1, errors.size());
        assertTrue(errors.get(0).contains("email must be EMAIL"));
    }

    @Test
    void unknownFieldIsReportedWhenSchemaIsStrict() {
        Map<String, Object> record = new HashMap<>(contact("Lovelace", 100));
        record.put("favourite_colour", "blue");

        List<String> errors = schema.validate(record);

        assertEquals(List.of("internal.contact.favourite_colour is not a known field"), errors);
    }
}
