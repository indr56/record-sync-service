package com.recordsync.transform;

import com.recordsync.demo.SampleConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.recordsync.TestEvents.contact;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ObjectMappingTest {

    @Test
    void mapsToSalesforceModel() {
        Map<String, Object> external = SampleConfig.salesforceContactMapping().toExternal(contact("Lovelace", 500));

        assertEquals("Lovelace", external.get("LastName"));
        assertEquals("lovelace@acme.com", external.get("Email"));
        assertEquals("Acme", external.get("Account_Name__c"));
        assertEquals(500L, external.get("AnnualRevenue"));
        assertEquals("LEAD", external.get("Lifecycle_Stage__c"));
    }

    @Test
    void mapsToHubspotModelWithDifferentTypes() {
        Map<String, Object> external = SampleConfig.hubspotContactMapping().toExternal(contact("Lovelace", 500));

        assertEquals("Lovelace", external.get("lastname"));
        assertEquals("500", external.get("annualrevenue"));   // number converted to string
        assertEquals("lead", external.get("lifecyclestage")); // lower case
    }

    @Test
    void unmappedInternalFieldsAreNotSent() {
        Map<String, Object> internal = new HashMap<>(contact("Lovelace", 500));
        internal.put("internal_notes", "secret");

        Map<String, Object> external = SampleConfig.salesforceContactMapping().toExternal(internal);

        assertFalse(external.containsValue("secret"));
        assertFalse(external.containsKey("sync_enabled"));
    }

    @Test
    void transformedOutputPassesTheCrmSchema() {
        ObjectMapping mapping = SampleConfig.hubspotContactMapping();
        Map<String, Object> external = mapping.toExternal(contact("Lovelace", 500));

        assertTrue(mapping.externalSchema().validate(external).isEmpty());
    }
}
