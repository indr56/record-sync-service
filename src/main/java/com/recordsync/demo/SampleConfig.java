package com.recordsync.demo;

import com.recordsync.crm.CrmClient;
import com.recordsync.model.ChangeType;
import com.recordsync.pipeline.CrmConnection;
import com.recordsync.rules.RuleEngine;
import com.recordsync.rules.SyncRule;
import com.recordsync.schema.FieldSpec;
import com.recordsync.schema.FieldType;
import com.recordsync.schema.Schema;
import com.recordsync.transform.Converters;
import com.recordsync.transform.FieldMapping;
import com.recordsync.transform.ObjectMapping;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

/**
 * Sample configuration used by the demo and the tests: one internal object type ("contact")
 * and two CRM providers whose object models differ from ours and from each other.
 *
 * In production all of this would be stored configuration (per tenant), not code.
 */
public final class SampleConfig {

    public static final String CONTACT = "contact";
    public static final String SALESFORCE = "salesforce";
    public static final String HUBSPOT = "hubspot";

    private SampleConfig() {
    }

    /** The internal "contact" model in System A. */
    public static Schema internalContactSchema() {
        return new Schema("internal.contact", List.of(
                FieldSpec.optional("first_name", FieldType.STRING),
                FieldSpec.required("last_name", FieldType.STRING),
                FieldSpec.required("email", FieldType.EMAIL),
                FieldSpec.optional("company_name", FieldType.STRING),
                FieldSpec.optional("annual_revenue", FieldType.NUMBER),
                FieldSpec.optional("lifecycle_stage", FieldType.STRING),
                FieldSpec.optional("sync_enabled", FieldType.BOOLEAN),
                FieldSpec.optional("internal_notes", FieldType.STRING)
        ), false);
    }

    public static Map<String, Schema> internalSchemas() {
        return Map.of(CONTACT, internalContactSchema());
    }

    /** Salesforce-style "Contact": PascalCase names, a custom field, revenue as a number, stage in upper case. */
    public static ObjectMapping salesforceContactMapping() {
        Schema salesforceSchema = new Schema("salesforce.Contact", List.of(
                FieldSpec.optional("FirstName", FieldType.STRING),
                FieldSpec.required("LastName", FieldType.STRING),
                FieldSpec.required("Email", FieldType.EMAIL),
                FieldSpec.optional("Account_Name__c", FieldType.STRING),
                FieldSpec.optional("AnnualRevenue", FieldType.NUMBER),
                FieldSpec.optional("Lifecycle_Stage__c", FieldType.STRING)
        ), false);
        return new ObjectMapping(CONTACT, "Contact", List.of(
                FieldMapping.rename("first_name", "FirstName"),
                FieldMapping.rename("last_name", "LastName"),
                FieldMapping.rename("email", "Email"),
                FieldMapping.rename("company_name", "Account_Name__c"),
                FieldMapping.rename("annual_revenue", "AnnualRevenue"),
                FieldMapping.convert("lifecycle_stage", "Lifecycle_Stage__c", Converters.upperCase())
        ), salesforceSchema);
    }

    /** HubSpot-style "contacts": lower-case names, revenue as a string, stage in lower case. */
    public static ObjectMapping hubspotContactMapping() {
        Schema hubspotSchema = new Schema("hubspot.contacts", List.of(
                FieldSpec.optional("firstname", FieldType.STRING),
                FieldSpec.required("lastname", FieldType.STRING),
                FieldSpec.required("email", FieldType.EMAIL),
                FieldSpec.optional("company", FieldType.STRING),
                FieldSpec.optional("annualrevenue", FieldType.STRING),
                FieldSpec.optional("lifecyclestage", FieldType.STRING)
        ), false);
        return new ObjectMapping(CONTACT, "contacts", List.of(
                FieldMapping.rename("first_name", "firstname"),
                FieldMapping.rename("last_name", "lastname"),
                FieldMapping.rename("email", "email"),
                FieldMapping.rename("company_name", "company"),
                FieldMapping.convert("annual_revenue", "annualrevenue", Converters.numberToString()),
                FieldMapping.convert("lifecycle_stage", "lifecyclestage", Converters.lowerCase())
        ), hubspotSchema);
    }

    /** Salesforce rule: sync contacts on create, update and delete, but only when sync_enabled is true. */
    public static CrmConnection salesforceConnection(String tenantId, CrmClient client) {
        RuleEngine rules = new RuleEngine(List.of(
                SyncRule.when(CONTACT, EnumSet.allOf(ChangeType.class), "sync_enabled", true)));
        return new CrmConnection(tenantId, client, Map.of(CONTACT, salesforceContactMapping()), rules);
    }

    /** HubSpot rule: sync every contact change. */
    public static CrmConnection hubspotConnection(String tenantId, CrmClient client) {
        RuleEngine rules = new RuleEngine(List.of(
                SyncRule.always(CONTACT, EnumSet.allOf(ChangeType.class))));
        return new CrmConnection(tenantId, client, Map.of(CONTACT, hubspotContactMapping()), rules);
    }
}
