package com.recordsync.pipeline;

import com.recordsync.crm.CrmClient;
import com.recordsync.rules.RuleEngine;
import com.recordsync.transform.ObjectMapping;

import java.util.Map;

/**
 * One tenant's connection to one CRM: which client to call, how each internal object type
 * maps to the CRM's object model, and the rules that decide what gets synced.
 *
 * A tenant can have several connections (for example Salesforce and HubSpot at once);
 * each change event fans out to every connection of its tenant.
 */
public record CrmConnection(
        String tenantId,
        CrmClient client,
        Map<String, ObjectMapping> mappingsByObjectType,
        RuleEngine ruleEngine) {

    public String provider() {
        return client.providerName();
    }
}
