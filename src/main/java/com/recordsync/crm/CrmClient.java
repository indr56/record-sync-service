package com.recordsync.crm;

import java.util.Map;

/**
 * The adapter every CRM provider implements (Salesforce, HubSpot, Dynamics, ...).
 * The pipeline only talks to this interface, so adding a provider means adding an
 * adapter plus mapping configuration, with no change to the pipeline.
 *
 * Implementations throw a {@link CrmException} subclass to signal failures.
 */
public interface CrmClient {

    /** Provider name, for example "salesforce". */
    String providerName();

    /**
     * Creates a record.
     *
     * @param idempotencyKey a key that is the same for every retry of the same logical create;
     *                       if the CRM has already seen it, it must return the existing record
     *                       instead of creating a duplicate
     * @return the CRM's id for the record
     */
    String create(String tenantId, String objectType, Map<String, Object> payload, String idempotencyKey);

    void update(String tenantId, String objectType, String externalId, Map<String, Object> payload);

    void delete(String tenantId, String objectType, String externalId);
}
