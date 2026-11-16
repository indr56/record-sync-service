package com.recordsync.transform;

import com.recordsync.schema.Schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * How one internal object type maps to one CRM object type, for one CRM provider.
 * Example: internal "contact" maps to Salesforce "Contact" with a list of field mappings.
 *
 * Mappings are data, not code: adding a new CRM provider means adding a new mapping and
 * schema, not changing the pipeline.
 */
public class ObjectMapping {

    private final String internalObjectType;
    private final String externalObjectType;
    private final List<FieldMapping> fieldMappings;
    private final Schema externalSchema;

    public ObjectMapping(String internalObjectType,
                         String externalObjectType,
                         List<FieldMapping> fieldMappings,
                         Schema externalSchema) {
        this.internalObjectType = internalObjectType;
        this.externalObjectType = externalObjectType;
        this.fieldMappings = List.copyOf(fieldMappings);
        this.externalSchema = externalSchema;
    }

    public String internalObjectType() {
        return internalObjectType;
    }

    public String externalObjectType() {
        return externalObjectType;
    }

    public Schema externalSchema() {
        return externalSchema;
    }

    /**
     * Converts an internal record into the CRM's object model.
     * Internal fields that have no mapping are not sent.
     */
    public Map<String, Object> toExternal(Map<String, Object> internalFields) {
        Map<String, Object> external = new HashMap<>();
        for (FieldMapping mapping : fieldMappings) {
            Object value = internalFields.get(mapping.internalField());
            if (value != null) {
                external.put(mapping.externalField(), mapping.converter().apply(value));
            }
        }
        return external;
    }
}
