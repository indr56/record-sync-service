package com.recordsync.transform;

import java.util.function.Function;

/**
 * Maps one internal field to one external field, with an optional value conversion.
 * Example: internal "company_name" becomes Salesforce "Account_Name__c".
 */
public record FieldMapping(String internalField, String externalField, Function<Object, Object> converter) {

    public static FieldMapping rename(String internalField, String externalField) {
        return new FieldMapping(internalField, externalField, Function.identity());
    }

    public static FieldMapping convert(String internalField, String externalField, Function<Object, Object> converter) {
        return new FieldMapping(internalField, externalField, converter);
    }
}
