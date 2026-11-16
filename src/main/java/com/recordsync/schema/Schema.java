package com.recordsync.schema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * A predefined schema for one object model, for example the internal "contact" or
 * Salesforce's "Contact". Used to validate data both before and after transformation.
 */
public class Schema {

    private static final Pattern EMAIL_PATTERN = Pattern.compile("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$");

    private final String name;
    private final Map<String, FieldSpec> fields = new LinkedHashMap<>();
    private final boolean allowUnknownFields;

    public Schema(String name, List<FieldSpec> fieldSpecs, boolean allowUnknownFields) {
        this.name = name;
        for (FieldSpec spec : fieldSpecs) {
            fields.put(spec.name(), spec);
        }
        this.allowUnknownFields = allowUnknownFields;
    }

    public String name() {
        return name;
    }

    /**
     * Checks a record against this schema.
     *
     * @return the list of problems found; an empty list means the record is valid
     */
    public List<String> validate(Map<String, Object> record) {
        List<String> errors = new ArrayList<>();

        for (FieldSpec spec : fields.values()) {
            Object value = record.get(spec.name());
            if (value == null) {
                if (spec.required()) {
                    errors.add(name + "." + spec.name() + " is required");
                }
                continue;
            }
            if (!matchesType(value, spec.type())) {
                errors.add(name + "." + spec.name() + " must be " + spec.type() + " but was '" + value + "'");
            }
        }

        if (!allowUnknownFields) {
            for (String fieldName : record.keySet()) {
                if (!fields.containsKey(fieldName)) {
                    errors.add(name + "." + fieldName + " is not a known field");
                }
            }
        }
        return errors;
    }

    private static boolean matchesType(Object value, FieldType type) {
        switch (type) {
            case STRING:
                return value instanceof String;
            case NUMBER:
                return value instanceof Number;
            case BOOLEAN:
                return value instanceof Boolean;
            case EMAIL:
                return value instanceof String && EMAIL_PATTERN.matcher((String) value).matches();
            default:
                return false;
        }
    }
}
