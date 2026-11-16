package com.recordsync.schema;

public record FieldSpec(String name, FieldType type, boolean required) {

    public static FieldSpec required(String name, FieldType type) {
        return new FieldSpec(name, type, true);
    }

    public static FieldSpec optional(String name, FieldType type) {
        return new FieldSpec(name, type, false);
    }
}
