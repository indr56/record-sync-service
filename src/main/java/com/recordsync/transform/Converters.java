package com.recordsync.transform;

import java.util.Locale;
import java.util.function.Function;

/**
 * Small, reusable value conversions used by field mappings.
 * In production these would be selected by name from stored mapping configuration.
 */
public final class Converters {

    private Converters() {
    }

    public static Function<Object, Object> numberToString() {
        return value -> value == null ? null : String.valueOf(value);
    }

    public static Function<Object, Object> lowerCase() {
        return value -> value == null ? null : value.toString().toLowerCase(Locale.ROOT);
    }

    public static Function<Object, Object> upperCase() {
        return value -> value == null ? null : value.toString().toUpperCase(Locale.ROOT);
    }
}
