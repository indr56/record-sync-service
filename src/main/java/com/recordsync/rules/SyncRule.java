package com.recordsync.rules;

import com.recordsync.model.ChangeEvent;
import com.recordsync.model.ChangeType;

import java.util.Objects;
import java.util.Set;

/**
 * A pre-configured trigger: "for this object type, when these kinds of change happen,
 * and (optionally) when this field has this value, sync the record".
 *
 * Example: sync contacts on create, update and delete, but only when "sync_enabled" is true.
 *
 * @param objectType     internal object type this rule applies to
 * @param triggers       which change types fire the rule
 * @param conditionField optional field to check; null means "no condition"
 * @param conditionValue the value conditionField must have for the rule to match
 */
public record SyncRule(String objectType, Set<ChangeType> triggers, String conditionField, Object conditionValue) {

    public static SyncRule always(String objectType, Set<ChangeType> triggers) {
        return new SyncRule(objectType, triggers, null, null);
    }

    public static SyncRule when(String objectType, Set<ChangeType> triggers, String field, Object value) {
        return new SyncRule(objectType, triggers, field, value);
    }

    public boolean matches(ChangeEvent event) {
        if (!objectType.equals(event.objectType())) {
            return false;
        }
        if (!triggers.contains(event.changeType())) {
            return false;
        }
        // Deletes carry no field values, so a field condition cannot be checked; a matching
        // delete trigger is enough. Deleting something we never created is skipped later.
        if (conditionField == null || event.changeType() == ChangeType.DELETED) {
            return true;
        }
        return Objects.equals(event.fields().get(conditionField), conditionValue);
    }
}
