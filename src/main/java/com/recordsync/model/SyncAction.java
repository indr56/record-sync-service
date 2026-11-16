package com.recordsync.model;

/**
 * What we decide to do in the external CRM (System B) for one change.
 * SKIP means "no API call is needed".
 */
public enum SyncAction {
    CREATE,
    UPDATE,
    DELETE,
    SKIP
}
