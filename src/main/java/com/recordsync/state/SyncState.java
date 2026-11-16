package com.recordsync.state;

/**
 * What we remember about one record in one CRM after the last successful sync.
 *
 * @param externalId        the CRM's id for the record (null if not in the CRM, for example after a delete)
 * @param lastSyncedVersion the internal version that was last written to the CRM
 * @param lastPayloadHash   fingerprint of the last payload sent, to skip no-op updates
 */
public record SyncState(String externalId, long lastSyncedVersion, String lastPayloadHash) {
}
