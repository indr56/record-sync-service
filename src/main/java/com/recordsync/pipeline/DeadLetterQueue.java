package com.recordsync.pipeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Where tasks go when they cannot be synced: invalid data, a permanent CRM error, or too many
 * transient failures. Nothing is silently dropped; each entry keeps the reason so it can be
 * inspected, fixed and replayed.
 *
 * In production this would be a separate topic or table with alerting on its growth.
 */
public class DeadLetterQueue {

    /** One failed task and why it failed. */
    public record DeadLetter(SyncTask task, String reason) {
    }

    private final List<DeadLetter> entries = Collections.synchronizedList(new ArrayList<>());

    public void add(SyncTask task, String reason) {
        entries.add(new DeadLetter(task, reason));
    }

    public List<DeadLetter> entries() {
        synchronized (entries) {
            return new ArrayList<>(entries);
        }
    }

    public int size() {
        return entries.size();
    }
}
