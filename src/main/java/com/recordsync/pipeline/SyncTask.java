package com.recordsync.pipeline;

import com.recordsync.model.ChangeEvent;

/**
 * The unit of work: sync one change event to one CRM connection.
 * Tracks how many times it has failed with a transient error and how many times the CRM has
 * throttled it, for retry limits.
 */
public class SyncTask {

    private final ChangeEvent event;
    private final CrmConnection connection;
    private int failedAttempts;
    private int throttledAttempts;
    private boolean permitReserved;

    public SyncTask(ChangeEvent event, CrmConnection connection) {
        this.event = event;
        this.connection = connection;
    }

    public ChangeEvent event() {
        return event;
    }

    public CrmConnection connection() {
        return connection;
    }

    /**
     * Ordering key: all tasks with the same key are processed one at a time, in arrival order.
     * One record in one CRM.
     */
    public String orderingKey() {
        return connection.provider() + "|" + event.recordKey();
    }

    public int failedAttempts() {
        return failedAttempts;
    }

    public int recordFailedAttempt() {
        failedAttempts++;
        return failedAttempts;
    }

    public int throttledAttempts() {
        return throttledAttempts;
    }

    public int recordThrottledAttempt() {
        throttledAttempts++;
        return throttledAttempts;
    }

    /** True if this task has already booked a rate-limit slot and only needs to wait for it. */
    public boolean hasReservedPermit() {
        return permitReserved;
    }

    public void setReservedPermit(boolean reserved) {
        this.permitReserved = reserved;
    }

    @Override
    public String toString() {
        return event.changeType() + " " + event.recordKey() + " v" + event.version() + " -> " + connection.provider();
    }
}
