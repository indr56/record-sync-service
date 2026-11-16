package com.recordsync.pipeline;

/**
 * What happened when a worker processed a task:
 * DONE means the task is finished (synced, skipped or dead-lettered) and the next task for the
 * same record can start; RETRY_LATER means the same task must run again after a delay, and
 * later tasks for the same record wait behind it to keep the order.
 */
public record ProcessResult(Kind kind, long delayMillis) {

    public enum Kind {
        DONE,
        RETRY_LATER
    }

    public static ProcessResult done() {
        return new ProcessResult(Kind.DONE, 0);
    }

    public static ProcessResult retryAfter(long delayMillis) {
        return new ProcessResult(Kind.RETRY_LATER, delayMillis);
    }
}
