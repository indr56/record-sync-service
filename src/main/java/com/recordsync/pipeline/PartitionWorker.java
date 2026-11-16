package com.recordsync.pipeline;

import com.recordsync.model.ChangeType;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Processes the tasks of one partition on a single thread, with a small queue per record.
 *
 * Tasks for the same record run one at a time, in arrival order. A task waiting for a retry or
 * a rate-limit slot waits on a timer, so other records in the partition keep flowing. The CRM
 * call itself is synchronous on this thread, so a slow call still delays the partition.
 *
 * Everything runs on the partition's one thread, so the per-record queues need no locks.
 */
public class PartitionWorker {

    private final ScheduledExecutorService executor;
    private final SyncTaskProcessor processor;
    private final SyncMetrics metrics;
    private final Runnable onTaskFinished;

    /** Pending tasks per ordering key. Only touched on this worker's thread. */
    private final Map<String, Deque<SyncTask>> pendingByRecord = new HashMap<>();

    public PartitionWorker(int partitionNumber, SyncTaskProcessor processor, SyncMetrics metrics, Runnable onTaskFinished) {
        this.processor = processor;
        this.metrics = metrics;
        this.onTaskFinished = onTaskFinished;
        this.executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "partition-" + partitionNumber);
            thread.setDaemon(true);
            return thread;
        });
    }

    /** Adds a task. Safe to call from any thread; the work happens on this worker's thread. */
    public void submit(SyncTask task) {
        executor.execute(() -> enqueue(task));
    }

    private void enqueue(SyncTask task) {
        String key = task.orderingKey();
        Deque<SyncTask> queue = pendingByRecord.computeIfAbsent(key, k -> new ArrayDeque<>());
        boolean recordWasIdle = queue.isEmpty();
        queue.addLast(task);
        if (recordWasIdle) {
            executor.execute(() -> processFront(key));
        }
        // Otherwise a task for this record is already running or waiting; this one runs after it.
    }

    private void processFront(String key) {
        Deque<SyncTask> queue = pendingByRecord.get(key);
        if (queue == null || queue.isEmpty()) {
            return;
        }

        coalesceSupersededChanges(queue);

        SyncTask task = queue.peekFirst();
        ProcessResult result = processor.process(task);

        if (result.kind() == ProcessResult.Kind.RETRY_LATER) {
            // Keep the task at the front so newer changes for this record cannot overtake it.
            executor.schedule(() -> processFront(key), result.delayMillis(), TimeUnit.MILLISECONDS);
            return;
        }

        queue.pollFirst();
        onTaskFinished.run();
        if (queue.isEmpty()) {
            pendingByRecord.remove(key);
        } else {
            executor.execute(() -> processFront(key));
        }
    }

    /**
     * Each event carries a full snapshot of the record, so when several creates/updates for the
     * same record are queued, only the newest one needs to reach the CRM. Sending the older ones
     * would spend API quota on states that are already out of date.
     *
     * Only a NEWER version replaces an older one; a late-arriving older event never replaces a
     * newer one (it is skipped later as stale). Deletes are never merged away. A task that has
     * already failed once is also kept, because
     * its earlier attempt may have reached the CRM; replacing it with a newer event would change
     * its idempotency key and could create a duplicate.
     */
    private void coalesceSupersededChanges(Deque<SyncTask> queue) {
        while (queue.size() > 1) {
            Iterator<SyncTask> iterator = queue.iterator();
            SyncTask front = iterator.next();
            SyncTask next = iterator.next();
            boolean bothAreWrites = front.event().changeType() != ChangeType.DELETED
                    && next.event().changeType() != ChangeType.DELETED;
            boolean nextIsNewer = next.event().version() > front.event().version();
            if (!bothAreWrites || !nextIsNewer || front.failedAttempts() > 0) {
                return;
            }
            queue.pollFirst();
            if (front.hasReservedPermit()) {
                // Hand the already-booked rate-limit slot to the task that replaces it.
                next.setReservedPermit(true);
            }
            onTaskFinished.run();
            metrics.increment(SyncMetrics.COALESCED);
            SyncLog.info("COALESCE         " + front + " superseded by v" + next.event().version());
        }
    }

    public void shutdown() {
        executor.shutdownNow();
    }
}
