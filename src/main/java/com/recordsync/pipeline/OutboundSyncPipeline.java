package com.recordsync.pipeline;

import com.recordsync.model.ChangeEvent;
import com.recordsync.ratelimit.RateLimiterRegistry;
import com.recordsync.schema.Schema;
import com.recordsync.state.SyncStateStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Entry point of the internal -> external sync pipeline.
 *
 * For each change event from the internal system:
 * 1. Fan out: one task per CRM connection of the event's tenant.
 * 2. Partition: hash the task's ordering key (provider + record) to pick a partition, so all
 *    changes to one record in one CRM always go to the same single-threaded worker, in order.
 * 3. The worker runs the task through {@link SyncTaskProcessor}.
 */
public class OutboundSyncPipeline implements AutoCloseable {

    private final Map<String, List<CrmConnection>> connectionsByTenant = new HashMap<>();
    private final List<PartitionWorker> workers = new ArrayList<>();
    private final SyncStateStore stateStore = new SyncStateStore();
    private final DeadLetterQueue deadLetterQueue = new DeadLetterQueue();
    private final SyncMetrics metrics = new SyncMetrics();
    private final AtomicInteger tasksInFlight = new AtomicInteger();

    public OutboundSyncPipeline(List<CrmConnection> connections,
                                Map<String, Schema> internalSchemas,
                                RateLimiterRegistry rateLimiters,
                                RetryPolicy retryPolicy,
                                int partitionCount) {
        for (CrmConnection connection : connections) {
            connectionsByTenant.computeIfAbsent(connection.tenantId(), id -> new ArrayList<>()).add(connection);
        }
        SyncTaskProcessor processor = new SyncTaskProcessor(
                internalSchemas, stateStore, rateLimiters, retryPolicy, deadLetterQueue, metrics);
        for (int i = 0; i < partitionCount; i++) {
            workers.add(new PartitionWorker(i, processor, metrics, tasksInFlight::decrementAndGet));
        }
    }

    /** Accepts one change event from the internal system. Returns immediately; work is asynchronous. */
    public void submit(ChangeEvent event) {
        List<CrmConnection> connections = connectionsByTenant.getOrDefault(event.tenantId(), List.of());
        for (CrmConnection connection : connections) {
            SyncTask task = new SyncTask(event, connection);
            tasksInFlight.incrementAndGet();
            workerFor(task).submit(task);
        }
    }

    private PartitionWorker workerFor(SyncTask task) {
        int partition = Math.floorMod(task.orderingKey().hashCode(), workers.size());
        return workers.get(partition);
    }

    /**
     * Waits until every submitted task has finished (synced, skipped or dead-lettered).
     * Used by the demo and tests.
     *
     * @return true if everything finished within the timeout
     */
    public boolean awaitIdle(long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (tasksInFlight.get() > 0) {
            if (System.currentTimeMillis() > deadline) {
                return false;
            }
            Thread.sleep(10);
        }
        return true;
    }

    public SyncMetrics metrics() {
        return metrics;
    }

    public DeadLetterQueue deadLetterQueue() {
        return deadLetterQueue;
    }

    public SyncStateStore stateStore() {
        return stateStore;
    }

    @Override
    public void close() {
        for (PartitionWorker worker : workers) {
            worker.shutdown();
        }
    }
}
