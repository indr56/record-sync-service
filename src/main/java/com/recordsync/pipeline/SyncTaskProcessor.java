package com.recordsync.pipeline;

import com.recordsync.crm.CrmClient;
import com.recordsync.crm.PermanentCrmException;
import com.recordsync.crm.RateLimitedException;
import com.recordsync.model.ChangeEvent;
import com.recordsync.model.ChangeType;
import com.recordsync.model.SyncAction;
import com.recordsync.ratelimit.RateLimiterRegistry;
import com.recordsync.schema.Schema;
import com.recordsync.state.SyncState;
import com.recordsync.state.SyncStateStore;
import com.recordsync.transform.ObjectMapping;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Processes one sync task: the core of the outbound pipeline. Takes one change event for one
 * CRM connection through validation, rules, transformation, rate limiting and the CRM call,
 * and reports whether the task is done or must run again later. Each step is commented in
 * {@link #process(SyncTask)}.
 *
 * This class holds no per-record state of its own; ordering is guaranteed by the caller
 * ({@link PartitionWorker}), which never runs two tasks for the same record at once.
 */
public class SyncTaskProcessor {

    private final Map<String, Schema> internalSchemas;
    private final SyncStateStore stateStore;
    private final RateLimiterRegistry rateLimiters;
    private final RetryPolicy retryPolicy;
    private final DeadLetterQueue deadLetterQueue;
    private final SyncMetrics metrics;

    public SyncTaskProcessor(Map<String, Schema> internalSchemas,
                             SyncStateStore stateStore,
                             RateLimiterRegistry rateLimiters,
                             RetryPolicy retryPolicy,
                             DeadLetterQueue deadLetterQueue,
                             SyncMetrics metrics) {
        this.internalSchemas = Map.copyOf(internalSchemas);
        this.stateStore = stateStore;
        this.rateLimiters = rateLimiters;
        this.retryPolicy = retryPolicy;
        this.deadLetterQueue = deadLetterQueue;
        this.metrics = metrics;
    }

    public ProcessResult process(SyncTask task) {
        ChangeEvent event = task.event();
        CrmConnection connection = task.connection();
        String provider = connection.provider();

        // 1. Loop prevention: this change was written into System A by our inbound sync from this
        //    same CRM. Sending it back would create an endless ping-pong between the two systems.
        if (provider.equals(event.source())) {
            metrics.increment(SyncMetrics.SKIPPED_ECHO);
            SyncLog.info("SKIP echo        " + task);
            return ProcessResult.done();
        }

        // 2. Stale or duplicate: the CRM already has this version or a newer one. Covers events
        //    delivered twice by the queue and late-arriving older events.
        SyncState state = stateStore.get(provider, event.recordKey());
        if (state != null && event.version() <= state.lastSyncedVersion()) {
            metrics.increment(SyncMetrics.SKIPPED_STALE);
            SyncLog.info("SKIP stale       " + task + " (CRM already has v" + state.lastSyncedVersion() + ")");
            return ProcessResult.done();
        }

        ObjectMapping mapping = connection.mappingsByObjectType().get(event.objectType());
        if (mapping == null) {
            metrics.increment(SyncMetrics.SKIPPED_RULE);
            SyncLog.info("SKIP no mapping  " + task);
            return ProcessResult.done();
        }

        // 3. Validate the input against the internal schema (deletes carry no fields).
        if (event.changeType() != ChangeType.DELETED) {
            Schema internalSchema = internalSchemas.get(event.objectType());
            if (internalSchema == null) {
                return deadLetter(task, "no internal schema for object type " + event.objectType());
            }
            List<String> inputErrors = internalSchema.validate(event.fields());
            if (!inputErrors.isEmpty()) {
                return deadLetter(task, "invalid input: " + inputErrors);
            }
        }

        // 4. Decide the action from the rules and whether the record already exists in the CRM.
        boolean existsInCrm = state != null && state.externalId() != null;
        SyncAction action = connection.ruleEngine().decide(event, existsInCrm);
        if (action == SyncAction.SKIP) {
            metrics.increment(SyncMetrics.SKIPPED_RULE);
            SyncLog.info("SKIP by rules    " + task);
            return ProcessResult.done();
        }

        // 5. Transform to the CRM's object model and validate the output against the CRM schema.
        Map<String, Object> payload = null;
        String payloadHash = null;
        if (action == SyncAction.CREATE || action == SyncAction.UPDATE) {
            payload = mapping.toExternal(event.fields());
            List<String> outputErrors = mapping.externalSchema().validate(payload);
            if (!outputErrors.isEmpty()) {
                return deadLetter(task, "invalid output for " + provider + ": " + outputErrors);
            }
            payloadHash = fingerprint(payload);

            // 6. No-op: the fields this CRM cares about did not change. Saves API quota.
            if (action == SyncAction.UPDATE && payloadHash.equals(state.lastPayloadHash())) {
                stateStore.put(provider, event.recordKey(),
                        new SyncState(state.externalId(), event.version(), payloadHash));
                metrics.increment(SyncMetrics.SKIPPED_NO_CHANGE);
                SyncLog.info("SKIP no change   " + task);
                return ProcessResult.done();
            }
        }

        // 7. Client-side rate limit: stay inside this tenant's budget for this CRM.
        //    Book a slot once; if it is in the future, come back exactly then and go ahead.
        if (!task.hasReservedPermit()) {
            long waitMillis = rateLimiters.reserve(provider, event.tenantId());
            if (waitMillis > 0) {
                task.setReservedPermit(true);
                metrics.increment(SyncMetrics.DEFERRED_RATE_LIMIT);
                return ProcessResult.retryAfter(waitMillis);
            }
        }
        task.setReservedPermit(false); // the slot is used by the call below

        // 8 + 9. Call the CRM and record the new state.
        CrmClient client = connection.client();
        String externalObject = mapping.externalObjectType();
        try {
            switch (action) {
                case CREATE: {
                    // Same key for every retry of this create, so a lost response cannot cause a duplicate.
                    String idempotencyKey = provider + "|" + event.recordKey() + "|v" + event.version();
                    String externalId = client.create(event.tenantId(), externalObject, payload, idempotencyKey);
                    stateStore.put(provider, event.recordKey(), new SyncState(externalId, event.version(), payloadHash));
                    metrics.increment(SyncMetrics.SYNCED_CREATE);
                    SyncLog.info("CREATE ok        " + task + " => " + externalId);
                    break;
                }
                case UPDATE: {
                    client.update(event.tenantId(), externalObject, state.externalId(), payload);
                    stateStore.put(provider, event.recordKey(), new SyncState(state.externalId(), event.version(), payloadHash));
                    metrics.increment(SyncMetrics.SYNCED_UPDATE);
                    SyncLog.info("UPDATE ok        " + task + " => " + state.externalId());
                    break;
                }
                case DELETE: {
                    client.delete(event.tenantId(), externalObject, state.externalId());
                    stateStore.put(provider, event.recordKey(), new SyncState(null, event.version(), null));
                    metrics.increment(SyncMetrics.SYNCED_DELETE);
                    SyncLog.info("DELETE ok        " + task + " => " + state.externalId());
                    break;
                }
                default:
                    break;
            }
            return ProcessResult.done();

        } catch (RateLimitedException e) {
            // The CRM itself throttled us (our budget estimate was too optimistic). Not the
            // record's fault, so it does not count as a failed attempt. It has its own, higher
            // limit, so a CRM that never stops throttling cannot keep the record waiting forever.
            int throttles = task.recordThrottledAttempt();
            if (!retryPolicy.shouldWaitForThrottle(throttles)) {
                return deadLetter(task, "still throttled after " + throttles + " attempts: " + e.getMessage());
            }
            metrics.increment(SyncMetrics.DEFERRED_RATE_LIMIT);
            SyncLog.info("THROTTLED        " + task + " retry in " + e.retryAfterMillis() + "ms");
            return ProcessResult.retryAfter(e.retryAfterMillis());

        } catch (PermanentCrmException e) {
            return deadLetter(task, e.getMessage());

        } catch (RuntimeException e) {
            // TransientCrmException, and any unexpected error, is retried a bounded number of
            // times with backoff, then dead-lettered.
            int failures = task.recordFailedAttempt();
            if (retryPolicy.shouldRetry(failures)) {
                long delay = retryPolicy.delayMillis(failures);
                metrics.increment(SyncMetrics.RETRIED_TRANSIENT);
                SyncLog.info("RETRY " + failures + "/" + retryPolicy.maxAttempts() + "      " + task
                        + " in " + delay + "ms (" + e.getMessage() + ")");
                return ProcessResult.retryAfter(delay);
            }
            return deadLetter(task, "gave up after " + failures + " attempts: " + e.getMessage());
        }
    }

    private ProcessResult deadLetter(SyncTask task, String reason) {
        deadLetterQueue.add(task, reason);
        metrics.increment(SyncMetrics.DEAD_LETTERED);
        SyncLog.info("DEAD-LETTER      " + task + " : " + reason);
        return ProcessResult.done();
    }

    /** Stable fingerprint of a payload: SHA-256 of its fields sorted by name. */
    static String fingerprint(Map<String, Object> payload) {
        String canonical = new TreeMap<>(payload).toString();
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
