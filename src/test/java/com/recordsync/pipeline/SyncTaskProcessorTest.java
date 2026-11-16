package com.recordsync.pipeline;

import com.recordsync.crm.MockCrmClient;
import com.recordsync.demo.SampleConfig;
import com.recordsync.model.ChangeEvent;
import com.recordsync.model.ChangeType;
import com.recordsync.ratelimit.RateLimiterRegistry;
import com.recordsync.ratelimit.RateLimiterRegistry.RateLimit;
import com.recordsync.ratelimit.TokenBucket;
import com.recordsync.state.SyncState;
import com.recordsync.state.SyncStateStore;
import com.recordsync.transform.FieldMapping;
import com.recordsync.transform.ObjectMapping;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.recordsync.TestEvents.contact;
import static com.recordsync.TestEvents.delete;
import static com.recordsync.TestEvents.event;
import static com.recordsync.TestEvents.recordKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the processing steps one task at a time, on the test thread (no concurrency),
 * so each decision can be checked precisely.
 */
class SyncTaskProcessorTest {

    private MockCrmClient crm;
    private CrmConnection connection;
    private SyncStateStore stateStore;
    private DeadLetterQueue deadLetters;
    private SyncMetrics metrics;
    private SyncTaskProcessor processor;

    @BeforeEach
    void setUp() {
        crm = new MockCrmClient(SampleConfig.SALESFORCE, "sf", null);
        connection = SampleConfig.salesforceConnection("acme", crm);
        processor = newProcessor(new RateLimit(1_000, 1_000));
    }

    private SyncTaskProcessor newProcessor(RateLimit limit) {
        stateStore = new SyncStateStore();
        deadLetters = new DeadLetterQueue();
        metrics = new SyncMetrics();
        return new SyncTaskProcessor(SampleConfig.internalSchemas(), stateStore,
                new RateLimiterRegistry(Map.of(), limit), new RetryPolicy(3, 10, 100), deadLetters, metrics);
    }

    private ProcessResult process(ChangeEvent event) {
        return processor.process(new SyncTask(event, connection));
    }

    private SyncState state(String recordId) {
        return stateStore.get(SampleConfig.SALESFORCE, recordKey(recordId));
    }

    @Test
    void createThenUpdateThenDelete() {
        process(event("c-1", ChangeType.CREATED, 1, contact("Lovelace", 100)));
        String externalId = state("c-1").externalId();
        assertEquals("Lovelace", crm.getRecord(externalId).get("LastName"));

        process(event("c-1", ChangeType.UPDATED, 2, contact("Lovelace", 200)));
        assertEquals(200L, crm.getRecord(externalId).get("AnnualRevenue"));

        process(delete("c-1", 3));
        assertNull(crm.getRecord(externalId));
        assertNull(state("c-1").externalId());
        assertEquals(3, state("c-1").lastSyncedVersion());
    }

    @Test
    void echoFromSameCrmIsSkipped() {
        process(event("c-1", ChangeType.CREATED, 1, contact("Lovelace", 100), SampleConfig.SALESFORCE));

        assertEquals(0, crm.apiCallCount());
        assertEquals(1, metrics.get(SyncMetrics.SKIPPED_ECHO));
    }

    @Test
    void duplicateAndOlderEventsAreSkipped() {
        process(event("c-1", ChangeType.CREATED, 1, contact("Lovelace", 100)));
        process(event("c-1", ChangeType.UPDATED, 5, contact("Lovelace", 500)));
        int callsSoFar = crm.apiCallCount();

        process(event("c-1", ChangeType.UPDATED, 5, contact("Lovelace", 500))); // delivered twice
        process(event("c-1", ChangeType.UPDATED, 3, contact("Lovelace", 300))); // older, arrived late

        assertEquals(callsSoFar, crm.apiCallCount());
        assertEquals(2, metrics.get(SyncMetrics.SKIPPED_STALE));
        assertEquals(500L, crm.getRecord(state("c-1").externalId()).get("AnnualRevenue"));
    }

    @Test
    void invalidInputIsDeadLetteredWithoutCallingCrm() {
        process(event("c-1", ChangeType.CREATED, 1, contact("Lovelace", "bad-email", 1, true)));

        assertEquals(0, crm.apiCallCount());
        assertEquals(1, deadLetters.size());
        assertTrue(deadLetters.entries().get(0).reason().startsWith("invalid input"));
    }

    @Test
    void invalidOutputIsDeadLettered() {
        // A misconfigured mapping: writes "Surname" (unknown to Salesforce) and leaves out the
        // required LastName and Email. The output check catches it before any API call.
        ObjectMapping brokenMapping = new ObjectMapping(SampleConfig.CONTACT, "Contact",
                List.of(FieldMapping.rename("last_name", "Surname")),
                SampleConfig.salesforceContactMapping().externalSchema());
        connection = new CrmConnection("acme", crm, Map.of(SampleConfig.CONTACT, brokenMapping),
                SampleConfig.salesforceConnection("acme", crm).ruleEngine());

        process(event("c-1", ChangeType.CREATED, 1, contact("Lovelace", 100)));

        assertEquals(0, crm.apiCallCount());
        assertTrue(deadLetters.entries().get(0).reason().startsWith("invalid output"));
    }

    @Test
    void updateWithNoChangeInMappedFieldsSkipsTheApiCall() {
        process(event("c-1", ChangeType.CREATED, 1, contact("Lovelace", 100)));
        Map<String, Object> sameButInternalNoteChanged = new HashMap<>(contact("Lovelace", 100));
        sameButInternalNoteChanged.put("internal_notes", "edited internally only");

        process(event("c-1", ChangeType.UPDATED, 2, sameButInternalNoteChanged));

        assertEquals(1, crm.apiCallCount());
        assertEquals(1, metrics.get(SyncMetrics.SKIPPED_NO_CHANGE));
        assertEquals(2, state("c-1").lastSyncedVersion());
    }

    @Test
    void lostCreateResponseIsRetriedWithoutCreatingADuplicate() {
        crm.loseNextCreateResponse();
        SyncTask task = new SyncTask(event("c-1", ChangeType.CREATED, 1, contact("Lovelace", 100)), connection);

        ProcessResult first = processor.process(task);
        ProcessResult second = processor.process(task);

        assertEquals(ProcessResult.Kind.RETRY_LATER, first.kind());
        assertEquals(ProcessResult.Kind.DONE, second.kind());
        assertEquals(1, crm.recordCount());
        assertTrue(crm.callLog().get(1).contains("idempotent replay"));
    }

    @Test
    void transientErrorsAreRetriedThenDeadLettered() {
        crm.injectTransientFailures(10);
        SyncTask task = new SyncTask(event("c-1", ChangeType.CREATED, 1, contact("Lovelace", 100)), connection);

        assertEquals(ProcessResult.Kind.RETRY_LATER, processor.process(task).kind()); // attempt 1
        assertEquals(ProcessResult.Kind.RETRY_LATER, processor.process(task).kind()); // attempt 2
        assertEquals(ProcessResult.Kind.DONE, processor.process(task).kind());        // attempt 3 = max

        assertEquals(1, deadLetters.size());
        assertTrue(deadLetters.entries().get(0).reason().startsWith("gave up after 3 attempts"));
    }

    @Test
    void permanentErrorIsDeadLetteredImmediately() {
        process(event("c-1", ChangeType.CREATED, 1, contact("Lovelace", 100)));
        // Someone deleted the record directly in the CRM, so our update gets a 404.
        crm.delete("acme", "Contact", state("c-1").externalId());

        ProcessResult result = process(event("c-1", ChangeType.UPDATED, 2, contact("Lovelace", 200)));

        assertEquals(ProcessResult.Kind.DONE, result.kind());
        assertTrue(deadLetters.entries().get(0).reason().contains("404"));
    }

    @Test
    void crmThrottlingDoesNotCountAsAFailedAttempt() {
        crm = new MockCrmClient(SampleConfig.SALESFORCE, "sf", new TokenBucket(1, 1));
        connection = SampleConfig.salesforceConnection("acme", crm);
        process(event("c-1", ChangeType.CREATED, 1, contact("Lovelace", 100)));

        SyncTask task = new SyncTask(event("c-2", ChangeType.CREATED, 1, contact("Turing", 100)), connection);
        ProcessResult result = processor.process(task);

        assertEquals(ProcessResult.Kind.RETRY_LATER, result.kind());
        assertEquals(0, task.failedAttempts());
        assertEquals(1, metrics.get(SyncMetrics.DEFERRED_RATE_LIMIT));
    }

    @Test
    void crmThatKeepsThrottlingIsDeadLetteredAfterTheThrottleLimit() {
        crm = new MockCrmClient(SampleConfig.SALESFORCE, "sf", new TokenBucket(1, 0.001));
        connection = SampleConfig.salesforceConnection("acme", crm);
        processor = new SyncTaskProcessor(SampleConfig.internalSchemas(), stateStore,
                new RateLimiterRegistry(Map.of(), new RateLimit(1_000, 1_000)),
                new RetryPolicy(3, 10, 100, 3), deadLetters, metrics);
        process(event("c-1", ChangeType.CREATED, 1, contact("Lovelace", 100))); // uses the CRM's only token

        SyncTask task = new SyncTask(event("c-2", ChangeType.CREATED, 1, contact("Turing", 100)), connection);
        assertEquals(ProcessResult.Kind.RETRY_LATER, processor.process(task).kind());
        assertEquals(ProcessResult.Kind.RETRY_LATER, processor.process(task).kind());
        ProcessResult third = processor.process(task);

        assertEquals(ProcessResult.Kind.DONE, third.kind());
        assertEquals(0, task.failedAttempts());
        assertEquals(1, deadLetters.entries().size());
        assertTrue(deadLetters.entries().get(0).reason().contains("still throttled after 3 attempts"));
        assertEquals(1, crm.recordCount());
    }

    @Test
    void clientRateLimitBooksASlotOnceAndThenProceeds() {
        processor = newProcessor(new RateLimit(1, 1)); // 1 call per second
        process(event("c-1", ChangeType.CREATED, 1, contact("Lovelace", 100))); // uses the only token

        SyncTask task = new SyncTask(event("c-2", ChangeType.CREATED, 1, contact("Turing", 100)), connection);
        ProcessResult first = processor.process(task);
        assertEquals(ProcessResult.Kind.RETRY_LATER, first.kind());
        assertTrue(first.delayMillis() > 0);
        assertTrue(task.hasReservedPermit());

        // When it comes back it does not ask the limiter again: it already holds a booked slot.
        ProcessResult second = processor.process(task);
        assertEquals(ProcessResult.Kind.DONE, second.kind());
        assertEquals(2, crm.recordCount());
    }

    @Test
    void ruleThatDoesNotFireMeansNoApiCall() {
        process(event("c-1", ChangeType.CREATED, 1, contact("Private", "p@acme.com", 1, false)));

        assertEquals(0, crm.apiCallCount());
        assertEquals(1, metrics.get(SyncMetrics.SKIPPED_RULE));
    }
}
