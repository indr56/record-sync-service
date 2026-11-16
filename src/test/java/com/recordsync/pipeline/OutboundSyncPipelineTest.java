package com.recordsync.pipeline;

import com.recordsync.crm.MockCrmClient;
import com.recordsync.demo.SampleConfig;
import com.recordsync.model.ChangeEvent;
import com.recordsync.model.ChangeType;
import com.recordsync.ratelimit.RateLimiterRegistry;
import com.recordsync.ratelimit.RateLimiterRegistry.RateLimit;
import com.recordsync.ratelimit.TokenBucket;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

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
 * End-to-end tests of the asynchronous pipeline: partitions, worker threads, retries and rate limits together.
 */
class OutboundSyncPipelineTest {

    private OutboundSyncPipeline pipeline;

    @AfterEach
    void tearDown() {
        if (pipeline != null) {
            pipeline.close();
        }
    }

    private OutboundSyncPipeline newPipeline(List<CrmConnection> connections, RateLimit salesforceLimit) {
        RateLimiterRegistry limits = new RateLimiterRegistry(
                Map.of(SampleConfig.SALESFORCE, salesforceLimit), new RateLimit(1_000, 1_000));
        return new OutboundSyncPipeline(connections, SampleConfig.internalSchemas(), limits,
                new RetryPolicy(10, 5, 50), 4);
    }

    private String externalId(String provider, String recordId) {
        return pipeline.stateStore().get(provider, recordKey(recordId)).externalId();
    }

    @Test
    void eachChangeFansOutToEveryCrmOfTheTenant() throws Exception {
        MockCrmClient salesforce = new MockCrmClient(SampleConfig.SALESFORCE, "sf", null);
        MockCrmClient hubspot = new MockCrmClient(SampleConfig.HUBSPOT, "hs", null);
        pipeline = newPipeline(List.of(
                SampleConfig.salesforceConnection("acme", salesforce),
                SampleConfig.hubspotConnection("acme", hubspot)), new RateLimit(1_000, 1_000));

        pipeline.submit(event("c-1", ChangeType.CREATED, 1, contact("Lovelace", 100)));

        assertTrue(pipeline.awaitIdle(5_000));
        assertEquals("Lovelace", salesforce.getRecord(externalId(SampleConfig.SALESFORCE, "c-1")).get("LastName"));
        assertEquals("Lovelace", hubspot.getRecord(externalId(SampleConfig.HUBSPOT, "c-1")).get("lastname"));
    }

    @Test
    void writesForEachRecordReachTheCrmInVersionOrderDespiteThrottlingAndFailures() throws Exception {
        // The CRM throttles hard and also fails some calls; ordering must still hold for every record.
        MockCrmClient salesforce = new MockCrmClient(SampleConfig.SALESFORCE, "sf", new TokenBucket(20, 200));
        salesforce.injectTransientFailures(15);
        pipeline = newPipeline(List.of(SampleConfig.salesforceConnection("acme", salesforce)),
                new RateLimit(30, 300));

        int records = 30;
        int versionsPerRecord = 8;
        for (int version = 1; version <= versionsPerRecord; version++) {
            for (int r = 1; r <= records; r++) {
                ChangeType type = version == 1 ? ChangeType.CREATED : ChangeType.UPDATED;
                pipeline.submit(event("c-" + r, type, version, contact("Name" + r, version)));
            }
        }

        assertTrue(pipeline.awaitIdle(30_000));
        assertEquals(records, salesforce.recordCount());
        assertEquals(0, pipeline.deadLetterQueue().size());

        for (int r = 1; r <= records; r++) {
            String externalId = externalId(SampleConfig.SALESFORCE, "c-" + r);
            List<Map<String, Object>> writes = salesforce.writesFor(externalId);
            long previous = 0;
            for (Map<String, Object> write : writes) {
                long revenue = (Long) write.get("AnnualRevenue"); // revenue == version in this test
                assertTrue(revenue > previous, "record c-" + r + " received v" + revenue + " after v" + previous);
                previous = revenue;
            }
            assertEquals(versionsPerRecord, previous, "record c-" + r + " must end on the latest version");
        }
    }

    @Test
    void queuedUpdatesAreMergedSoFewerApiCallsAreMade() throws Exception {
        // 1 call per second: while the first write waits, the rest pile up and get merged.
        MockCrmClient salesforce = new MockCrmClient(SampleConfig.SALESFORCE, "sf", null);
        pipeline = newPipeline(List.of(SampleConfig.salesforceConnection("acme", salesforce)),
                new RateLimit(1, 1));

        pipeline.submit(event("c-1", ChangeType.CREATED, 1, contact("Lovelace", 1)));
        for (int version = 2; version <= 20; version++) {
            pipeline.submit(event("c-1", ChangeType.UPDATED, version, contact("Lovelace", version)));
        }

        assertTrue(pipeline.awaitIdle(10_000));
        String externalId = externalId(SampleConfig.SALESFORCE, "c-1");
        assertEquals(20L, salesforce.getRecord(externalId).get("AnnualRevenue"));
        assertTrue(salesforce.apiCallCount() <= 3, "expected the updates to be merged, but made "
                + salesforce.apiCallCount() + " calls");
        assertTrue(pipeline.metrics().get(SyncMetrics.COALESCED) >= 17);
    }

    @Test
    void deleteIsNeverMergedAway() throws Exception {
        MockCrmClient salesforce = new MockCrmClient(SampleConfig.SALESFORCE, "sf", null);
        pipeline = newPipeline(List.of(SampleConfig.salesforceConnection("acme", salesforce)),
                new RateLimit(1, 2));

        pipeline.submit(event("c-1", ChangeType.CREATED, 1, contact("Lovelace", 1)));
        pipeline.submit(event("c-1", ChangeType.UPDATED, 2, contact("Lovelace", 2)));
        pipeline.submit(delete("c-1", 3));

        assertTrue(pipeline.awaitIdle(10_000));
        assertEquals(0, salesforce.recordCount());
        assertNull(pipeline.stateStore().get(SampleConfig.SALESFORCE, recordKey("c-1")).externalId());
        assertEquals(1, pipeline.metrics().get(SyncMetrics.SYNCED_DELETE));
    }

    @Test
    void aBusyTenantDoesNotConsumeAnotherTenantsBudget() throws Exception {
        MockCrmClient salesforce = new MockCrmClient(SampleConfig.SALESFORCE, "sf", null);
        pipeline = newPipeline(List.of(
                SampleConfig.salesforceConnection("busy", salesforce),
                SampleConfig.salesforceConnection("quiet", salesforce)), new RateLimit(5, 5));

        // The busy tenant queues 50 changes: about 9 seconds of its 5-per-second budget.
        for (int i = 1; i <= 50; i++) {
            pipeline.submit(new ChangeEvent("b" + i, "busy", SampleConfig.CONTACT, "b-" + i,
                    ChangeType.CREATED, 1, contact("Busy" + i, i), null));
        }
        long start = System.currentTimeMillis();
        pipeline.submit(new ChangeEvent("q1", "quiet", SampleConfig.CONTACT, "q-1",
                ChangeType.CREATED, 1, contact("Quiet", 1), null));

        // The quiet tenant's record goes through straight away, without waiting behind the busy backlog.
        long deadline = start + 2_000;
        while (pipeline.stateStore().get(SampleConfig.SALESFORCE, "quiet/contact/q-1") == null
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertTrue(pipeline.stateStore().get(SampleConfig.SALESFORCE, "quiet/contact/q-1") != null,
                "the quiet tenant was blocked by the busy tenant");
    }
}
