package com.recordsync.demo;

import com.recordsync.crm.MockCrmClient;
import com.recordsync.model.ChangeEvent;
import com.recordsync.model.ChangeType;
import com.recordsync.pipeline.DeadLetterQueue;
import com.recordsync.pipeline.OutboundSyncPipeline;
import com.recordsync.pipeline.RetryPolicy;
import com.recordsync.pipeline.SyncLog;
import com.recordsync.ratelimit.RateLimiterRegistry;
import com.recordsync.ratelimit.RateLimiterRegistry.RateLimit;
import com.recordsync.ratelimit.TokenBucket;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Runs the outbound sync pipeline against two mock CRMs and walks through the main scenarios.
 * Run with: ./mvnw -q compile exec:java
 */
public class DemoApp {

    private static final String ACME = "acme";
    private static final String GLOBEX = "globex";
    private static final AtomicLong eventSequence = new AtomicLong();

    public static void main(String[] args) throws Exception {
        SyncLog.setEnabled(true);

        // Salesforce mock enforces its own limit of 3 calls/second per org; HubSpot has none.
        MockCrmClient salesforce = new MockCrmClient(SampleConfig.SALESFORCE, "sf", new TokenBucket(3, 3));
        MockCrmClient hubspot = new MockCrmClient(SampleConfig.HUBSPOT, "hs", null);

        // Our client-side budgets, per provider and per tenant. Salesforce's is deliberately a
        // little above the CRM's real limit, so the demo shows both our limiter and a 429 from the CRM.
        RateLimiterRegistry rateLimiters = new RateLimiterRegistry(
                Map.of(SampleConfig.SALESFORCE, new RateLimit(4, 4),
                        SampleConfig.HUBSPOT, new RateLimit(50, 50)),
                new RateLimit(10, 10));

        try (OutboundSyncPipeline pipeline = new OutboundSyncPipeline(
                List.of(SampleConfig.salesforceConnection(ACME, salesforce),
                        SampleConfig.hubspotConnection(ACME, hubspot),
                        SampleConfig.hubspotConnection(GLOBEX, hubspot)),
                SampleConfig.internalSchemas(),
                rateLimiters,
                new RetryPolicy(5, 100, 2_000),
                4)) {

            section("1. New contact c-1 is created -> created in Salesforce and HubSpot (different object models)");
            pipeline.submit(event(ACME, "c-1", ChangeType.CREATED, 1,
                    contact("Ada", "Lovelace", "ada@acme.com", "Acme", 5_000_000, "Lead", true)));
            pipeline.awaitIdle(10_000);
            System.out.println("  Salesforce c-1: " + salesforce.getRecord(externalId(pipeline, SampleConfig.SALESFORCE, "c-1")));
            System.out.println("  HubSpot    c-1: " + hubspot.getRecord(externalId(pipeline, SampleConfig.HUBSPOT, "c-1")));

            section("2. Five quick updates to c-1 -> updates still waiting in the queue are merged; only the newest is sent");
            for (int version = 2; version <= 6; version++) {
                pipeline.submit(event(ACME, "c-1", ChangeType.UPDATED, version,
                        contact("Ada", "Lovelace", "ada@acme.com", "Acme", 5_000_000 + version, "Opportunity", true)));
            }
            pipeline.awaitIdle(10_000);
            System.out.println("  Salesforce c-1 final revenue: "
                    + salesforce.getRecord(externalId(pipeline, SampleConfig.SALESFORCE, "c-1")).get("AnnualRevenue"));

            section("3. Same update delivered twice, and an old one arriving late -> skipped, no extra API calls");
            pipeline.submit(event(ACME, "c-1", ChangeType.UPDATED, 6,
                    contact("Ada", "Lovelace", "ada@acme.com", "Acme", 5_000_006, "Opportunity", true)));
            pipeline.submit(event(ACME, "c-1", ChangeType.UPDATED, 3,
                    contact("Ada", "Lovelace", "ada@acme.com", "Acme", 1, "Lead", true)));
            pipeline.awaitIdle(10_000);

            section("4. Salesforce creates c-2 but the response is lost (timeout) -> retry reuses the idempotency key, no duplicate");
            salesforce.loseNextCreateResponse();
            pipeline.submit(event(ACME, "c-2", ChangeType.CREATED, 1,
                    contact("Alan", "Turing", "alan@acme.com", "Acme", 100, "Lead", true)));
            pipeline.awaitIdle(10_000);
            String c2Id = externalId(pipeline, SampleConfig.SALESFORCE, "c-2");
            System.out.println("  Salesforce call log for " + c2Id + ":");
            for (String call : salesforce.callLog()) {
                if (call.contains(c2Id + " ")) {
                    System.out.println("    " + call);
                }
            }

            section("5. HubSpot returns 503 twice -> retried with exponential backoff, then succeeds");
            hubspot.injectTransientFailures(2);
            pipeline.submit(event(ACME, "c-3", ChangeType.CREATED, 1,
                    contact("Grace", "Hopper", "grace@acme.com", "Acme", 200, "Customer", true)));
            pipeline.awaitIdle(10_000);

            section("6. A change that our inbound sync wrote FROM Salesforce -> not sent back to Salesforce (loop prevention)");
            pipeline.submit(new ChangeEvent(nextEventId(), ACME, SampleConfig.CONTACT, "c-2", ChangeType.UPDATED, 2,
                    contact("Alan", "Turing", "alan@acme.com", "Acme Labs", 100, "Lead", true), SampleConfig.SALESFORCE));
            pipeline.awaitIdle(10_000);

            section("7. Record with an invalid email -> rejected by schema validation, sent to the dead-letter queue");
            pipeline.submit(event(ACME, "c-4", ChangeType.CREATED, 1,
                    contact("Bad", "Data", "not-an-email", "Acme", 1, "Lead", true)));
            pipeline.awaitIdle(10_000);

            section("8. Contact with sync_enabled=false -> Salesforce rule does not fire; HubSpot rule does");
            pipeline.submit(event(ACME, "c-5", ChangeType.CREATED, 1,
                    contact("Linus", "Private", "linus@acme.com", "Acme", 1, "Lead", false)));
            pipeline.awaitIdle(10_000);

            section("9. c-1 is deleted -> deleted in both CRMs");
            pipeline.submit(event(ACME, "c-1", ChangeType.DELETED, 7, Map.of()));
            pipeline.awaitIdle(10_000);

            section("10. Load: 300 new contacts for globex and 12 for acme -> each tenant uses its own rate-limit budget");
            SyncLog.setEnabled(false);
            long start = System.currentTimeMillis();
            for (int i = 1; i <= 300; i++) {
                pipeline.submit(event(GLOBEX, "g-" + i, ChangeType.CREATED, 1,
                        contact("User", "Globex" + i, "user" + i + "@globex.com", "Globex", i, "Lead", true)));
            }
            for (int i = 1; i <= 12; i++) {
                pipeline.submit(event(ACME, "bulk-" + i, ChangeType.CREATED, 1,
                        contact("User", "Acme" + i, "user" + i + "@acme.com", "Acme", i, "Lead", true)));
            }
            boolean finished = pipeline.awaitIdle(60_000);
            System.out.println("  Finished: " + finished + " in " + (System.currentTimeMillis() - start) + " ms");
            System.out.println("  (globex is paced by its own HubSpot budget of 50/sec; acme's Salesforce calls by its"
                    + " own budget of ~3-4/sec. Neither tenant's backlog uses up the other's quota.)");

            section("Summary");
            System.out.println("  Metrics:");
            for (Map.Entry<String, Long> entry : pipeline.metrics().snapshot().entrySet()) {
                System.out.println("    " + entry.getKey() + " = " + entry.getValue());
            }
            System.out.println("  Salesforce records: " + salesforce.recordCount() + ", API calls: " + salesforce.apiCallCount());
            System.out.println("  HubSpot records:    " + hubspot.recordCount() + ", API calls: " + hubspot.apiCallCount());
            System.out.println("  Dead-letter queue:");
            for (DeadLetterQueue.DeadLetter letter : pipeline.deadLetterQueue().entries()) {
                System.out.println("    " + letter.task() + " : " + letter.reason());
            }
        }
    }

    private static void section(String title) {
        System.out.println();
        System.out.println("=== " + title);
    }

    private static String externalId(OutboundSyncPipeline pipeline, String provider, String recordId) {
        return pipeline.stateStore().get(provider, ACME + "/" + SampleConfig.CONTACT + "/" + recordId).externalId();
    }

    private static ChangeEvent event(String tenant, String recordId, ChangeType type, long version, Map<String, Object> fields) {
        return new ChangeEvent(nextEventId(), tenant, SampleConfig.CONTACT, recordId, type, version, fields,
                ChangeEvent.SOURCE_INTERNAL);
    }

    private static String nextEventId() {
        return "evt-" + eventSequence.incrementAndGet();
    }

    private static Map<String, Object> contact(String first, String last, String email, String company,
                                               long revenue, String stage, boolean syncEnabled) {
        Map<String, Object> fields = new HashMap<>();
        fields.put("first_name", first);
        fields.put("last_name", last);
        fields.put("email", email);
        fields.put("company_name", company);
        fields.put("annual_revenue", revenue);
        fields.put("lifecycle_stage", stage);
        fields.put("sync_enabled", syncEnabled);
        fields.put("internal_notes", "internal only, never sent to a CRM");
        return fields;
    }
}
