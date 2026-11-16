# Record Sync Service: Outbound Sync Pipeline

Take-home exercise: design a bi-directional record synchronization service between an internal system and external CRMs, and implement one key part of it.

This repository implements **one key part**: the **outbound pipeline that syncs changes from the internal system to multiple CRMs**, record by record. **Read [DESIGN.md](DESIGN.md) first**: it covers the framing, assumptions, alternatives, trade-offs and the design of the rest of the system.

## What it does

For every change to an internal record, for every CRM the tenant has connected, the pipeline:

- skips loops, duplicates, stale events and no-op updates
- validates the input, applies pre-configured rules (create / update / delete / skip), transforms to the CRM's object model and validates the output
- stays within a rate-limit budget per (CRM provider, tenant)
- calls the CRM: 429 waits, timeouts and 5xx retry with backoff, permanent errors go to a dead-letter queue
- keeps per-record order, merges superseded queued updates, and uses idempotency keys so a retried create never makes a duplicate

Two mock CRMs with different object models (Salesforce-like and HubSpot-like) stand in for System B. They can throttle, fail and lose responses on demand.

## Requirements

- **Java 17 or newer** (`java -version` to check).
- Nothing else. The Maven Wrapper (`mvnw`) downloads Maven on first run. No Docker, database or message broker: everything runs in memory.

The main code uses only the JDK. The only dependency is JUnit 5, for tests.

## Run

macOS / Linux:

```bash
./mvnw test                   # run all tests
./mvnw -q compile exec:java   # run the demo
```

Windows:

```bat
mvnw.cmd test
mvnw.cmd -q compile exec:java
```

The first run downloads Maven and JUnit, so it takes a little longer.

## The demo

Ten scenarios, printed step by step:

1. A new contact is created in both CRMs, each in its own object model.
2. Five quick updates: only the newest reaches the CRM.
3. A duplicate and a late, older event are skipped with no API calls.
4. A lost create response: the retry reuses the idempotency key, so no duplicate.
5. HubSpot returns 503 twice: retried with backoff, then succeeds.
6. A change that came *from* Salesforce is not sent back to it, but still goes to HubSpot.
7. Invalid data (bad email) is dead-lettered by schema validation.
8. `sync_enabled=false`: the Salesforce rule does not fire, the HubSpot rule does.
9. A delete reaches both CRMs.
10. Load: 312 contacts across two tenants, each paced by its own rate-limit budget.

It ends with a summary of metrics, record counts, API calls and the dead-letter queue. Timings vary slightly between runs because of rate limits and randomised retry delays.

## Tests

`./mvnw test` runs 42 tests:

- `SchemaTest`, `ObjectMappingTest`, `RuleEngineTest`: validation, mapping to two CRM models, rule decisions.
- `TokenBucketTest`, `RetryPolicyTest`: rate limiting with slot reservations, backoff with jitter.
- `SyncTaskProcessorTest`: each pipeline step, including lost responses, retries, dead-lettering and 429 handling.
- `OutboundSyncPipelineTest` (end-to-end): fan-out, **per-record order** under throttling and failures, merging, deletes never merged, a busy tenant does not block a quiet one.

## Project layout

```
src/main/java/com/recordsync/
  model/       ChangeEvent, ChangeType, SyncAction
  rules/       SyncRule, RuleEngine                         pre-configured triggers -> CRUD action
  schema/      Schema, FieldSpec, FieldType                 input/output validation
  transform/   ObjectMapping, FieldMapping, Converters      internal model <-> CRM model
  ratelimit/   TokenBucket, RateLimiterRegistry             budget per (provider, tenant)
  state/       SyncStateStore, SyncState                    id map, last version, payload fingerprint
  crm/         CrmClient, MockCrmClient, error types        CRM adapter boundary
  pipeline/    OutboundSyncPipeline                         fan-out + partitioning
               PartitionWorker                              per-record order, merging, timers
               SyncTaskProcessor                            the sync steps 1-9
               RetryPolicy, DeadLetterQueue, SyncMetrics, SyncLog
  demo/        DemoApp, SampleConfig                        demo and sample tenant configuration
src/test/java/com/recordsync/                               unit and end-to-end tests
DESIGN.md                                                   design document
```

## Scope and simplifications

Deliberately simplified here; DESIGN.md (Section 6) describes the production version:

- Event bus, sync state, rate-limit budgets and dead-letter queue are in memory (production: Kafka, a durable key-value store, Redis, a dead-letter topic).
- Mappings, schemas and rules are built in code (`SampleConfig`), not loaded from per-tenant configuration.
- CRMs are mocked; real adapters add authentication, error mapping and bulk endpoints.
- Inbound sync, conflict resolution and reconciliation are designed, not implemented.
