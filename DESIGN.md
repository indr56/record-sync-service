# Record-to-Record Sync Service: Design

## Summary

- **Part built:** the **outbound path**, which syncs changes from the internal system (System A) to multiple CRMs (System B), record by record. This is where most of the stated requirements meet: rate-limited APIs, rules, validation, transformation, multiple providers (Section 4).
- **Key decisions** (Section 5):
  1. **Per-record ordering** by partitioning on provider + record, with a queue per record so other records progress during scheduled waits.
  2. **At-least-once delivery made safe by idempotency:** version check, idempotency key on create, payload fingerprint.
  3. **Rate-limit budget per (provider, tenant)**, with booked slots instead of repeated re-checks.
  4. **Merging superseded queued updates** to save API quota.
  5. **Error classes:** 429 waits, transient errors retry with backoff, permanent errors go to a dead-letter queue.
  6. **Loop prevention** using the change's `source`.
- **Designed, not built:** inbound sync, conflict resolution and reconciliation (Sections 3 and 7), and the production setup (Section 6).

## 1. Problem framing

We keep records (contacts, accounts, ...) in sync between:

- **System A, internal**: we own the services and storage.
- **System B, external CRMs** (Salesforce, HubSpot, Dynamics, ...): reachable only through their APIs, which are rate-limited and use their own data models.

Sync is bi-directional and record-by-record, at more than 300M sync requests a day, near real-time, with 99.9% availability. I built the **outbound path** (Section 4 explains why); Sections 3 and 6 to 8 place it in the whole system.

### What "correct" means for the outbound path

1. **Every relevant change reaches the CRM**, or ends up in a dead-letter queue with a reason. Nothing is silently lost.
2. **The CRM ends at the latest state** and never moves backwards. An older version must never overwrite a newer one.
3. **No duplicates** in the CRM, even when requests are retried or messages delivered twice.
4. **No tenant exceeds its CRM API limits**, and no tenant's backlog consumes another tenant's quota.
5. **No sync loops**: a change that came from a CRM must not be sent back to that CRM.

## 2. Numbers and assumptions

| | |
|---|---|
| Daily volume | 300M sync requests |
| Average rate | 300M / 86,400 s ≈ **3,500 per second** |
| Peak (assumed 5–10× average, business hours and bulk imports) | **≈ 20,000–35,000 per second** |
| Availability 99.9% | ≈ **43 minutes** of downtime a month |

The real bottleneck is not our compute but **CRM API quota**: CRMs typically allow a fixed number of calls per customer account per day, plus short-term throttling. So the design spends quota carefully: it skips no-ops, merges superseded updates and never retries blindly.

**Assumptions:**

1. **Change capture exists.** System A publishes an event for every committed change (transactional outbox or database change stream), with a **per-record version number that increases on every change**.
2. **Events carry a full snapshot** of the record, not a field diff, so a newer event can safely replace an older one.
3. **The event bus is at-least-once** (for example Kafka): events can arrive twice and, across partitions, out of order.
4. **Ordering matters per record, not globally.**
5. **Changes written by our own inbound sync carry a `source`** naming the CRM they came from.
6. **CRM create APIs support an idempotency key**, or an external-id upsert with the same effect.
7. **Mappings, schemas and rules are per-tenant configuration**, managed elsewhere. Here they are built in code (`SampleConfig`).
8. Authentication, token refresh and tenant onboarding are out of scope.

## 3. Whole-system overview

```
                          ┌──────────────── config service ────────────────┐
                          │ schemas · field mappings · sync rules (per tenant)│
                          └─────────────────────────────────────────────────┘
 SYSTEM A (internal)                                                         SYSTEM B (CRMs)
 ┌────────────┐  outbox / CDC   ┌──────────────────────┐   ┌──────────────────────────┐
 │ services + │ ───────────────►│ change topic         │──►│ OUTBOUND SYNC WORKERS    │──► Salesforce API
 │ database   │                 │ key = tenant+record  │   │ rules · validate ·       │──► HubSpot API
 └────────────┘                 └──────────────────────┘   │ transform · rate limit · │──► Dynamics API
       ▲                                                   │ CRM adapters             │
       │                                                   └───────────┬──────────────┘
       │                                                               │
       │                         ┌──────────────────────┐   sync state store (id map, versions, hashes)
       │  apply (source=crm)     │ INBOUND SYNC WORKERS │   rate-limit budgets (shared, e.g. Redis)
       └─────────────────────────│ validate · transform │   dead-letter queue + replay tool
                                 │ conflict resolution  │◄── webhooks / CRM change streams / polling
                                 └──────────────────────┘
                     reconciliation job (READ): periodic compare A vs B, repair drift
```

- **Outbound** (built here): change event → rules → validate → transform → rate limit → CRM API.
- **Inbound** (designed only): CRM notifications or polling → validate → transform to the internal model → conflict resolution → write to System A, tagged `source=<crm>` so outbound will not echo it back.
- **Read** in CRUD is used by the inbound path, by conflict checks and by a reconciliation job that periodically compares both sides and repairs drift (a missed webhook, a manual edit in the CRM). On the outbound path a read changes no data in System A, so there is nothing to send: the pipeline carries only create, update and delete events.

## 4. Why the outbound path

The task suggests examples: syncing updates internal → external, conflict resolution or schema transformation. I chose **internal → external** because it is where most of the stated requirements meet:

- **rate-limited external APIs**: quota per tenant, throttling, 429 responses
- **pre-configured rules and triggers** that decide CRUD actions
- **schema validation** of input and output
- **transformation** into each CRM's object model
- **multiple CRM providers** behind one pipeline
- **record-by-record** processing at high volume with **ordering, retries and idempotency**

Schema transformation is one step of it. Conflict resolution belongs mostly to the inbound path; Section 7 covers it.

## 5. Design of the outbound pipeline (implemented)

### Flow

```
ChangeEvent ─► fan out: one task per CRM connection of the tenant
            ─► partition by hash(provider + record)  ──►  single-threaded partition worker
                                                          per-record queue (order + merging)
                                                                     │
                                       SyncTaskProcessor, for the task at the front of a record's queue:
  1 loop prevention   source == this CRM?                   → skip
  2 stale/duplicate   version <= last synced version?       → skip
  3 validate input    internal schema                       → dead-letter if invalid
  4 decide action     rules + "already in CRM?"             → CREATE / UPDATE / DELETE / SKIP
  5 transform         internal model → CRM model, validate output → dead-letter if invalid
  6 no-op check       same payload fingerprint as last sync? → skip, record version
  7 rate limit        book a slot in (provider, tenant) budget → wait if in the future
  8 call CRM          create carries an idempotency key
                      429 → wait (not a failure) · 5xx/timeout → retry with backoff · 4xx → dead-letter
  9 record state      external id, version, payload fingerprint
```

### Key decisions and trade-offs

**1. Ordering: per record, via partitioning.**
- Options: global ordering (does not scale), version checks only, or per-record ordering.
- Chosen: route each task by `hash(provider + record)` to one single-threaded partition (in production, a Kafka topic keyed by record). The version check (step 2) catches anything still out of order.

**2. Other records progress during scheduled retry and rate-limit waits.**
- Chosen: a **queue per record** inside each partition. A waiting task waits on a timer, not a sleeping thread, so other records keep moving, while later changes to the *same* record stay behind it, in order.
- Limit: the CRM call itself is synchronous on the partition's worker thread, so a slow call still delays the other records in that partition for its duration. Production would use asynchronous calls with a cap on in-flight calls per partition, keeping the per-record order.

**3. Delivery: at-least-once plus idempotency, not exactly-once.**
- Exactly-once to a third-party API is not achievable: a request can succeed while its response is lost. So tasks retry until confirmed, made safe by:
  - **version check:** events at or below the last synced version are skipped
  - **idempotency key on create** (`provider|record|version`): a retry after a lost response returns the same CRM record (shown in the demo)
  - **payload fingerprint:** no update is sent if the mapped fields did not change

**4. Rate limiting: budget per (provider, tenant), with reservations.**
- Options: react to 429s only (wasteful, some CRMs penalise it); one limit per provider (a busy tenant starves others); a budget per tenant (CRM quotas are per account anyway).
- Chosen: a **token bucket per (provider, tenant)**. A task that cannot go now **books the next free slot** and wakes exactly once, instead of all waiting tasks re-checking whenever a token appears (a "thundering herd").
- CRM 429s wait for the CRM's `Retry-After` and do **not** count as a failed attempt. They have their own, much higher limit (100 by default), then dead-letter, so a CRM that never stops throttling cannot keep a record waiting forever.
- Production: budgets in a shared store such as Redis, also tracking the CRM's remaining daily quota.

**5. Merging superseded updates (coalescing).**
- Each event is a full snapshot, so of several queued creates/updates for one record only the newest needs sending. In the tests, a create plus 19 queued updates reach the CRM in at most 3 API calls.
- Guards: only a **newer** version replaces an older one; **deletes are never merged away**; a task **already attempted** is never replaced; a booked rate-limit slot passes to the replacing task.
- Trade-off: intermediate states never reach the CRM. If a CRM needed every transition, that would be a per-connection setting.

**6. Retries and failure classification.**
- **429:** wait, not a failure, bounded by its own limit. **Transient** (timeouts, 5xx): exponential backoff with jitter, bounded, then dead-letter. **Permanent** (4xx): dead-letter immediately.
- The **dead-letter queue** keeps each task and its reason, so it can be fixed and replayed.

**7. Loop prevention.**
- Inbound writes carry `source=<crm>`; outbound skips events whose source is the target CRM, but still sends them to the tenant's other CRMs (demo scenario 6). The payload fingerprint is a second line of defence.
- Alternative considered: timestamps or "last modified by integration user" fields. Weaker, because clocks and field semantics differ per CRM.

**8. Rules, transformation and validation as configuration.**
- Rules choose which object and change types sync, with an optional field condition (for example `sync_enabled == true`). The action follows from the change type and whether the record already exists in the CRM.
- Mappings are field renames plus small value converters, per CRM object model.
- Validation happens **twice**: input against the internal schema, output against the CRM schema before the call, catching bad mappings without spending an API call.
- A new CRM provider needs only a `CrmClient` adapter and configuration.

## 6. Scale and availability (production shape)

| This implementation | Production |
|---|---|
| `OutboundSyncPipeline.submit` | Kafka consumer group reading the change topic |
| `PartitionWorker` (one thread) | one Kafka partition consumed by one worker |
| per-record queue + timers | same, plus a "delay" retry topic for long waits so the partition offset can still be committed |
| `SyncStateStore` (in memory) | DynamoDB, Cassandra or sharded Postgres, keyed by (tenant, provider, object, record) |
| `RateLimiterRegistry` (in memory) | shared token buckets in Redis, or quota leased to workers |
| `DeadLetterQueue` | a dead-letter topic or table, with a replay tool and alerts |
| `SyncMetrics` | Prometheus/StatsD: lag, sync latency, 429 rate, retries, DLQ growth per tenant/provider |
| `MockCrmClient` | one adapter per CRM (auth, pagination, bulk endpoints, error mapping) |

**Throughput.** Workers are stateless apart from in-flight work, so throughput scales with partitions and instances. Sizing for the ~35k/s peak assumed in Section 2 depends on measured CRM API latency, calls in flight per worker, fan-out per event and CRM quotas, and would be set by load testing. The proposed production workers make CRM calls asynchronously; this implementation calls synchronously. In practice per-tenant CRM quotas usually cap throughput first.

**99.9% availability, by failure:**
- **Worker crash:** the partition moves to another worker; redelivered events are safe because of idempotency.
- **CRM outage or throttling for one tenant:** only that tenant backs off, thanks to separate budgets and timer-based waits. A per-(tenant, provider) circuit breaker would pause calls to a CRM that is fully down.
- **Poison message:** dead-lettered after classification or bounded retries, so it never blocks a record queue forever.
- **State store unavailable:** processing pauses rather than risk duplicates. The store is replicated across availability zones.
- **Deployment:** rolling restarts; Kafka holds the backlog.

**Near real-time** is a target: end-to-end latency is the event bus delay plus queueing plus the CRM's response time, and it depends on quota being available. It needs to be confirmed by load testing. When a tenant is over quota, latency grows for that tenant only, visible in its lag metric.

## 7. Conflict resolution (designed, not built)

A conflict is when the same record changes on both sides before either change has synced.

- **Detect:** we store the last version we synced *and* the CRM's last-modified marker (for example `SystemModstamp` or an etag). If the CRM's marker changed since our last sync, the CRM has an unsynced change.
- **Resolve, configurable per tenant and object:**
  - **field-level last-writer-wins:** merge fields, the newest change per field wins. Needs per-field timestamps.
  - **source of truth per field:** for example, `email` is owned by System A and `stage` is owned by the CRM. Simple, predictable, and what most customers want.
  - **flag for review** for high-value objects.
- Applied on the inbound path. On the outbound path, a conditional update (only if the CRM version matches) prevents silently overwriting a CRM-side change, where the CRM API supports it.

## 8. Next steps and known limitations

**What I would add next:**

1. Durable state and a Kafka-based runner (Section 6), with a delay topic for long waits.
2. A circuit breaker per (tenant, provider), and quota-aware scheduling.
3. Bulk CRM endpoints for backlogs: still record-by-record results, with far fewer API calls.
4. The inbound path with conflict resolution (Section 7) and the reconciliation job.
5. Field-level change detection, so an update sends only changed fields where the CRM supports partial updates.
6. A dead-letter replay tool and per-tenant dashboards.

**Known limitations of this implementation.** These are deliberate simplifications in this implementation and follow-ups for a production version:

- **Backpressure:** the in-memory queues are unbounded. In production, Kafka holds the backlog and consumers pause when workers fall behind.
- **Record that stops matching a rule:** it stops being updated but stays in the CRM. Whether to delete it, leave it or flag it should be a per-rule setting.
- **Kafka offset commits:** records finish out of order inside a partition, so an offset can be committed only up to the oldest unfinished event. That needs a per-partition watermark, not built here.
- **Security and tenant isolation:** CRM credentials (OAuth tokens) in a secrets store, encryption of synced data, and strict per-tenant separation are not covered.
- **Full-snapshot updates:** an update sends every mapped field, so it can overwrite a field edited directly in the CRM (item 5 above, and conditional updates in Section 7).
- **Mapping and schema versioning:** changing a mapping does not re-sync existing records, and mapping versions are not tracked.
- **Testing:** unit and concurrency tests only. Production also needs contract tests against CRM sandboxes, load tests and failure-injection tests.

## 9. Mapping to the code

| Concept | Where |
|---|---|
| Fan-out, partitioning | `pipeline/OutboundSyncPipeline` |
| Per-record ordering, progress during scheduled waits, merging | `pipeline/PartitionWorker` |
| Steps 1–9 | `pipeline/SyncTaskProcessor` |
| Rules / triggers | `rules/SyncRule`, `rules/RuleEngine` |
| Schemas and validation | `schema/Schema` |
| Transformation | `transform/ObjectMapping`, `transform/FieldMapping`, `transform/Converters` |
| Rate limiting | `ratelimit/TokenBucket`, `ratelimit/RateLimiterRegistry` |
| Retries | `pipeline/RetryPolicy` |
| Sync state (id map, versions) | `state/SyncStateStore` |
| CRM adapter boundary, error classes | `crm/CrmClient`, `crm/*Exception` |
| Mock CRM (throttling, failures, lost responses, idempotency) | `crm/MockCrmClient` |
