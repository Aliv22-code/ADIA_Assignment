# Data Monitoring

The biggest difficulty in monitoring ingestion pipelines is that failures are often silent: pipelines can complete successfully while still producing bad data (missing rows, schema-shifted fields, stale snapshots, or duplicated records). In my experience, monitoring only infrastructure (CPU, memory, job status) is not enough; we must monitor data quality, timeliness, and business meaning end-to-end.

From the assignments I worked on, the main pitfalls and monitoring approach are:

## 1) Freshness and latency failures
- **Risk:** Data arrives late, partial, or outside expected cadence.
- **Seen in practice:** Trade dates can be older than ingestion time (`late_arrival`) **(Assignment 3)**.
- **Monitor:** ingestion lag (`ingested_at - event_time`), SLA misses, delayed file arrivals.
- **Action:** alert on SLA breach, quarantine late partitions, mark data quality status, trigger replay/backfill.

## 2) Schema drift and parsing breakage
- **Risk:** Upstream changes headers, types, or formats without notice.
- **Seen in practice:** CSV/JSON ingestion can break when source schema changes **(Assignment 3, Assignment 4)**.
- **Monitor:** expected-vs-actual schema checks, missing/extra columns, type-cast failure rates.
- **Action:** fail fast for breaking changes, auto-map safe renames, route incompatible batches to quarantine, notify data owners.

## 3) Nulls, invalid values, and distribution shifts
- **Risk:** Data loads successfully but quality degrades silently.
- **Seen in practice:** Invalid numerics (`N/A`, `#REF!`), bad dates, missing keys **(Assignment 3)**.
- **Monitor:** null ratio by column, invalid parse counts, range checks, percentiles, and drift metrics against rolling baseline.
- **Action:** enforce thresholds, block publication when quality gates fail, publish row-level flags for downstream filtering.

## 4) Duplicates and idempotency gaps
- **Risk:** Retries or re-sends inflate counts and corrupt aggregates.
- **Seen in practice:** Duplicate-row handling required natural key/hash dedup logic **(Assignment 3)**.
- **Monitor:** duplicate rate by natural key/hash, row-count deltas per run, retry volume.
- **Action:** idempotent writes (`UPSERT` / `INSERT OR IGNORE`), dedup jobs, replay-safe orchestration.
- **Note:** In practice, a post-load row-count reconciliation against batch manifest was a simple but highly effective gate.

## 5) External dependency and API instability
- **Risk:** Token expiry, rate limits, and 4xx/5xx responses create silent data gaps.
- **Seen in practice:** API extraction behavior depends on auth state and endpoint availability **(Assignment 2)**.
- **Monitor:** API status-code breakdown, timeout rate, retry exhaustion, token health.
- **Action:** exponential backoff, circuit-breakers, token rotation checks, fallback queues for deferred processing.

## 6) Time alignment and leakage risk
- **Risk:** Using information not available at decision time causes optimistic model performance.
- **Seen in practice:** Financial workflows require strict event-time discipline **(Assignment 1)**.
- **Monitor:** timestamp lineage (`event_time`, `available_time`, `ingested_at`), as-of validation tests, leakage checks in feature builds.
- **Action:** enforce point-in-time joins and block datasets violating temporal constraints.

## Minimum monitoring stack
- **Pipeline health:** job success, runtime, retries, queue depth.
- **Freshness:** lag, missing partitions/files, SLA adherence.
- **Quality:** null/invalid/duplicate rates, schema compatibility.
- **Volume:** expected vs actual row counts with anomaly bands.
- **Business checks:** source-target reconciliations.
- **Lineage:** source version, ingestion timestamp, transformation version.

## Practical operating model
- Define severity levels (P1/P2/P3) with clear ownership and response SLAs.
- Separate quarantine and published zones so bad data never silently reaches consumers.
- Keep automated validation gates before publish (no data moves forward without quality checks).
- Maintain runbooks for common incidents: late file, schema drift, API outage, duplicate spike.
- Track quality scorecards over time to catch slow degradation that point-in-time alerts miss.

In short, good data monitoring is not only about detecting failures; it is about preventing bad data from propagating, preserving trust in downstream analytics/models, and enabling fast, deterministic recovery.
