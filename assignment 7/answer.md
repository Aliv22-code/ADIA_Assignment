# Point-in-time-ness

Point-in-time (PIT) data means that, for any prediction timestamp `t`, the model only sees information that was actually known at or before `t`. This is essential because most model failures in finance and macro forecasting come from subtle look-ahead leakage, not from model architecture.

This question is related to my previous assignments as follows:

- **Directly related: Assignment 1 (tick-data financial modeling).**  
Assignment 1 computes return statistics and comparisons across bar types. If timestamps are not treated in a strict as-of manner (for example, using future-known values or future bars), all conclusions become biased.
- **Partially related: Assignment 3 and Assignment 4 (ingestion pipelines).**  
These assignments focus on ingestion and data quality. PIT principles apply to how we store `ingested_at`, preserve historical versions, and avoid overwriting old states.
- **Weakly related: Assignment 2 (API enrichment).**  
This is mostly extraction logic, but PIT still matters if enriched attributes are used later for modeling (need to know *when* they became available).

In practical terms, PIT discipline protects against three major errors:

1. **Look-ahead bias** - using revised or future-known values (for example, final macro releases instead of initial releases available on the forecast date).
2. **Survivorship bias** - training on entities that are alive today and ignoring those that disappeared (delisted stocks, bankrupt firms, merged entities).
3. **As-of mismatch** - joining features and targets with the wrong effective dates, so the model gets information that became available later.

For financial and event-study use cases, PIT is non-negotiable. If event returns, fundamentals, and macro controls are not aligned to "what was known when," backtests become overly optimistic and signals do not survive in production. A model can appear statistically strong but fail immediately when moved to live data.

If I were building a PIT database, I would use these design principles:

- **Dual timestamps everywhere**:
  - `event_time`: when the real-world event happened
  - `available_time`: when that record/version became available to the model
- **Versioned storage (bitemporal)**:
  - keep full history of revisions, never overwrite in place
  - each row carries `valid_from`, `valid_to`, `loaded_at`, `source_version`
- **As-of query interface**:
  - every training set built with `as_of_time`
  - query returns the latest row where `available_time <= as_of_time`
- **Entity lifecycle handling**:
  - include delisted/inactive entities
  - maintain security/entity mapping history over time
- **Dataset reproducibility**:
  - store data snapshot IDs and feature view definitions
  - make train/test rebuildable exactly for audit
- **Leakage tests as CI checks**:
  - fail pipeline if any feature timestamp exceeds target timestamp
  - monitor freshness and missingness by effective date

A concrete implementation pattern:

1. Raw ingestion tables append every source update with ingestion metadata.
2. Normalized history tables keep revision chains per entity/field.
3. Feature views expose "as-of" snapshots for model training and backtesting.
4. Backtest engine only queries through these PIT views, never raw latest tables.

In short, point-in-time-ness is less a single table and more a system property: timestamp discipline, revision preservation, and strict as-of querying. Without it, estimated model performance is usually overstated and not production-realistic.