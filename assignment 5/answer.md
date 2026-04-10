# Statistical Biases

Statistical bias refers to systematic errors in data collection, sampling, or analysis that cause results to consistently deviate from the true value. Unlike random errors, biases skew results in a particular direction.Bias in datasets is often not obvious: data can look complete and still push models toward wrong conclusions.  
Using examples from Assignments 1–4, these are the most relevant bias types and how to correct them.

## 1) Temporal / Look-ahead Bias (Assignment 1)

- **What it is:** Features include information that would not have been available at prediction time.
- **Example:** In tick-data modeling, using future ticks (or non-point-in-time joins) can artificially improve return predictability.
- **Correction:** Strict time-based splits, as-of joins, and leakage checks (`feature_time <= prediction_time`).

## 2) Survivorship Bias (Assignment 1)

- **What it is:** Missing entities that dropped out (delisted/expired) makes results look too good.
- **Example:** Evaluating only currently active instruments or “clean” periods can overstate robustness of a trading signal.
- **Correction:** Preserve full historical universe, include delisted/rolled-out contracts, and test across regimes.

## 3) Selection / Coverage Bias (Assignment 2, Assignment 4)

- **What it is:** Upstream extraction only captures a subset of the true population.
- **Example:** API-extracted entities (Assignment 2) depend on endpoint coverage/token status; metadata completeness in zipped JSON papers (Assignment 4) varies by source.
- **Correction:** Track extraction coverage, quantify drop-off by source/time, and design fallback/retry pipelines.

## 4) Measurement / Parsing Bias (Assignment 3, Assignment 4)

- **What it is:** Inconsistent formats create systematic measurement errors.
- **Example:** Date and numeric parsing issues (`N/A`, `#REF!`, mixed date formats) in Assignment 3; inconsistent JSON fields in Assignment 4.
- **Correction:** Canonical parsers, schema contracts, strict type validation, and row-level quality flags.

## 5) Missing-Data Bias (Assignment 3, Assignment 4)

- **What it is:** Missingness is non-random and changes model behavior.
- **Example:** Missing `stockid/broker/measure` in Assignment 3 or absent author/email fields in Assignment 4 can skew downstream aggregates.
- **Correction:** Missingness profiling by source/time, indicator features, threshold-based rejection/quarantine, and imputation only with documented assumptions.

## 6) Duplicate / Retry Bias (Assignment 3)

- **What it is:** Re-sent files or retries inflate record counts.
- **Example:** Duplicate stock-loan rows can overweight certain entities or dates.
- **Correction:** Idempotent ingestion (`INSERT OR IGNORE` / key hash), duplicate-rate monitoring, and post-load reconciliation against expected counts.

## 7) Drift Bias (Assignment 1, Assignment 3)

- **What it is:** Statistical properties change over time, so historical fit no longer generalizes.
- **Example:** Return distribution and volatility regimes shift in tick data (Assignment 1); source feed quality shifts in ingestion streams (Assignment 3).
- **Correction:** Rolling-window validation, drift monitors, and periodic retraining/recalibration.

## Practical Correction Framework

1. **Detect bias early:** profile coverage, nulls, duplicates, schema stability, and time leakage.
2. **Design robust validation:** temporal splits for time series, stratified checks for source/entity groups.
3. **Instrument production monitoring:** freshness, quality KPIs, drift, and source-target reconciliation.
4. **Govern changes:** keep lineage/versioning, document assumptions, and maintain incident runbooks.

In summary, bias management is continuous: detect, quantify, mitigate, and monitor.  
Across Assignments 1–4, the same lesson holds: reliable models require reliable, point-in-time-correct, quality-controlled data.