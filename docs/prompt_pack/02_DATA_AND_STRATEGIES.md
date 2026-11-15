# PHASE 02 — Synthetic data and publication strategies

Build deterministic PySpark synthetic datasets with business keys, dataset_scope, numeric/string/categorical payload, controlled skew, null distribution, duplicate-key negative cases, stable seed, payload width, selected fraction of business-value changes and full-snapshot row counts. Generate independently staged publication inputs **before** timed transaction, and optionally report generation cost separately.

Schema: `dataset_scope`, `business_key`, payload, `effective_from_date TIMESTAMP`, `effective_to_date TIMESTAMP`, `publication_id STRING`, `publication_sequence LONG`, `publication_ts TIMESTAMP`; exact payload schema documented. Use strong uniqueness `(dataset_scope, business_key)` in input snapshots. Timestamp must be strictly increasing in logical order per scope.

Implement strategies:
A `scd2_full`: for each table in transaction, UPDATE active rows for scope to publication_ts and INSERT entire staged snapshot as new active versions; atomic ledger state included. Every row is versioned, even if payload unchanged.
B `append_only_full`: INSERT entire staged snapshot; no update of older rows; use ledger for latest publication selection, and window/LEAD to project effective-to for history queries.
C optional `scd2_changed_only` clearly separated and disabled by default; closes/versions only changed keys. Do not mislabel C as mandated full-snapshot semantics.

For proper comparison same deterministic data, same publication sizes, same projection queries, equivalent validation and identical publication sequencing. Exclude one-time table construction/data generation from timed publication; report warm vs cold, cache effects, compaction and table history. No accidental file-layout differences; if unavoidable, record them.

Acceptance: deterministic contents under same seed, correct full snapshot count, no duplicate keys in valid inputs, meaningful invalid inputs, strategy-specific small fixture tests.
