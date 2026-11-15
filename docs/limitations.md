# Limitations and assumptions

## Unverified in this repository (NOT_RUN until executed on Databricks)

* `BEGIN ATOMIC` behaviour on the target runtime: single-call commit, rollback on SIGNAL and on real errors, read-your-own-writes inside the block (the fence token check relies on this; the tutorial's `IF (SELECT …)` after an `INSERT` implies it), and repeatable reads.
* The fence-row conflict argument under real independent job runs (see architecture.md).
* Managed Iceberg as a transactional write target. The docs disagree: the transactions pages say "Delta or Iceberg", while the error reference lists `WRITE_NON_DELTA_TABLE`: "Only Delta tables can be written to in a multi-statement transaction". The probes `iceberg_in_txn`, `iceberg_in_txn_rollback` and `mixed_format_txn` decide. Iceberg cells are SKIPPED unless they pass.
* Whether Iceberg tables need or accept `delta.feature.catalogManaged` (recorded as INFO by `iceberg_catalog_commits_property`).
* Exact error conditions raised for transaction conflicts on DBR 18. The classifier uses the documented 2D521/40001/40000 conditions. Anything else is UNCLASSIFIED and is not retried, which can only under-retry.
* `databricks bundle validate/deploy`: the YAML follows the documented schema but has not been validated against a workspace. The DBR runtime key default (`18.0.x-scala2.13`) and node type are placeholders to confirm.
* Compute variants: `resources/jobs.yml` (default) uses serverless jobs (`environments`, environment version 5); `resources/classic/jobs_classic.yml` is the DBR 18.0+ job-cluster variant. Neither has been deployed. On serverless, cluster ids are unavailable, so "distinct clusters" evidence of overlap falls back to distinct job runs and Spark sessions.
* Databricks Free Edition (`-t free`, `config/free_edition.yaml`): at most 5 concurrent job tasks per account, so at most 4 writers; daily fair-use quota; small serverless sizes. Results from it demonstrate correctness and ordering, not performance.

## Known design limitations

* **Clock skew.** Cross-writer orderings that use client timestamps (arrival, ready, ack) are subject to clock skew between clusters. Commit *version* order from the ledger is skew-free and is reported alongside.
* **Ledger write timestamp.** `ledger_write_ts` is `current_timestamp()` inside the transaction, not the server commit time.
* **Cold reads.** `CLEAR CACHE` does not clear OS page cache, the disk cache on other nodes, or remote storage caches. "First after clear cache" is labelled as exactly that.
* **Fixed key sets.** Full-snapshot key sets do not churn across publications, so the contiguity check requires the last version of each key to be open. Deletes are only exercised by strategy C's `NOT MATCHED BY SOURCE`.
* **Zero-row scopes.** These are not generated. A zero-row publication is valid for the engine (`expected_rows=0`); SCD2 would then close all active rows and leave the scope with no active publication, so `one_active_publication_equals_ledger_latest` would report it. That case is documented but not exercised.
* **Fence row in `off` mode.** The `off` mode still updates the fence row so that duplicate deliveries stay exactly-once. This adds a same-row conflict between same-scope writers in every mode. Unsequenced behaviour without that serialization point is therefore *not* measured. Data-table UPDATEs would conflict on the same scope anyway.
* **Fault coverage.** "Writer termination" is implemented (`writer_termination`) but only through an independent-writer launcher, and it is not in the default fault list. Killing the Python driver may not stop the JVM-side statement; the ledger decides the outcome.
* **Retention.** Results are not automatically expired. `reporting.results_retention_days` is recorded for operators to enforce.
* **Statistics.** Small samples make p99 meaningless. `n` is always reported. No universal winner is implied by any chart.
* **Strict ordering depends on atomic visibility.** The `sequenced`/`monotonic` fence is correct only if a publication's fence update and its data DML become visible *together*. Local non-atomic emulation demonstrated the failure mode when they don't:
  1. writer k commits its fence update;
  2. writer k+1 passes the check and closes the still-active rows of publication k−1;
  3. writer k's INSERT then lands and stays active forever: two active rows per key.

  The validators detected it (`one_active_row_per_key`, `contiguous_validity_intervals`). The contention cells and `tests/databricks` must confirm that `BEGIN ATOMIC` closes this window on the target runtime.
* **Local emulation.** Local runs never test atomicity: in-transaction faults leak partial state by design (non-atomic), and the harness reports `EMULATION_LEAK_DETECTED`. Local timings are not Databricks performance.

## Documentation sources (retrieved 2026-11-14)

* https://docs.databricks.com/aws/en/transactions
* https://docs.databricks.com/aws/en/transactions/transaction-modes
* https://docs.databricks.com/aws/en/transactions/tutorial
* https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-txn-begin-atomic
* https://docs.databricks.com/aws/en/tables/features/catalog-commits
* https://docs.databricks.com/aws/en/tables/managed
* https://docs.databricks.com/aws/en/iceberg/
* https://docs.databricks.com/aws/en/optimizations/isolation/row-level-concurrency
* https://docs.databricks.com/aws/en/error-messages/multi-statement-transaction-not-supported-error-class
* https://docs.databricks.com/aws/en/error-messages/error-classes and .../sqlstates
* https://docs.databricks.com/aws/en/jobs/dynamic-value-references, .../jobs/task-parameters
* https://docs.databricks.com/aws/en/dev-tools/bundles/variables
* https://docs.databricks.com/aws/en/getting-started/free-edition-limitations (retrieved 2026-11-15)
* https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/ (retrieved 2026-11-15)
