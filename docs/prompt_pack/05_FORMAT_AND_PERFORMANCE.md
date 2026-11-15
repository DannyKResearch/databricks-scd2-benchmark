# PHASE 05 — Performance and Delta/Iceberg parity

Run strategy-by-format matrix: managed Delta SCD2 full, managed Delta append-only full, managed Iceberg SCD2 full, managed Iceberg append-only full; optional changed-only SCD2. Verify Iceberg DML/transaction feature support for workspace/runtime via live docs + actual capability probes. Unsupported comparisons = SKIPPED with reason. Require managed Catalog Commits-enabled write tables for multi-table transaction runs.

Parameterized sweeps: per-table rows [100K,1M,10M], tables [2,5,10,20], publications [5,20,50], writers [1,2,5,10,20], changed payload fraction [0%,1%,10%,100%], scopes, key skew, payload widths, full snapshot sizes. Start tiny; enforce budget/row guardrails. Controlled repetitions, randomized scenario order, warmups, same compute and data layouts; record confounders such as DV/partitioning/clustering/compaction and defaults. Compare row-level concurrency eligible unpartitioned variants vs partitioned variants separately.

Time: data preparation, staging, transaction execution, conflict backoff, end-to-end commit, read query latencies, maintenance; record seconds and percentiles. Estimate/best-effort actual I/O: Parquet/metadata files added/removed, deletion-vector counts/size, table history operationMetrics, table size via DESCRIBE DETAIL or equivalent, S3 object stats if authorized. Do not assert bytes rewritten from `DESCRIBE HISTORY` alone; explain observability gaps.

Read queries: latest current snapshot, point-in-time snapshot, all history for one key, aggregation, interval validity; include append-only derived end timestamps and filters. Cold/cache-cleared runs when feasible; document limitations.

Deliver CSV/JSON/Delta aggregated metrics, raw attempt logs, plots generated with matplotlib, README interpretation. Never fabricate numeric results or imply a universal winner.
