# PHASE 07 — Databricks deployment, operations and reports

Provide Databricks Asset Bundle/workflow job definitions where supported: separate init/generate/worker/verify/analyze tasks, plus a fan-out mechanism allowing N **separate job runs** for contention testing. Explain permissions and runtime settings. Optional notebook launcher for simple demonstrations; full suite remains importable PySpark package.

Produce clear commands for tiny smoke, 1M x 10 tables, 10-writer collision, managed Iceberg capability check, failure injection, export charts, and safe cleanup. Include SQL emitted by benchmark to facilitate review.

Results must include immutable `run_manifest.json`: git SHA, config hash, runtime version, spark conf, catalog/schema, table features, table format, compute, timestamps UTC, data seed, staged bytes/row counts, concurrency config, exact retry settings and whether scenario truly ran.

Generate machine-readable `attempts`, `publications`, `table_metrics`, `validation`, `capabilities`, `environment`, `summary` data and a Markdown report with tables and charts covering:
- latency p50/p95/p99; throughput; cost proxy;
- write amplification/storage/maintenance;
- failure and retry rates; arrival vs commit inversion;
- snapshot/history query results;
- Delta vs Iceberg matched pairs;
- correctness verdict and limitations.

Never automatically clean resources on test failure (retain evidence with TTL/explicit cleanup). Show cost and deletion warnings. Validate `--cleanup --run-id` refuses ambiguous scope and production resources. Add no fabricated benchmark charts. Finish with a final completion report listing verified tests, SKIPPED capabilities and next experiments.
