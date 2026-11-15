# databricks-scd2-benchmark (`dbxbench`)

A rerunnable, configuration-driven PySpark + Databricks SQL research suite that measures:

1. **Full-snapshot SCD2 vs strict append-only publication.** It covers write latency, read-query latency (including derived end dates for append-only), storage, maintenance and write amplification.
2. **Atomic multi-table publication under N independent concurrent writers.** Each publication is one `BEGIN ATOMIC … END;` block, submitted with **one** `spark.sql()` call, that updates every data table **and** a durable ledger. The suite records commit ordering against arrival order (FIFO is never assumed), conflict classes, retries, starvation, tail latency and throughput.
3. **Managed Delta vs managed Iceberg** on the same workload. Each variant is gated by functional capability probes. An unsupported variant is **SKIPPED with a reason**, never replaced by Delta.

> **Status of evidence in this repository.** Everything below the line "verified locally" was run on OSS Spark. Nothing here claims a Databricks result: no Databricks workspace was available while this suite was written. Every Databricks-only test is labelled `NOT_RUN` until you run it. See [Verification status](#verification-status).

## How it works (one paragraph)

`init` builds a deterministic plan of *cells*. A cell is a strategy × format × scenario combination, and each cell gets its own run-owned tables (targets, ledger, fence). `generate` materializes immutable staging snapshots with PySpark, using hash-based generation so contents are identical regardless of partitioning. `run` publishes through the engine. The engine prevalidates staging (untimed) and then submits **one** SQL block per attempt. That block does five things:

- checks the ledger for idempotency;
- reads the per-scope fence row and enforces the ordering mode;
- updates the fence row, so same-scope writers always write-write conflict;
- applies the strategy DML to every table;
- inserts the ledger row.

Retries re-run the whole block, and only for classified conflict errors. After any ambiguous outcome (lost reply, timeout) the engine reads the ledger before deciding anything. `verify` re-checks every invariant from the persisted tables. `analyze` builds summaries, CSV/JSONL exports, `report.md` and charts from measured data only.

Details: [docs/architecture.md](docs/architecture.md) · [docs/methodology.md](docs/methodology.md) · [docs/limitations.md](docs/limitations.md) · [docs/emitted_sql.md](docs/emitted_sql.md)

## Requirements

* **Databricks (primary).** Unity Catalog, plus compute that supports non-interactive transactions: DBR **18.0+** classic, serverless, or a SQL warehouse ([docs](https://docs.databricks.com/aws/en/transactions)). You need privileges to create schemas and tables in one catalog, and a UC Volume for results. Concurrency runs need the Jobs API.
* **Local.** Python 3.11+ (3.12 matches DBR 18), Java 17/21, and `pip install -e '.[local,dev,report]'` (PySpark 4.2.0 + delta-spark 4.4.1).

Authentication uses the standard Databricks SDK chain: `DATABRICKS_HOST` plus a token or OAuth from the environment, a `~/.databrickscfg` profile, or job identity. **No credential is ever read from YAML**; the config loader rejects secret-looking keys and values.

## Quick start: local tiny mode (harness + semantics only, NOT atomicity evidence)

```bash
uv venv --python 3.12 .venv && . .venv/bin/activate      # or python -m venv
pip install -e '.[local,dev,report,jobs]'
pytest tests/unit                                         # SQL is mocked; ~3 s
pytest -m spark tests/integration                         # local Spark + Delta
dbxbench init  --config config/local_tiny.yaml --run-id demo_dry --dry-run   # prints DDL + transaction SQL
dbxbench smoke --config config/local_tiny.yaml            # doctor+init+generate+run+verify+analyze
```

Local mode uses `transaction_mode: local_nonatomic_emulation`. OSS Spark has no `BEGIN ATOMIC`, so the same script runs as a plain SQL compound statement. That is useful for testing SQL shape, SCD2 semantics, validators and the multi-process harness. Every result is stamped `atomicity_evidence=false`. The in-transaction fault cells are *expected* to leak partial state there, and the harness must detect the leak (`EMULATION_LEAK_DETECTED`), which demonstrates validator sensitivity.

## Databricks commands

```bash
# 0) configure
cp config/example.yaml config/mine.yaml   # set project.catalog, project.results_root (UC Volume)
# 1) deploy the bundle (wheel + worker/suite/live-test jobs)
databricks bundle validate -t dev --var catalog=<cat> --var results_root=/Volumes/<cat>/<sch>/<vol>/runs
databricks bundle deploy   -t dev --var catalog=<cat> --var results_root=... --var git_sha=$(git rev-parse HEAD)
# 2) capability check incl. managed Iceberg (functional probes in a throwaway probe schema)
dbxbench doctor --config config/mine.yaml          # or the suite job's `doctor` task
# 3) live acceptance tests (BEGIN ATOMIC commit, rollback matrix, lost-ack, duplicates)
databricks bundle run dbxbench_live_tests -t dev
# 4) tiny smoke on Databricks
databricks bundle run dbxbench_suite -t dev --params run_id=smoke_$(date +%s)
```

### Databricks Free Edition

Full walkthrough, troubleshooting and what the first live run established: [HOW_TO_RUN_ON_DATABRICKS_FREE.md](HOW_TO_RUN_ON_DATABRICKS_FREE.md).

Free Edition is serverless-only, allows 5 concurrent job tasks per account, and has a daily fair-use quota. Use the `free` target: it selects `config/free_edition.yaml` (tiny workload, ≤ 4 writers) and the default `workspace` catalog. It measures correctness and ordering, not performance.

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com   # profile e.g. "free"
databricks schemas create dbxbench workspace -p free
databricks volumes create workspace dbxbench results MANAGED -p free
databricks bundle validate -t free -p free
databricks bundle deploy   -t free -p free --var git_sha=$(git rev-parse HEAD)
databricks bundle run dbxbench_live_tests -t free -p free
databricks bundle run dbxbench_suite      -t free -p free --params run_id=free_$(date -u +%Y%m%d%H%M%S)
databricks bundle run dbxbench_cleanup    -t free -p free --params run_id=<run_id>
```

Scenario recipes (CLI overrides apply to every command; run the suite job with a matching config):

| Goal | Overrides / config |
|---|---|
| Tiny smoke | `--tables 2 --rows 1000 --publications 3 --writers 1,2` |
| 1M rows × 10 tables | `--tables 10 --rows 1000000 --publications 5` (check the `max_estimated_*` guardrails) |
| 10-writer collision | `--writers 10 --contention same_scope --ordering off,sequenced` (needs `max_concurrent_runs` ≥ 10 on the worker job) |
| Iceberg capability only | `dbxbench doctor --config ...` → `capabilities.json` (`managed_iceberg_create`, `iceberg_in_txn`, `iceberg_in_txn_rollback`, `mixed_format_txn`) |
| Failure injection | `dbxbench run --kind fault ...` (expected outcome per fault: `runner.FAULT_EXPECTATIONS`) |
| Export charts/report | `dbxbench analyze --config ... --run-id <id>` → `report.md`, `charts/*.png`, `export/*` |
| Safe cleanup | set `project.allow_cleanup: true`, then `dbxbench cleanup --config ... --run-id <id> --yes` |

**Cost and deletion warnings.** The contention cells start one job cluster *per writer* (N clusters at once). Check `compute.max_parallel_job_runs` and your cluster policy before running 10 or more writers. Cleanup permanently drops the run-owned schema. It refuses to run without `allow_cleanup`, `--yes`, an exact `--run-id`, and ownership markers on the schema and on every table. It also refuses while FAILED evidence exists, unless you pass `--discard-failure-evidence`. Results files are never deleted.

## Results layout

`<results_root>/<run_id>/` contains:

- `run_manifest.json` (immutable: git SHA, config hash, runtime, Spark conf, catalog/schema, seed, staging rows/bytes, concurrency and exact retry settings, per-cell `truly_ran`);
- `capabilities.json`, `environment.json`, `plan.json`;
- `cells/<cell>/` with `{publications,attempts,table_metrics,queries}.jsonl`, `validation.json`, `telemetry/*.jsonl`, `launch.json` and `commit_order.json`;
- `verification.json`, `summary.json`, `export/`, `report.md` and `charts/`.

JSON schemas for the main records are in [results/schemas](results/schemas).

## Verification status

See [docs/VERIFICATION.md](docs/VERIFICATION.md) for the verified run log. In short: unit tests, lint and type checks, and local Spark integration (semantics, validators, multi-process harness) were run here. **Databricks transactional behaviour, Iceberg support and independent job-run concurrency are `NOT_RUN`** until executed in a workspace.
