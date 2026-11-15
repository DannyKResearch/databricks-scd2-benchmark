# How to run dbxbench on Databricks Free Edition

This is the sequence that was actually used to run this repository against a Databricks Free Edition workspace on 2026-11-15. It covers the commands, what each one does, and the problems hit along the way with how they were fixed.

## What Free Edition changes

Free Edition limits that matter here ([Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)):

| limit | consequence for dbxbench |
|---|---|
| Serverless compute only, no custom compute | The bundle's default jobs (`resources/jobs.yml`) run on serverless. Classic job clusters (`resources/classic/`) cannot be used. |
| At most 5 concurrent job tasks per account | The suite's coordinating task is one of the five, so `config/free_edition.yaml` uses at most 4 concurrent writers. |
| Daily fair-use quota | Compute shuts down for the day if you exceed it. Run the live tests first and the suite second, not everything at once. |
| At most 100 tables per schema (observed: `UC_RESOURCE_QUOTA_EXCEEDED … limit: 100`) | Use `schema_layout: per_cell`: one schema per experiment cell. |
| Small serverless sizes | Results show correctness and ordering. They are **not** performance numbers. |

The `free` target in `databricks.yml` sets:

- catalog `workspace`;
- results Volume `/Volumes/workspace/dbxbench/results/runs`;
- config `config/free_edition.yaml`.

## Prerequisites

* WSL/Linux or macOS with `git`.
* [uv](https://docs.astral.sh/uv/) on `PATH`. The bundle builds the wheel with `uv build`.
* A Free Edition workspace. Its URL looks like `https://dbc-xxxxxxxx-xxxx.cloud.databricks.com`.

## 1. Install the Databricks CLI

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh
databricks --version
```

## 2. Log in (OAuth, no tokens in the repo)

```bash
databricks auth login --host https://dbc-xxxxxxxx-xxxx.cloud.databricks.com
```

Accept or choose a profile name. The examples below use the workspace name as the profile: `-p dbc-xxxxxxxx-xxxx`. If the browser does not open from WSL, copy the printed URL into a browser yourself.

```bash
databricks current-user me -p dbc-xxxxxxxx-xxxx      # sanity check
export DATABRICKS_CONFIG_PROFILE=dbc-xxxxxxxx-xxxx   # optional: lets you omit -p below
```

## 3. Create the results location (once)

```bash
databricks schemas create dbxbench workspace -p dbc-xxxxxxxx-xxxx
databricks volumes create workspace dbxbench results MANAGED -p dbc-xxxxxxxx-xxxx
```

The benchmark creates its own run-owned schemas next to these, named `<prefix>_<run_id>` and `<prefix>_<run_id>__<cell>`. It never writes into schemas it did not create.

## 4. Validate and deploy the bundle

```bash
cd databricks-scd2-benchmark
databricks bundle validate -t free -p dbc-xxxxxxxx-xxxx
databricks bundle deploy   -t free -p dbc-xxxxxxxx-xxxx --var git_sha=$(git rev-parse HEAD)
```

`-t` is the **bundle target** from `databricks.yml` (`free`). `-p` is the **CLI profile**. They are different things.

Deploy creates four jobs, each prefixed `[dev <you>]`:

| job | what it does |
|---|---|
| `dbxbench_worker` | one independent writer; the suite starts N concurrent runs of it |
| `dbxbench_suite` | doctor → init → generate → performance → faults → contention → verify → analyze |
| `dbxbench_live_tests` | `pytest -m databricks`: live BEGIN ATOMIC commit and rollback, fault matrix, duplicate delivery |
| `dbxbench_cleanup` | ownership-checked drop of one run's schemas; result files are kept |

## 5. Run the live acceptance tests first

```bash
databricks bundle run dbxbench_live_tests -t free -p dbc-xxxxxxxx-xxxx
```

* This takes about 50 minutes on Free Edition. Each SQL statement is a serverless round trip, and the fixture runs doctor, init, generate, 4 performance cells and 22 fault cells.
* `--params` is **not** a `bundle deploy` flag. Job parameters go on `bundle run`.

### Watching progress while it runs

The CLI only prints RUNNING until the end. To see real progress, list the cell status files on the Volume:

```bash
databricks fs ls dbfs:/Volumes/workspace/dbxbench/results/runs -p dbc-xxxxxxxx-xxxx
mkdir -p ./results/free && databricks fs cp -r dbfs:/Volumes/workspace/dbxbench/results/runs ./results/free \
  -p dbc-xxxxxxxx-xxxx --overwrite
# each cells/<cell>/status.json moves PLANNED -> RUNNING -> COMPLETE/FAILED/SKIPPED
```

### If a test fails, get the task output

```bash
databricks jobs get-run <run_id> -p dbc-xxxxxxxx-xxxx -o json      # find tasks[].run_id
databricks jobs get-run-output <task_run_id> -p dbc-xxxxxxxx-xxxx -o json
```

To stop a run that is clearly going to fail, and so save quota:

```bash
databricks jobs cancel-run <run_id> -p dbc-xxxxxxxx-xxxx
```

A failed serverless task may be **retried automatically**; this happened once. A retry repeats the whole fixture and creates another run schema.

## 6. Run the benchmark suite

```bash
databricks bundle run dbxbench_suite -t free -p dbc-xxxxxxxx-xxxx \
  --params run_id=free_$(date -u +%Y%m%d%H%M%S)
```

Results go to `/Volumes/workspace/dbxbench/results/runs/<run_id>/`. `report.md` is the summary; `capabilities.json` and each cell's `status.json` and `validation.json` hold the evidence.

## 7. Clean up (Free Edition quotas make this routine)

```bash
# suite runs (schema prefix scd2_bench)
databricks bundle run dbxbench_cleanup -t free -p dbc-xxxxxxxx-xxxx --params run_id=<run_id>
# live-test runs (schema prefix dbxbench_live); FAILED cells keep their tables unless you discard them
databricks bundle run dbxbench_cleanup -t free -p dbc-xxxxxxxx-xxxx \
  --params run_id=<run_id>,schema_prefix=dbxbench_live,discard_failure_evidence=yes
```

Cleanup refuses unless all of these hold:

- the config allows cleanup (`free_edition.yaml` does);
- `--yes` is passed (the job passes it);
- one exact run id is given;
- every schema and table carries the `dbxbench.owner` marker and **exactly** that `dbxbench.run_id`.

It drops `<prefix>_<run_id>` and its `<prefix>_<run_id>__<cell>` schemas, and nothing else. Result files on the Volume are never deleted. To check what is left:

```bash
databricks schemas list workspace -p dbc-xxxxxxxx-xxxx
```

## What happened on the first runs, and the fixes

| symptom | cause | fix |
|---|---|---|
| `Error: unknown flag: --params` on `bundle deploy` | `--params` belongs to `bundle run`; `-t` was given the profile name | `deploy -t free -p <profile>`, then `run … --params …` |
| Live-test job failed in seconds with `SystemExit: 4` | `ImportError while loading conftest … OSError: [Errno 95] Operation not supported: '/Workspace/…/tests/__pycache__'`: workspace files reject pytest's cache and bytecode writes | `scripts/run_live_tests.py` copies `tests/`, `config/` and `pyproject.toml` to a local temp directory and disables bytecode writes |
| 3 fault cells: `QUOTA_EXCEEDED.UC_RESOURCE_QUOTA_EXCEEDED … (estimated count: 128, limit: 100)` | one schema held every table of the run | `project.schema_layout: per_cell` (Free Edition config and live tests) |
| After the `client_timeout` fault, every later cell failed with `TRANSACTION_CONTEXT_MISMATCH.ACTIVE_IN_SPARK_SESSION_BUT_NOT_IN_THREAD` | The engine stopped waiting for a transaction that was still open on a worker thread, then sent ledger reconciliation from another thread. Databricks binds an open transaction to its thread, and the session stayed broken. | `SparkSqlExecutor` runs every statement of a session on one dedicated thread, in order, so a timed-out transaction is always finished before the next statement. Client timeouts no longer interrupt the server statement. A broken or stalled session fails fast as `SESSION_UNUSABLE` instead of producing misleading per-cell errors. Unit test: `tests/unit/test_session_thread.py`. |
| Live tests passed 5/5 but the task showed `FAILED` with `SystemExit: 0`, then restarted by itself | serverless treats any `SystemExit` as failure, and auto-optimization retries failed tasks by default | the runner only exits on failure; every task sets `max_retries: 0` and `disable_auto_optimization: true` |
| Bundle could not run on serverless (job clusters, `spark_env_vars`) | Free Edition is serverless-only | serverless `resources/jobs.yml` (environment version 5); `--git-sha` flag and `KEY=VALUE` live-test arguments instead of environment variables |

## Result after the fixes

Run `r20261115t160720_7168bf` (2026-11-15): **5/5 live tests passed** in about 51 minutes.

* All 4 performance cells passed on real `BEGIN ATOMIC`.
* 21 fault cells passed across Delta and Iceberg, including `client_timeout`.
* Iceberg `constraint_violation_real` was SKIPPED (no CHECK constraints on Iceberg).

Next on Free Edition: `dbxbench_suite` (contention cells with up to 4 independent writer job runs).

## What the first (partial) live run established

From `capabilities.json` and the cells that ran before the session broke. Serverless, `workspace` catalog, 2026-11-15:

* **SUPPORTED** (functional probes):
  - `begin_atomic_multi_table`, `begin_atomic_rollback`;
  - `catalog_commits_delta`, `staging_read_in_txn`, `row_commit_version`;
  - managed Iceberg creation and `UPDATE`;
  - `iceberg_in_txn`, `iceberg_in_txn_rollback`, `mixed_format_txn` (Delta and Iceberg in one transaction);
  - `merge_in_txn_delta`, `merge_in_txn_iceberg`.
* PASS on real `BEGIN ATOMIC`:
  - performance cells for SCD2 and append-only on both Delta and Iceberg;
  - fault cells `after_first_insert` and `lost_ack` (Delta and Iceberg), `after_ledger`, `late_delivery` and `constraint_violation_real` (Delta), and `duplicate_keys_input` (Iceberg).
* SKIPPED: `constraint_violation_real` on Iceberg, because CHECK constraints are not available on managed Iceberg tables.
* Not yet established: all the remaining fault cells, the contention cells (independent concurrent writers) and the full suite. **Rerun steps 5 and 6** with the fixes above.

Free Edition timings are not performance results and must not be published as such.
