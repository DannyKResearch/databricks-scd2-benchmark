# Architecture

```
            config/*.yaml ──► BenchConfig (pydantic, extra=forbid, guardrails)
                                   │
   dbxbench doctor ──► Prober (functional probes in a throwaway probe schema) ──► capabilities.json
                                   │
   dbxbench init ────► Plan (cells) ──► gate(capabilities) ──► SKIPPED(reason) | DDL (outside txn)
                                   │
   dbxbench generate ► datagen (hash-deterministic PySpark) ──► immutable staging tables
                                   │
   dbxbench run ─────► performance cells: inline engine, sequential publications
                       fault cells:       inline engine + deterministic fault plan
                       contention cells:  coordinator ─► N independent writers
                                            (Databricks: N job runs of dbxbench_worker;
                                             local: N OS processes, harness self-test only)
                                            ─► file barrier (ready / go / abort) on a UC Volume
                                            ─► each writer: PublicationEngine ─► ONE spark.sql(BEGIN ATOMIC…END)
                                            ─► per-writer JSONL telemetry (outside the write set)
   dbxbench verify ──► Validator (SQL invariants) + cross-cell parity  ──► verification.json
   dbxbench analyze ─► analysis (percentiles, Kendall τ, overlap, matched pairs) ─► summary/report/charts
```

## Modules (`src/dbxbench/`)

| module | responsibility |
|---|---|
| `config.py` | typed config, enums, cross-field rules (tables-per-transaction cap, local vs Databricks modes), CLI overrides, secret rejection, config hash |
| `sqlutil.py` | identifier allowlist + backtick quoting, typed literal rendering (escaped strings, explicit-UTC microsecond timestamps) |
| `naming.py` | run ids, run-owned namespace (per-run schema or prefixed tables), ownership properties |
| `model.py` | exact table schemas, `Publication`, `DataProfile`, attempt records |
| `sqlgen.py` | DDL (never `IF NOT EXISTS`/`OR REPLACE`) and the publication block for all strategies, ordering modes and in-transaction fault points |
| `engine.py` | prevalidation, single-call submission, per-attempt timeout, classification, ledger reconciliation, whole-block retry with backoff/deadline |
| `errors.py` | documented conflict classes → retryable; marker codes raised from SQL; everything unknown → terminal |
| `datagen.py` | deterministic snapshot generation, change epochs, skew, nulls, payload width, negative cases |
| `validation.py` | invariant SQL checks (atomicity / idempotency / history / ordering) and parity fingerprints |
| `capabilities.py` | doctor probes, environment capture, cell gating |
| `barrier.py`, `launcher.py`, `worker.py` | independent-writer harness |
| `metrics.py`, `queries.py` | DESCRIBE DETAIL/HISTORY metrics, OPTIMIZE, commit order, read-query benchmark |
| `runner.py` | orchestration of doctor/init/generate/run/verify, fault matrix, manifest |
| `analysis.py`, `report.py` | offline analysis, CSV/JSONL export, Markdown report + matplotlib charts |
| `cleanup.py` | explicit ownership-checked cleanup |

## The transaction block

`sqlgen.build_publication_sql` produces the block below. `engine.PublicationEngine` submits it with **one** `SqlExecutor.execute` call, which is one `spark.sql` call. The unit tests assert this through `RecordingExecutor`, and the live tests assert it from the persisted per-attempt SQL files.

1. `DECLARE` local variables.
2. **Idempotency:** count ledger rows for `(scope, publication_id)`; if there are any, `SIGNAL … DBXBENCH_DUPLICATE_PUBLICATION`. Nothing is written.
3. **Fence read:** `SET v_last_seq = (SELECT max(last_sequence) FROM fence WHERE scope = …)`.
4. **Ordering:**
   - `monotonic`: `seq <= last` ⇒ `STALE_SEQUENCE`.
   - `sequenced`: also `last < seq - 1` ⇒ `SEQUENCE_NOT_READY`, which is retryable (the writer waits).
   - `off`: no check.
5. **Fence write:** `UPDATE fence … WHERE scope = …`, in **every** mode, then verify that our own token is visible.
6. For each table in the publication:
   - optional in-transaction staging count check;
   - `scd2_full`: `UPDATE … SET effective_to_date = ts WHERE scope AND effective_to_date IS NULL`, then `INSERT … SELECT` of the full staged snapshot with `effective_from_date = ts`;
   - `append_only_full`: `INSERT` only;
   - `scd2_changed_only` (optional C): `MERGE` closes changed and vanished keys, then `INSERT` of new and changed keys.
7. `INSERT INTO ledger …`.

### Why this is concurrency-safe, and what still has to be proven live

We do **not** rely on UC primary-key constraints; they are informational and not enforced. Every same-scope publication modifies the same fence row. With optimistic concurrency and row-level conflict detection, two transactions that update the same row conflict ([row-level concurrency docs](https://docs.databricks.com/aws/en/optimizations/isolation/row-level-concurrency)), so at most one commits. The loser gets a classified concurrency error and re-runs the whole block with fresh reads. It then sees the winner's ledger row (duplicate) or its fence sequence (stale or not ready).

This argument rests on documented behaviour, but the suite treats it as a hypothesis:

* the contention cells (independent job runs) check ledger exactly-once, fence/ledger agreement and history invariants after every run;
* `tests/databricks` checks rollback, duplicate and lost-ack behaviour live.

If either fails on a given runtime, strict ordering is not supported there, and the report shows FAIL rather than a claim.

### Unknown outcomes

A lost reply or client timeout is classified `UNKNOWN_OUTCOME`. The engine requests cancellation and reads the ledger:

| ledger shows | outcome |
|---|---|
| our attempt token | `COMMITTED_RECONCILED` |
| another token | `ALREADY_COMMITTED` |
| nothing | safe to retry: the in-transaction idempotency check plus the fence conflict mean the abandoned attempt and the retry cannot both commit |

Three further rules (each found during review and covered by a regression test):

* `STALE_SEQUENCE` is also reconciled against the ledger. An abandoned attempt of ours may have committed and advanced the fence.
* Before reporting any terminal failure (`FAILED_RETRIES_EXHAUSTED`, `DEADLINE_EXCEEDED`, `FAILED_NONRETRYABLE`) after an unknown outcome, the engine **waits (bounded) for its abandoned in-flight attempts** and then checks the ledger again. If an attempt is still running, the status is `OUTCOME_UNKNOWN`, never a definitive failure.
* A late finish of an abandoned attempt is recorded as a `late_attempt_finished` telemetry event.

### Files on UC Volumes

The docs state that `/Volumes` paths do not support appends or random writes. All results-root writes go through `storage.py`:

* JSONL logs are appended to a local spool, which is then copied sequentially over the destination, so the destination is always a complete prefix of the log;
* immutable files use an existence check plus a sequential write;
* there are no renames.

Unit tests force this mode (`DBXBENCH_NO_APPEND=1`) and assert that the destination is never opened for append.

## Independent writers

* **Databricks (primary evidence).** `DatabricksJobsLauncher` calls Jobs API `run-now` N times concurrently from a thread pool, with an idempotency token per writer plan. `dbxbench_worker` has `max_concurrent_runs: 100` and its own job cluster per run. Correlation runs through `{{job.run_id}}` and `{{task.run_id}}` passed to the worker, plus the `run_id` returned by `run-now`.
* **Barrier.** Plain files on a UC Volume: `ready/<writer>.json`, `go.json` (start instant a few seconds ahead) and `abort.json` (fail closed on timeout). No Spark executor blocks and no driver memory is shared. Stragglers that arrive after an abort exit immediately.
* **Telemetry.** One JSONL file per writer, fsync'ed per event, outside the transactional write set. It therefore adds no conflicts to the measured transactions and survives rollback.
* **Local (`local_process`).** N OS processes, each with its own JVM and SparkSession over shared Delta paths. This only validates harness mechanics.
