# Verification log and review (phase 08)

Environment: WSL2 Ubuntu 20.04, Python 3.12.16 (uv), OpenJDK 21, PySpark 4.2.0, delta-spark 4.4.1, pydantic 2.14.1, pytest 9.1.1. Everything not executed is listed as `NOT_RUN` below; none of it is claimed to work.

## Databricks Free Edition: first live run (2026-11-15, serverless, `workspace` catalog; PARTIAL)

`dbxbench_live_tests`, live-test run `r20261115t144616_38d7e6`. The run was cancelled after the session broke (see the P0 below). Details are in [HOW_TO_RUN_ON_DATABRICKS_FREE.md](../HOW_TO_RUN_ON_DATABRICKS_FREE.md).

* Capability probes, all **SUPPORTED**:
  - BEGIN ATOMIC multi-table commit and rollback;
  - `catalogManaged` Delta;
  - managed Iceberg create and UPDATE;
  - **Iceberg inside transactions, including rollback**;
  - **mixed Delta + Iceberg transactions**;
  - MERGE in a transaction (both formats);
  - `row_commit_version`.
* Cells with **PASS** on real atomic transactions:
  - all 4 performance cells (SCD2 and append-only × Delta and Iceberg);
  - 8 fault cells.
* Not established yet:
  - the remaining fault cells (broken by the P0 or the quota);
  - contention with independent job runs;
  - the full suite.
* `bundle validate` and `bundle deploy` succeeded. `dbxbench_cleanup` dropped both run-owned schemas on the platform, with the ownership checks passing.

## What was executed, and the result

| layer | command | result |
|---|---|---|
| lint / format | `ruff check src tests`, `ruff format --check` | pass |
| types | `mypy` (strict, pydantic plugin, 30 modules) | pass |
| unit (SQL mocked) | `pytest tests/unit` | **104 passed** |
| Databricks live tests | `pytest tests/databricks` (off-platform) | **5 SKIPPED (NOT_RUN)**: never reported as pass |
| Spark integration | `pytest -m spark tests/integration` (local OSS Spark + Delta) | **10 passed** in 17 min 19 s (2026-11-15 05:26–05:43 UTC) |
| doctor (local) | `dbxbench doctor --config config/local_tiny.yaml` | BEGIN ATOMIC → UNSUPPORTED (parse error); managed Iceberg → UNSUPPORTED; local scripting → SUPPORTED |
| dry-run | `dbxbench init --dry-run` | prints DDL + one `BEGIN ATOMIC` block per (kind, strategy, format); writes nothing (see `docs/emitted_sql.md`) |
| wheel | `uv build --wheel` | builds; entry points `dbxbench`, `dbxbench-worker`, `dbxbench-task`; pyspark not a core dependency |
| bundle YAML | parsed with PyYAML | parses; **`databricks bundle validate` NOT_RUN** |

### Final local end-to-end run (local_nonatomic_emulation; NOT atomicity evidence)

The final run used config `local_tiny` with 2 tables × 50 rows × 2 publications × 1 scope and 3 independent writer processes. Every cell met its stated expectation:

| cells | outcome |
|---|---|
| performance scd2_full/delta, append_only_full/delta | **PASS** (all invariants); content parity SCD2 ↔ append-only **PASS**; all 5 read queries return **identical answers** across strategies |
| performance */iceberg | **SKIPPED**: `managed_iceberg_create=UNSUPPORTED` (probe-gated, never substituted by Delta) |
| fault duplicate_delivery, late_delivery, lost_ack, client_timeout | **PASS_EMULATION**: exactly-once ledger, stale rejected without touching newer rows, lost reply reconciled from ledger without re-applying |
| fault after_first_update, after_first_insert, last_table, after_ledger, schema_mismatch, duplicate_keys_input, constraint_violation_real (a *real* engine error mid-block) | **EMULATION_LEAK_DETECTED**: without atomicity these leave partial state (or an unauthorised commit), and the validators or ledger reconciliation detected every one. This proves the validators are sensitive; it says nothing about atomicity |
| contention same_scope / `off` | 3 processes, 3 attempts in flight at once; commit order ≠ sequence order (τ = −0.33); history corruption **FLAGGED**, not repaired; reader probe saw cross-table skew in 4/25 samples |
| contention same_scope / `sequenced` | 3 processes; commit order == sequence order (τ = +1.0) despite 2 arrival inversions; 6 `ORDERING_NOT_READY` waits; **EMULATION_LEAK_DETECTED**: non-atomic interleaving corrupted history (see limitations: ordering depends on atomic visibility) |

All timings from this run are from a laptop over local Delta and must not be read as performance results.

## Severity-ranked review findings (all fixed unless marked open)

| sev | finding | fix | how it was found |
|---|---|---|---|
| P0 | `/Volumes` does not support appends (docs: volumes/volume-files); telemetry, attempts, publications and the SQL log all appended, so **every Databricks writer would fail** | `storage.py`: local spool + sequential full copy; no rename/O_EXCL on volumes; unit test asserts the destination is never opened for append | doc review |
| P0 | `sum(xxhash64(..))` overflows BIGINT under ANSI mode (default in Spark 4 / recent DBR), so **parity and fingerprint checks would error** | sum as `DECIMAL(38,0)` | local integration failure |
| P1 | `REJECTED_STALE` trusted without a ledger check; a publication committed by our own abandoned attempt was reported stale | STALE is reconciled against the ledger | local client_timeout fault cell |
| P1 | terminal failure reported while our abandoned attempt could still commit | bounded wait for abandoned attempts, re-check ledger, new `OUTCOME_UNKNOWN` status | review + regression test |
| P1 | no per-table verification that write targets are MANAGED with `catalogManaged` | post-DDL `DESCRIBE DETAIL` / `DESCRIBE TABLE EXTENDED` gate per table | review |
| P1 | one cell's DDL failure aborted the whole `init` | per-cell FAILED setup status | review |
| P1 | `run`/`verify` exited 0 on FAIL verdicts | non-zero exit on any FAIL | review |
| P1 | raw `Py4JJavaError` (inside SQL scripting) classified UNCLASSIFIED with message "An error occurred while calling o58.sql." | condition extracted from the message body; informative line used | real constraint-violation fault |
| P1 | jitter seeded with Python `hash()` (randomised per process): not reproducible | `crc32`-based `stable_seed` | review |
| P2 | spec gap: no reader-visible snapshot test | `ReaderProbe` measurement during contention cells | review |
| P2 | contention chart drew a one-point line on a fractional axis | bars when there is a single writer count; integer ticks | rendered output inspected |
| P2 | no job-run cost warning | `init` prints the independent job runs / peak clusters | review |
| P2 | wrong fault test setup (CHECK constraint rejected existing rows) | constraint admits existing rows, rejects the new publication | local run |
| P1 | bundle used classic job clusters only, so it could not run on serverless-only workspaces (e.g. Free Edition); `spark_env_vars` do not exist on serverless | serverless `resources/jobs.yml` (environments, env version 5) as default; `--git-sha` flag and KEY=VALUE live-test args instead of env vars; classic variant kept in `resources/classic/`; `free` target + `config/free_edition.yaml` (≤ 4 writers for the 5-concurrent-task limit) | review for Free Edition |
| P2 | wheel library path `./dist/*.whl` in `resources/jobs.yml` (bundle paths resolve relative to the defining file) | `../dist/*.whl` (`../../dist` in the classic file) | review |
| P0 | after a client timeout the engine reconciled from another thread while the abandoned BEGIN ATOMIC was still open; Databricks raised `TRANSACTION_CONTEXT_MISMATCH.ACTIVE_IN_SPARK_SESSION_BUT_NOT_IN_THREAD` and the session stayed broken, failing every later cell | `SparkSqlExecutor` runs all statements of a session on one dedicated thread, in order; client timeouts no longer interrupt; broken or stalled sessions fail fast as `SESSION_UNUSABLE`; `test_session_thread.py` reproduces the rule (and fails against the old executor) | live Free Edition run |
| P1 | `UC_RESOURCE_QUOTA_EXCEEDED` (100 tables per schema on Free Edition) | `project.schema_layout: per_cell`; cleanup covers `<run schema>__<cell>` schemas and requires an exact run-id match | live Free Edition run |
| P1 | live-test job crashed loading conftest: `/Workspace` files reject `__pycache__` (Errno 95) | run pytest from a local temp copy, with bytecode writes off | live Free Edition run |
| P2 | no way to clean up from a serverless-only workspace | `dbxbench_cleanup` bundle job | live Free Edition run |
| open | everything in "NOT_RUN" below | needs a workspace | |

## NOT_RUN (requires a supported Databricks environment)

1. ~~`BEGIN ATOMIC` single-call commit and rollback~~: **partially RUN** on Free Edition serverless (probes and 12 cells PASS; see above). Still to do: the full fault matrix after the P0 fix.
2. Fence-row conflict safety and strict ordering under **independent job runs** (`dbxbench_suite` contention task + `dbxbench_worker`), including commit-version ordering and the reader-skew measurement.
3. Managed Iceberg: the probes ran on Free Edition serverless and **all passed** (creation, UPDATE, in-transaction writes and rollback, mixed-format transactions). Fault cells on Iceberg are only partially run.
4. Conflict error conditions actually raised on DBR 18 (classifier coverage).
5. ~~`databricks bundle validate/deploy`~~: serverless variant deployed on Free Edition. The classic variant (`resources/classic/`) and its runtime key / node type defaults are still NOT_RUN.
6. Volume write semantics in practice (designed to need only sequential whole-file writes).
7. All performance, storage, maintenance and query measurements on Databricks. **No Databricks numbers exist in this repository.**

## Acceptance status

Per `08_REVIEW_AND_ACCEPTANCE.md`: **NOT DONE**.

* A partial live run on Free Edition serverless passed the capability probes and 12 cells.
* The full fault matrix, the contention cells (independent concurrent job runs) and the suite have not completed on Databricks.

Next step: redeploy, then `bundle run dbxbench_live_tests`, then `bundle run dbxbench_suite` (see HOW_TO_RUN_ON_DATABRICKS_FREE.md), then review `report.md`.
