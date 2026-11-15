# MASTER IMPLEMENTATION PROMPT — paste into Claude

Act as a senior Databricks/PySpark engineer, distributed-systems test engineer and benchmark author. Implement a production-quality, **rerunnable research suite**, not a mock demo, in a Git-ready Python project. Follow all other numbered prompt documents and `config/example.yaml`.

## Research questions
1. For a complete dataset publication, how do in-place SCD2 validity end-date UPDATE plus full-snapshot INSERT compare with strict append-only full-snapshot publication, including query latency, storage overhead, maintenance and write amplification?
2. With N **independent concurrent Spark job runs/sessions** contending on multiple overlapping tables, does the transactional publication commit atomically? What are arrival-versus-commit ordering, contention, conflict classes, retries, starvation, tail latencies and throughput? No assumption of FIFO.
3. With an otherwise equivalent SCD2 workload, how do managed Delta and managed Iceberg tables compare? What is supported in the target workspace?

## Mandatory semantics
- Full snapshot per `(dataset_scope, publication_id)`; close **all** currently active rows for that scope with `effective_to_date = publication_ts`; insert **all** rows from the immutable new snapshot with `effective_from_date = publication_ts`, `effective_to_date = NULL`.
- Use TIMESTAMP types despite `_date` suffix because multiple loads per day are expected; preserve UTC precision and record time-zone settings.
- All data tables and a durable publication ledger must be atomically updated or all rolled back. Retry the **entire transaction** on safe, classified conflicts, with fresh table reads; never retry only the failed statement.
- One publication timestamp shared across all tables. Logical publication sequencing must prevent out-of-order commits from corrupting SCD2 history. A transaction being ACID does NOT prove FIFO; implement ordering enforcement as an explicit optional mode and measure both natural/unsequenced and ordered modes. In unsequenced mode, flag invalid/out-of-order histories instead of silently 'fixing' them.
- No duplicate publication on unknown success/connection loss; idempotency and a durable ledger are required. Never infer transaction status merely from an exception.
- Compare append-only semantics equivalently: complete snapshots per publication, no updates/deletes, with latest/history projected using publication order. Include end-date derivation cost in query benchmarks.
- Do NOT fake cross-table atomicity by issuing separate `spark.sql()` statements. `BEGIN ATOMIC ... END;` must be a SINGLE `spark.sql()` call on supported compute. PySpark orchestrates; SQL performs the transactional DML. Explicitly verify supported statements/SQL scripting syntax against official Databricks docs.
- Do not claim two different Spark threads within one session are independent jobs. Concurrency testing must launch separate Databricks job runs/clusters or verified separate isolated Spark sessions in independently executing tasks, with barriers implemented outside the target transaction. Primary benchmark = independent Jobs runs.
- Delta and Iceberg comparison must not assume feature parity. Each variant performs an environment capability probe; unsupported variants are SKIPPED with reason, never silently use Delta as a substitute.

## Deliverables
Git-ready tree `src/dbxbench/`, `tests/unit/`, `tests/integration/`, `tests/databricks/`, `config/`, `databricks.yml` (if Bundles supported), `pyproject.toml`, CLI, notebooks/ or entrypoint scripts, README, docs/architecture.md, docs/methodology.md, docs/limitations.md, results/ schemas and analysis scripts. No unmanaged production resources.

Use Python 3.11+ where supported, type hints, structured logging, pytest, YAML config with pydantic validation or dataclasses, deterministic seeds, dependency pinning, fail-fast capabilities and explicit cleanup. Prefer PySpark DataFrames for generation/staging and SQL text for atomic transactions. Avoid Pandas for large data.

## Implementation contract
- Implement tiny mode, then scale-up. Tests and examples must actually run or clearly be labelled `NOT_RUN` with reason.
- Produce real files and commands, not pseudocode. Work in phases 01–07, show what changed and verification after each.
- Treat Databricks documentation as authoritative; if unavailable, make the uncertain feature a capability-gated test and document the exact assumption.
- Never automatically drop data outside the uniquely configured dedicated benchmark namespace. Refuse destructive run without ownership marker and explicit opt-in.
- Report empirical data only; never invent measurements.

First: summarize the intended architecture in <=20 lines, list required workspace details and risks, scaffold repo, implement Phase 01. Then continue sequentially if possible.
