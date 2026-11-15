# PHASE 06 — Invariants and failure injection

Implement independent validators run after each publication and after contention tests:
- Exactly one active row per `(table, dataset_scope, business_key)` in SCD2 full snapshots; if zero-row scope permitted, document expected behavior.
- Exactly one active *publication* per `(table, scope)` and it matches the ledger's latest publication; all expected keys present; no duplicates, gaps, overlaps, inverted boundaries or timestampless records.
- For every historical version, effective_from < effective_to when non-null; consecutive versions connect at exact timestamps; publication_ts consistent across every table in batch.
- All tables have same successfully committed publication sequences and counts for each scope (subject to explicitly empty input tables). Ledger complete and agrees with data; no ghost ledger entries after rollback.
- Append-only latest/version queries reproduce equivalent snapshots and derived interval history; deterministic content parity across Delta/Iceberg.
- Invariant checker should not confuse concurrent in-flight transactions with corruption; use a single stable post-run inspection, and test controlled reader-visible snapshots separately.

Failure matrix: raise after UPDATE; raise after INSERT; last-table failure; writer process termination pre-commit; lost response post-commit; stale/out-of-order publication; duplicate retry; schema mismatch; unsupported table format; concurrent same scope; concurrent disjoint scope; 20-writer collision; artificial delay exceeding configurable client timeout. Differentiate simulated faults from real observed error classes.

Use pytest unit/property tests with mocked transaction executor + integration tests tagged `databricks`; any destructive tests isolated to run-owned tables. Test harness itself must report FAIL if it detects integrity loss, unexpected skips or unhandled exception.

Acceptance: correctness assertions automated and independently reproducible from persisted results, failure test expected outcome stated for each, no false PASS on skipped cases.
