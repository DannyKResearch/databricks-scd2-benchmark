# PHASE 03 — Atomic SQL publication engine

Implement an engine that generates and submits the entire `BEGIN ATOMIC ... END;` block via ONE `spark.sql(sql)` call. No Python-side multi-call pretend transaction. Ensure ledger and every target table are managed and Catalog Commits enabled. Input staging tables must be immutable, fully materialized before txn, and accessed by stable table identifiers rather than path access inside the transaction.

Before submitting: validate manifest, publication sequence, timestamp, dataset scope, schema, completeness, and staging row counts. Enforce schema/identifier allowlists; quote UC identifiers securely and escape SQL literals or use supported parameterization without relying on nonexistent binding APIs.

Inside transaction: validate or enforce atomic idempotency/order fencing via documented SQL features. If a guaranteed unique constraint or locking primitive isn't supported, explicitly explain and implement conflict-safe fencing that is **proven** by competing writers and post-commit checks; do not assume UC primary key constraints are enforced. Consider a managed per-scope state row read-and-modified inside the same transaction, detect stale sequence, and test genuine conflicting commits. The ledger stores `(run_id, dataset_scope, publication_id, sequence, logical timestamp, attempt metadata as appropriate)`. If no correct concurrency-safe design is available, mark strict ordering mode unsupported rather than claiming correctness.

Test rollback when failure is injected after earlier table UPDATE or INSERT; use supported `SIGNAL SQLSTATE` or deterministic SQL failure **inside** atomic block after verifying syntax. Test unknown outcome: simulate lost reply after commit and reconcile against ledger before retry. Test table subset modifications, entire batch retries, stale submissions and rejected older timestamps.

Error classification with retryable known conflict categories, nonretryable schema/permission/data errors, bounded exponential backoff with jitter, attempt cap and global deadline. Do not catch every exception and retry blindly. Persist attempt telemetry outside the transaction with an independent per-run sink; successful ledger updates must be inside.

Acceptance: in every injected failure zero partial rows/ledger changes; success updates all targets; exactly-once logical publication under duplicate invocations; older batch can't close newer state; SQL submitted as one block.
