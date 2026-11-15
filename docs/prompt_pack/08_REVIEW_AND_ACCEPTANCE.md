# FINAL CLAUDE REVIEW PROMPT — paste after implementation

Independently review the implemented suite as if it will determine a firm-wide architecture decision. Verify against current official Databricks docs and actual run output. Inspect code, not README promises.

Critical findings to check:
1. Multi-table UPDATE + INSERT and ledger really occur in ONE `spark.sql("BEGIN ATOMIC ... END;")` call, not individual DML commits; supported compute and managed/Catalog Commit gates enforced.
2. Correct full-snapshot SCD2 semantics, timestamp precision, same timestamp across tables, no UPDATE for append-only baseline.
3. Ordering: earlier job arrival is NOT automatically FIFO; strict-order test actually prevents stale and reversed snapshots; independently verify idempotency with duplicate and lost-ack cases. No reliance on non-enforced primary keys.
4. Concurrency: true independent Spark Jobs runs, overlapping time windows, finite barrier and reliable event timestamps.
5. Unsupported Iceberg capability is SKIPPED, never rebranded Delta or shown as a success; settings and optimizations are stated.
6. Rollback injection occurs inside one transaction, correctly evaluated from persistent state; partial commits cannot pass.
7. Tests enforce contiguous validity intervals, one active row per key, whole-batch table membership, ledger consistency and reader snapshot guarantees at the advertised scope.
8. Benchmark fair: same data/compute, staging excluded or reported separately, maintenance included, no invention of byte counts, records confidence and limitations.
9. Safe cleanup only within owned benchmark namespace, no secrets printed, budgets/guards functional.
10. Produce evidence: runnable commands, test summary, actual metric output, known gaps, precise docs/source URLs.

Return severity-ranked review with file:line references. Fix P0/P1 defects; rerun tests. Do not mark DONE if primary correctness or concurrency test has not been exercised against a supported Databricks environment. Clearly distinguish unit-tested, integration-tested, and unexecuted sections.
