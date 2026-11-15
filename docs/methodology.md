# Methodology

## Data

Schema. `dataset_scope` and `business_key` are the keys. Payload columns:

- `attr_int BIGINT`
- `attr_decimal DECIMAL(18,4)`
- `attr_category STRING`
- `attr_flag BOOLEAN`
- `attr_text STRING` (exactly `payload_width_bytes` characters)
- `payload_hash STRING` (sha256 of the payload, with a null sentinel)

Target tables add:

- SCD2: `effective_from_date`, `effective_to_date`, `publication_id`, `publication_sequence BIGINT`, `publication_ts`, all timestamps as TIMESTAMP;
- append-only: the same set without `effective_to_date`, which is derived at query time.

Timestamps:

* `TIMESTAMP` is used despite the `_date` suffix, because multiple loads per day are expected.
* Literals are rendered with an explicit `+00:00` offset and microsecond precision, so they do not depend on the session time zone. The session time zone is still set to `UTC` and recorded, and the `session_timezone_utc` probe checks it.
* `publication_ts = origin + sequence × interval`, so timestamps strictly increase in logical order per scope. Prevalidation checks this.

Determinism:

* Every value is `xxhash64`/`sha2` of `(seed, table_index, scope, key_id, change_epoch, salt)`, so contents do not depend on partitioning. The tests verify this by repartitioning and by generating subsets of pairs.
* In publication `q ≥ 2`, a key changes with probability `changed_value_fraction`. Its payload is a function of its latest change epoch, so unchanged keys have byte-identical payloads.

Other knobs:

* Keys are unique per `(scope, snapshot)`. Negative inputs (duplicate keys, schema drift) are generated explicitly for fault cells.
* `category_skew` concentrates `attr_category`, the aggregation dimension. `scope_size_skew` gives Zipf-sized scopes. `null_fraction` nulls the payload columns.

## Strategies (identical data, sequencing, validation)

* **A `scd2_full`**: close all active rows of the scope at `publication_ts`, then insert the whole snapshot. Every row is versioned, even if unchanged.
* **B `append_only_full`**: insert the whole snapshot; no UPDATE or DELETE ever runs on data tables (unit-tested). The latest and as-of snapshots are selected through the ledger; history end dates come from `LEAD(publication_ts)` over publication order.
* **C `scd2_changed_only`**: optional, off by default, reported separately. It is **not** the mandated semantics.

All strategies share the same fence, ledger, ordering modes and validators. Cells with the same data profile read the same staging tables.

## Timing

| measure | definition |
|---|---|
| generation | per staging table, reported in `generation.jsonl`; never inside a publication timing |
| prevalidation | schema + counts + duplicate-key checks before the first attempt (`prevalidation_s`) |
| attempt duration | wall time of the single `spark.sql` call |
| end-to-end | first attempt start → acknowledged commit, including backoff (`end_to_end_s`) |
| backoff | recorded per attempt |
| maintenance | `OPTIMIZE` per target table after the publications (`table_metrics.jsonl`, phase `maintenance`) |
| read queries | first run after `CLEAR CACHE` + N repeats; result hashes recorded and compared across strategies/formats |

Warm-up publications (`benchmark.warmup_publications`) are excluded from latency statistics but kept in the data. Cells run in a seeded random order. Repetitions use fresh tables.

## Storage and write amplification: what is observable

* `DESCRIBE DETAIL`: `numFiles` and `sizeInBytes` (current snapshot only), table features, properties and clustering.
* `DESCRIBE HISTORY` `operationMetrics`: engine-reported per-commit counters (files added/removed, rows, deletion vectors, bytes when reported). These are **counters, not ground truth**. We do not claim "bytes rewritten" from them.
* Not collected: object-store byte counts (they need storage credentials), and VACUUM effects (the 7-day retention default; disabling the check is unsafe).
* Iceberg: whichever of the above the probes show to work. Anything else is reported as not observable.

## Concurrency analysis

For each writer the suite records:

- submission time (launcher);
- `arrival` (process start);
- `ready` (barrier);
- `start` (after go + offset);
- first `attempt_start`;
- every `attempt_end` with its category;
- `commit_ack_time`;
- the ledger commit version, from `_metadata.row_commit_version` when available, otherwise by diffing ledger versions with time travel outside the transaction.

Reported per cell:

- Kendall τ-b and inversion counts for submission/arrival/ready/first-attempt/sequence against commit-ack and commit-version order;
- whether FIFO held;
- throughput (commits / (last ack − first attempt));
- p50/p95/p99 latency;
- attempts per publication, conflict categories and starved writers;
- maximum concurrently in-flight attempts, and distinct processes and clusters, as **evidence of real overlap**.

Ordering modes:

- `off`: natural, unsequenced. History corruption from out-of-order commits is **FLAGGED**, never repaired.
- `monotonic`: stale publications are rejected.
- `sequenced`: strict contiguous order; early arrivals wait. This measures the throughput penalty.

## Validation (per cell, after all writers finish)

| class | checks |
|---|---|
| atomicity | no ghost data (rows without a ledger row); whole-batch membership (every ledger publication has exactly `rows_per_table` rows in every listed table); `publication_ts`/sequence match the ledger; fence `commit_count` == ledger rows; ledger agrees with client/reconciled outcomes (no ghost ledger rows after rollback); a failed fault step left no trace |
| idempotency | exactly one ledger row per publication; no duplicate versions |
| history | one active row per key; no timestampless rows; no inverted/empty intervals; contiguous validity intervals |
| ordering | exactly one active publication per scope and it equals the ledger's highest committed sequence; the current snapshot equals the staged snapshot of that publication; fence sequence == ledger max; contiguous sequences in `sequenced` |
| parity | per-publication content fingerprints identical across SCD2/append-only and Delta/Iceberg cells with the same profile |

Every check's SQL is stored with its result. `dbxbench verify` recomputes everything from the tables. A SKIPPED or NOT_RUN check can never produce a PASS verdict.

## Reader-visible snapshots (measured, not assumed)

Post-run validators inspect one stable state after every writer has finished, so in-flight transactions are never mistaken for corruption. Separately, while the writers of a contention cell run, the coordinator's `ReaderProbe` issues one non-transactional query every 250 ms. That query reads the latest committed sequence of every target table and of the ledger for the scope.

A sample is *skewed* when a single query observes different values. That means a reader outside any transaction saw one table of an atomic publication set before another. Results go to `reader_probe.jsonl` and the report ("reader skew"). The docs promise repeatable reads *inside* a transaction; they do not promise cross-table snapshot consistency to plain readers. The suite therefore reports what it observes and asserts nothing. Overlapping-table cells are excluded, because their publications touch different table subsets by design.
