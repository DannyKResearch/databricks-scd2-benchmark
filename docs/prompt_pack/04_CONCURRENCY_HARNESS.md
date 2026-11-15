# PHASE 04 — Real independent writer concurrency

Implement a Databricks Jobs API/SDK launcher (prefer Workflows job with task parameters) to start N independent PySpark job runs competing over the same managed table set. Do not use Python thread pools inside one SparkContext as the primary evidence. Worker receives run_id, job_id, publication_id, sequence, staged snapshot identifiers, barrier/deadline config; no generation work in critical timed interval.

Tests: same tables/same scope; same tables/disjoint scope; overlapping subsets; staggered arrivals; synchronized attempts; randomized startup; different transaction lengths; 1,2,5,10,20+ writers. Configure N, rate, seed, startup skew, per-task timeout, max retries and sequence policy. Track explicit `arrival_time`, `ready_time`, `attempt_start`, `attempt_end`, `commit_ack_time`; note server actual commit timestamps may differ and capture table/transaction metadata when accessible.

Barrier: external coordinator/manifest ready flags, bounded timeout, fail closed. Do not implement barrier with Spark executors hanging; do not use driver memory in one job as cross-job coordination. Ensure no deadlocks when not all writers arrive. All jobs must use same immutable pre-generated data.

Analyze FIFO scientifically: distinguish job submission order, barrier-ready order, first transaction attempt order and observed commit order. Report inversions/Kendall tau, starvation, retry counts, transient/permanent failures, p50/p95/p99 latency, throughput, attempt success, eventual success, conflict categories. Never assume or require FIFO unless explicitly adding an external queue. Separate optional sequenced publisher mode and measure its throughput penalty.

Collect event records in an external append-only telemetry sink safe for concurrent writes, avoiding contaminating the transactional target write set. If using Delta logs telemetry writes, document their overhead and whether it causes additional conflicts; optional S3 JSONL per worker/run is acceptable.

Acceptance: evidence of truly overlapping distinct job runs, no hidden serial launcher bottleneck, correct job/run correlation, per-writer failure classification and reproducible per-run output.
