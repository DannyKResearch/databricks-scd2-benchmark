# PHASE 01 — Repository and configuration

Implement:
- pip-installable package and CLI: `dbxbench doctor`, `init`, `generate`, `run`, `verify`, `analyze`, `cleanup` plus `--config`, `--run-id`, `--dry-run`.
- `config/example.yaml` as baseline, with strongly typed and validated keys. No hardcoded catalog/schema, S3 bucket or cluster ID. CLI overrides for tables, row count, writers, publications, formats, strategy, workload.
- Run isolation: each benchmark uses unique `run_id`, managed table prefix and scratch staging location; publish manifest of settings and environment into results. Safe reruns with unique IDs; explicit `--resume` only where correct.
- Preflight `doctor`: UC, managed storage, permissions, DBR version, catalog commits, SQL transactions, Iceberg table support, external staging (optional), cluster/job launcher availability, ability to inspect history/metrics. Output machine-readable support matrix.
- When unsupported: actionable explanation and a SKIPPED result; do not silently degrade to independent writes.
- Limits: tables accessed in transaction <= documented cap (currently 100 including ledger and other tables), configurable guardrails on total rows and estimated storage/cost. Keep setup/DDL outside transaction.
- No secrets in YAML or logs. CLI accepts standard Databricks SDK auth via environment/secure profile, not embedded credentials.
- Document local `pytest` (SQL mocked) and Databricks integration execution.

Acceptance: lint/type checks, config validation tests, `doctor` smoke, `--dry-run` emits intended SQL/table names safely, no production-table overwrite.
