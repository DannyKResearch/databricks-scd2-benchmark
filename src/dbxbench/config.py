"""Strongly typed, validated benchmark configuration.

The YAML contract lives in ``config/example.yaml``. Every key is validated by pydantic with
``extra="forbid"`` so that typos fail fast instead of silently using defaults.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
from enum import StrEnum
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# Documented limit: "A transaction can access up to 100 tables combined" (docs.databricks.com/aws/en/transactions).
DOCUMENTED_MAX_TABLES_PER_TRANSACTION = 100

_IDENT = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_PLACEHOLDER_CATALOGS = {"", "replace_me", "changeme", "your_catalog"}


class Strategy(StrEnum):
    SCD2_FULL = "scd2_full"
    APPEND_ONLY_FULL = "append_only_full"
    SCD2_CHANGED_ONLY = "scd2_changed_only"  # optional, NOT the mandated full-snapshot semantics


class TableFormat(StrEnum):
    DELTA = "delta"
    ICEBERG = "iceberg"


class OrderingMode(StrEnum):
    OFF = "off"  # natural/unsequenced: no order fence; out-of-order histories are FLAGGED, not fixed
    MONOTONIC = "monotonic"  # reject any publication whose sequence <= last committed sequence
    SEQUENCED = "sequenced"  # strict contiguous: seq must equal last+1; early arrivals wait/retry


class TransactionMode(StrEnum):
    BEGIN_ATOMIC = "begin_atomic"
    # Local OSS Spark has no BEGIN ATOMIC. This mode submits the same script as a plain
    # (NON-ATOMIC) SQL compound statement so SQL shape/semantics/validators can be tested locally.
    # Results produced in this mode are never evidence of atomicity.
    LOCAL_NONATOMIC_EMULATION = "local_nonatomic_emulation"


class ComputeMode(StrEnum):
    JOBS_CLUSTER = "jobs_cluster"
    SERVERLESS = "serverless"
    EXISTING_CLUSTER = "existing_cluster"
    LOCAL = "local"


class Launcher(StrEnum):
    INLINE = "inline"  # one process, sequential; only valid for a single writer
    LOCAL_PROCESS = "local_process"  # N separate OS processes, each its own Spark session (harness test)
    DATABRICKS_JOBS = "databricks_jobs"  # N separate Databricks job runs (primary evidence)


class ContentionMode(StrEnum):
    SAME_SCOPE = "same_scope"
    DISJOINT_SCOPES = "disjoint_scopes"
    OVERLAPPING_TABLES = "overlapping_tables"


class StartMode(StrEnum):
    BARRIER = "barrier"
    STAGGERED = "staggered"
    RANDOMIZED = "randomized"


class NamespaceMode(StrEnum):
    PER_RUN_SCHEMA = "per_run_schema"
    FIXED_SCHEMA = "fixed_schema"


class SchemaLayout(StrEnum):
    SINGLE = "single"  # every table of the run in <schema_prefix>_<run_id>
    PER_CELL = "per_cell"  # staging in the run schema; each cell's tables in <run schema>__<cell_key>


class SweepMode(StrEnum):
    BASELINE_ONLY = "baseline_only"
    ONE_AT_A_TIME = "one_at_a_time"
    CARTESIAN = "cartesian"


class FaultScenario(StrEnum):
    AFTER_FIRST_UPDATE = "after_first_update"
    AFTER_FIRST_INSERT = "after_first_insert"
    LAST_TABLE = "last_table"
    AFTER_LEDGER = "after_ledger"
    DUPLICATE_DELIVERY = "duplicate_delivery"
    LATE_DELIVERY = "late_delivery"
    LOST_ACK = "lost_ack"
    CLIENT_TIMEOUT = "client_timeout"
    WRITER_TERMINATION = "writer_termination"
    SCHEMA_MISMATCH = "schema_mismatch"
    DUPLICATE_KEYS_INPUT = "duplicate_keys_input"
    CONSTRAINT_VIOLATION_REAL = "constraint_violation_real"  # genuine engine error mid-transaction (Delta)


def _coerce_yaml_off(v: Any) -> Any:
    """YAML 1.1 parses an unquoted ``off`` as boolean False; map it back to OrderingMode.OFF."""
    if v is False:
        return "off"
    if isinstance(v, list):
        return ["off" if x is False else x for x in v]
    return v


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, use_enum_values=False)


class ProjectConfig(_Strict):
    name: str = "dbx-scd2-transactions"
    seed: int = 20261114
    catalog: str
    schema_prefix: str = "scd2_bench"
    namespace_mode: NamespaceMode = NamespaceMode.PER_RUN_SCHEMA
    fixed_schema: str | None = None
    # per_cell keeps each schema small, e.g. under a per-schema table quota (Free Edition: 100 tables)
    schema_layout: SchemaLayout = SchemaLayout.SINGLE
    results_root: str = "./results/runs"
    s3_staging_uri: str | None = None
    require_ownership_marker: bool = True
    allow_cleanup: bool = False
    max_estimated_total_rows: int = Field(2_000_000_000, gt=0)
    max_estimated_storage_gb: float = Field(500.0, gt=0)
    max_tables_per_transaction: int = Field(DOCUMENTED_MAX_TABLES_PER_TRANSACTION, ge=4)

    @field_validator("catalog")
    @classmethod
    def _catalog(cls, v: str) -> str:
        if v.lower() in _PLACEHOLDER_CATALOGS:
            raise ValueError("project.catalog must be set to an authorized Unity Catalog catalog")
        if not _IDENT.match(v):
            raise ValueError(f"project.catalog {v!r} must match {_IDENT.pattern}")
        return v

    @field_validator("schema_prefix")
    @classmethod
    def _prefix(cls, v: str) -> str:
        if not _IDENT.match(v) or len(v) > 40:
            raise ValueError(f"project.schema_prefix {v!r} must match {_IDENT.pattern} and be <= 40 chars")
        return v

    @field_validator("fixed_schema")
    @classmethod
    def _fixed(cls, v: str | None) -> str | None:
        if v is not None and not _IDENT.match(v):
            raise ValueError(f"project.fixed_schema {v!r} must match {_IDENT.pattern}")
        return v

    @field_validator("s3_staging_uri")
    @classmethod
    def _s3(cls, v: str | None) -> str | None:
        if v is not None and not re.match(r"^s3://[a-z0-9.\-]{3,63}(/[A-Za-z0-9_./\-]*)?$", v):
            raise ValueError("project.s3_staging_uri must be an s3:// URI without credentials")
        return v

    @field_validator("max_tables_per_transaction")
    @classmethod
    def _cap(cls, v: int) -> int:
        if v > DOCUMENTED_MAX_TABLES_PER_TRANSACTION:
            raise ValueError(
                f"max_tables_per_transaction={v} exceeds the documented cap of "
                f"{DOCUMENTED_MAX_TABLES_PER_TRANSACTION} tables accessed per transaction"
            )
        return v

    @model_validator(mode="after")
    def _ns(self) -> ProjectConfig:
        if self.namespace_mode is NamespaceMode.FIXED_SCHEMA and not self.fixed_schema:
            raise ValueError("namespace_mode=fixed_schema requires project.fixed_schema")
        if self.schema_layout is SchemaLayout.PER_CELL and self.namespace_mode is not NamespaceMode.PER_RUN_SCHEMA:
            raise ValueError("schema_layout=per_cell requires namespace_mode=per_run_schema")
        return self


class ComputeConfig(_Strict):
    required_min_dbr: str = "18.0"
    preferred_mode: ComputeMode = ComputeMode.JOBS_CLUSTER
    transaction_mode: TransactionMode = TransactionMode.BEGIN_ATOMIC
    timezone: str = "UTC"
    max_parallel_job_runs: int = Field(20, ge=1, le=200)

    @field_validator("required_min_dbr")
    @classmethod
    def _dbr(cls, v: str) -> str:
        if not re.match(r"^\d+\.\d+$", v):
            raise ValueError("required_min_dbr must look like '18.0'")
        return v

    @model_validator(mode="after")
    def _modes(self) -> ComputeConfig:
        if (
            self.transaction_mode is TransactionMode.LOCAL_NONATOMIC_EMULATION
            and self.preferred_mode is not ComputeMode.LOCAL
        ):
            raise ValueError("local_nonatomic_emulation is only allowed with compute.preferred_mode=local")
        return self


class WorkloadConfig(_Strict):
    table_count: int = Field(3, ge=1, le=49)
    rows_per_table_per_publication: int = Field(1000, ge=1)
    publication_count: int = Field(3, ge=1, le=500)
    dataset_scope_count: int = Field(1, ge=1, le=1000)
    payload_width_bytes: int = Field(128, ge=8, le=65536)
    changed_value_fraction: float = Field(1.0, ge=0.0, le=1.0)
    category_skew: float = Field(0.0, ge=0.0, le=0.99)
    category_cardinality: int = Field(50, ge=1, le=100000)
    scope_size_skew: float = Field(0.0, ge=0.0, le=3.0)
    null_fraction: float = Field(0.0, ge=0.0, le=1.0)
    staging_materialized_before_timing: bool = True
    publication_interval_millis: int = Field(1000, ge=1)
    publication_timestamp_origin_utc: str = "2026-01-01T00:00:00Z"
    in_transaction_staging_count_check: bool = True

    @field_validator("staging_materialized_before_timing")
    @classmethod
    def _must_stage(cls, v: bool) -> bool:
        if not v:
            raise ValueError("staging must be materialized before the timed transaction (spec phase 02/03)")
        return v

    @field_validator("publication_timestamp_origin_utc")
    @classmethod
    def _origin(cls, v: str) -> str:
        if not re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z$", v):
            raise ValueError("publication_timestamp_origin_utc must be ISO-8601 UTC ending in 'Z'")
        return v


class SweepConfig(_Strict):
    mode: SweepMode = SweepMode.BASELINE_ONLY
    rows_per_table_per_publication: list[int] = Field(default_factory=list)
    table_count: list[int] = Field(default_factory=list)
    publication_count: list[int] = Field(default_factory=list)
    changed_value_fraction: list[float] = Field(default_factory=list)
    payload_width_bytes: list[int] = Field(default_factory=list)
    dataset_scope_count: list[int] = Field(default_factory=list)


class ConcurrencyConfig(_Strict):
    enabled: bool = True
    launcher: Launcher = Launcher.DATABRICKS_JOBS
    strategies: list[Strategy] = Field(default_factory=lambda: [Strategy.SCD2_FULL])
    formats: list[TableFormat] = Field(default_factory=lambda: [TableFormat.DELTA])
    writer_counts: list[int] = Field(default_factory=lambda: [1, 2, 5])
    contention_modes: list[ContentionMode] = Field(default_factory=lambda: [ContentionMode.SAME_SCOPE])
    start_modes: list[StartMode] = Field(default_factory=lambda: [StartMode.BARRIER])
    strict_ordering_modes: list[OrderingMode] = Field(
        default_factory=lambda: [OrderingMode.OFF, OrderingMode.SEQUENCED]
    )
    publications_per_writer: int = Field(1, ge=1, le=100)
    overlapping_table_window: int = Field(2, ge=1)
    repetitions: int = Field(1, ge=1, le=50)
    max_attempts: int = Field(5, ge=1, le=1000)
    initial_backoff_ms: int = Field(250, ge=1)
    max_backoff_ms: int = Field(8000, ge=1)
    per_attempt_timeout_seconds: float = Field(600, gt=0)
    overall_deadline_seconds: float = Field(1800, gt=0)
    barrier_timeout_seconds: float = Field(300, gt=0)
    barrier_go_lead_seconds: float = Field(5.0, ge=0)
    barrier_poll_seconds: float = Field(0.5, gt=0)
    stagger_millis: int = Field(500, ge=0)
    random_start_max_millis: int = Field(5000, ge=0)
    worker_job_id: int | None = None
    job_run_poll_seconds: float = Field(10.0, gt=0)

    @field_validator("strict_ordering_modes", mode="before")
    @classmethod
    def _yaml_off(cls, v: Any) -> Any:
        return _coerce_yaml_off(v)

    @model_validator(mode="after")
    def _check(self) -> ConcurrencyConfig:
        if self.max_backoff_ms < self.initial_backoff_ms:
            raise ValueError("max_backoff_ms must be >= initial_backoff_ms")
        if any(w < 1 for w in self.writer_counts):
            raise ValueError("writer_counts must be >= 1")
        if self.per_attempt_timeout_seconds > self.overall_deadline_seconds:
            raise ValueError("per_attempt_timeout_seconds must be <= overall_deadline_seconds")
        return self


class BenchmarkConfig(_Strict):
    repetitions: int = Field(3, ge=1, le=100)
    warmup_publications: int = Field(1, ge=0)
    randomize_order: bool = True
    measure_read_queries: bool = True
    read_query_repetitions: int = Field(3, ge=1, le=50)
    measure_storage_metrics: bool = True
    include_maintenance_cost: bool = True
    clear_cache_before_cold_queries: bool = True
    fail_on_unexpected_skip: bool = False
    ordering_mode: OrderingMode = OrderingMode.SEQUENCED

    @field_validator("ordering_mode", mode="before")
    @classmethod
    def _yaml_off(cls, v: Any) -> Any:
        return _coerce_yaml_off(v)


class FaultInjectionConfig(_Strict):
    enabled: bool = True
    formats: list[TableFormat] = Field(default_factory=lambda: [TableFormat.DELTA])
    scenarios: list[FaultScenario] = Field(
        default_factory=lambda: [
            FaultScenario.AFTER_FIRST_UPDATE,
            FaultScenario.AFTER_FIRST_INSERT,
            FaultScenario.LAST_TABLE,
            FaultScenario.DUPLICATE_DELIVERY,
            FaultScenario.LATE_DELIVERY,
            FaultScenario.LOST_ACK,
        ]
    )
    client_timeout_seconds: float = Field(0.05, gt=0)


class ReportingConfig(_Strict):
    charts: bool = True
    save_executed_sql: bool = True
    save_execution_plan: bool = True
    results_retention_days: int = Field(30, ge=1)


class BenchConfig(_Strict):
    project: ProjectConfig
    compute: ComputeConfig = ComputeConfig()
    workload: WorkloadConfig = WorkloadConfig()
    sweeps: SweepConfig = SweepConfig()
    strategies: list[Strategy] = Field(default_factory=lambda: [Strategy.SCD2_FULL, Strategy.APPEND_ONLY_FULL])
    formats: list[TableFormat] = Field(default_factory=lambda: [TableFormat.DELTA, TableFormat.ICEBERG])
    concurrency: ConcurrencyConfig = ConcurrencyConfig()
    benchmark: BenchmarkConfig = BenchmarkConfig()
    fault_injection: FaultInjectionConfig = FaultInjectionConfig()
    reporting: ReportingConfig = ReportingConfig()

    @model_validator(mode="after")
    def _cross(self) -> BenchConfig:
        if not self.strategies:
            raise ValueError("at least one strategy is required")
        if len(set(self.strategies)) != len(self.strategies) or len(set(self.formats)) != len(self.formats):
            raise ValueError("strategies/formats must not contain duplicates")
        # tables accessed in one transaction: T targets + T staging + ledger + fence
        max_t = max([self.workload.table_count, *self.sweeps.table_count])
        accessed = 2 * max_t + 2
        if accessed > self.project.max_tables_per_transaction:
            raise ValueError(
                f"table_count={max_t} implies {accessed} tables accessed per transaction "
                f"(targets + staging + ledger + fence) > max_tables_per_transaction="
                f"{self.project.max_tables_per_transaction}"
            )
        if self.compute.transaction_mode is TransactionMode.BEGIN_ATOMIC and (
            self.compute.preferred_mode is ComputeMode.LOCAL
        ):
            raise ValueError("begin_atomic cannot run on local OSS Spark; use local_nonatomic_emulation locally")
        if self.project.schema_layout is SchemaLayout.PER_CELL and self.compute.preferred_mode is ComputeMode.LOCAL:
            raise ValueError("schema_layout=per_cell is for Unity Catalog; local mode uses a single schema")
        if self.concurrency.launcher is Launcher.DATABRICKS_JOBS and self.compute.preferred_mode is ComputeMode.LOCAL:
            raise ValueError("launcher=databricks_jobs requires Databricks compute, not preferred_mode=local")
        if self.concurrency.launcher is Launcher.INLINE and any(w > 1 for w in self.concurrency.writer_counts):
            raise ValueError("launcher=inline cannot provide independent concurrent writers (writer_counts > 1)")
        return self

    # ------------------------------------------------------------------ helpers
    def canonical_json(self) -> str:
        return json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))

    def config_hash(self) -> str:
        return hashlib.sha256(self.canonical_json().encode()).hexdigest()

    @property
    def is_local(self) -> bool:
        return self.compute.preferred_mode is ComputeMode.LOCAL


# ---------------------------------------------------------------------- loading
OVERRIDE_PATHS: dict[str, tuple[str, ...]] = {
    "tables": ("workload", "table_count"),
    "rows": ("workload", "rows_per_table_per_publication"),
    "publications": ("workload", "publication_count"),
    "scopes": ("workload", "dataset_scope_count"),
    "changed_fraction": ("workload", "changed_value_fraction"),
    "writers": ("concurrency", "writer_counts"),
    "formats": ("formats",),
    "strategies": ("strategies",),
    "contention": ("concurrency", "contention_modes"),
    "ordering": ("concurrency", "strict_ordering_modes"),
    "launcher": ("concurrency", "launcher"),
    "catalog": ("project", "catalog"),
    "results_root": ("project", "results_root"),
    "repetitions": ("benchmark", "repetitions"),
    "worker_job_id": ("concurrency", "worker_job_id"),
    "compute_mode": ("compute", "preferred_mode"),
    "schema_prefix": ("project", "schema_prefix"),
    "schema_layout": ("project", "schema_layout"),
}


def apply_overrides(raw: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(raw)
    for key, value in overrides.items():
        if value is None:
            continue
        if key not in OVERRIDE_PATHS:
            raise KeyError(f"unknown override {key!r}")
        path = OVERRIDE_PATHS[key]
        node = out
        for part in path[:-1]:
            node = node.setdefault(part, {})
        node[path[-1]] = value
    return out


def load_config(path: str | Path, overrides: dict[str, Any] | None = None) -> BenchConfig:
    text = Path(path).read_text(encoding="utf-8")
    raw = yaml.safe_load(text) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"{path}: top-level YAML must be a mapping")
    _reject_secrets(raw)
    return BenchConfig.model_validate(apply_overrides(raw, overrides or {}))


_SECRET_KEYS = re.compile(r"(token|password|secret|client_secret|access_key|pat)$", re.I)
_SECRET_VALUES = re.compile(r"(dapi[0-9a-f]{20,}|AKIA[0-9A-Z]{16})")


def _reject_secrets(node: Any, path: str = "") -> None:
    """Secrets must come from the Databricks SDK auth chain, never from YAML."""
    if isinstance(node, dict):
        for k, v in node.items():
            if _SECRET_KEYS.search(str(k)):
                raise ValueError(f"config key {path}{k} looks like a secret; use Databricks SDK auth instead")
            _reject_secrets(v, f"{path}{k}.")
    elif isinstance(node, list):
        for i, v in enumerate(node):
            _reject_secrets(v, f"{path}[{i}].")
    elif isinstance(node, str) and _SECRET_VALUES.search(node):
        raise ValueError(f"config value at {path.rstrip('.')} looks like a credential; remove it")
