"""Core value objects shared by the generator, engine, harness and validators."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any

from .sqlutil import iso, parse_utc

SCOPE_RE = re.compile(r"^scope_[0-9]{4}$")
PUBLICATION_ID_RE = re.compile(r"^scope_[0-9]{4}_p[0-9]{6}$")

# Exact payload schema (documented in docs/methodology.md).
KEY_COLUMNS: tuple[tuple[str, str], ...] = (("dataset_scope", "STRING"), ("business_key", "STRING"))
PAYLOAD_COLUMNS: tuple[tuple[str, str], ...] = (
    ("attr_int", "BIGINT"),
    ("attr_decimal", "DECIMAL(18,4)"),
    ("attr_category", "STRING"),
    ("attr_flag", "BOOLEAN"),
    ("attr_text", "STRING"),
    ("payload_hash", "STRING"),
)
STAGING_EXTRA: tuple[tuple[str, str], ...] = (("publication_id", "STRING"), ("publication_sequence", "BIGINT"))
SCD2_EXTRA: tuple[tuple[str, str], ...] = (
    ("effective_from_date", "TIMESTAMP"),
    ("effective_to_date", "TIMESTAMP"),
    ("publication_id", "STRING"),
    ("publication_sequence", "BIGINT"),
    ("publication_ts", "TIMESTAMP"),
)
# Append-only stores no end date: it is derived at query time with LEAD() over publication order.
APPEND_EXTRA: tuple[tuple[str, str], ...] = (
    ("effective_from_date", "TIMESTAMP"),
    ("publication_id", "STRING"),
    ("publication_sequence", "BIGINT"),
    ("publication_ts", "TIMESTAMP"),
)
LEDGER_COLUMNS: tuple[tuple[str, str], ...] = (
    ("run_id", "STRING"),
    ("cell_key", "STRING"),
    ("dataset_scope", "STRING"),
    ("publication_id", "STRING"),
    ("publication_sequence", "BIGINT"),
    ("publication_ts", "TIMESTAMP"),
    ("strategy", "STRING"),
    ("ordering_mode", "STRING"),
    ("writer_id", "STRING"),
    ("attempt_token", "STRING"),
    ("attempt_number", "INT"),
    ("table_names", "ARRAY<STRING>"),
    ("rows_per_table", "BIGINT"),
    # current_timestamp() evaluated by the ledger INSERT statement inside the transaction.
    # It is NOT the server commit timestamp (which is only observable via table history).
    ("ledger_write_ts", "TIMESTAMP"),
)
FENCE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("dataset_scope", "STRING"),
    ("last_sequence", "BIGINT"),
    ("last_publication_id", "STRING"),
    ("last_publication_ts", "TIMESTAMP"),
    ("commit_count", "BIGINT"),
    ("updated_by_attempt", "STRING"),
)


def scope_name(index: int) -> str:
    return f"scope_{index + 1:04d}"


def publication_id(scope: str, sequence: int) -> str:
    return f"{scope}_p{sequence:06d}"


def publication_ts(origin_utc: str, interval_millis: int, sequence: int) -> datetime:
    """Logical publication timestamp: strictly increasing in sequence order, microsecond precision."""
    if sequence < 1:
        raise ValueError("publication sequence starts at 1")
    return parse_utc(origin_utc) + timedelta(milliseconds=interval_millis * sequence)


@dataclass(frozen=True)
class Publication:
    dataset_scope: str
    publication_id: str
    sequence: int
    publication_ts: datetime
    table_indexes: tuple[int, ...]
    expected_rows: int
    profile_key: str

    def __post_init__(self) -> None:
        if not SCOPE_RE.match(self.dataset_scope):
            raise ValueError(f"bad dataset_scope {self.dataset_scope!r}")
        if not PUBLICATION_ID_RE.match(self.publication_id):
            raise ValueError(f"bad publication_id {self.publication_id!r}")
        if self.sequence < 1:
            raise ValueError("sequence must be >= 1")
        if self.publication_ts.tzinfo is None:
            raise ValueError("publication_ts must be timezone-aware")
        if not self.table_indexes or len(set(self.table_indexes)) != len(self.table_indexes):
            raise ValueError("table_indexes must be non-empty and unique")
        if self.expected_rows < 0:
            raise ValueError("expected_rows must be >= 0")

    def to_json(self) -> dict[str, Any]:
        d = asdict(self)
        d["publication_ts"] = iso(self.publication_ts)
        d["table_indexes"] = list(self.table_indexes)
        return d

    @staticmethod
    def from_json(d: dict[str, Any]) -> Publication:
        return Publication(
            dataset_scope=d["dataset_scope"],
            publication_id=d["publication_id"],
            sequence=int(d["sequence"]),
            publication_ts=parse_utc(d["publication_ts"]),
            table_indexes=tuple(int(i) for i in d["table_indexes"]),
            expected_rows=int(d["expected_rows"]),
            profile_key=d["profile_key"],
        )


@dataclass(frozen=True)
class DataProfile:
    """Everything that determines staged data content. Same profile => byte-identical logical data."""

    profile_key: str
    seed: int
    table_count: int
    rows_per_table_per_publication: int
    publication_count: int
    dataset_scope_count: int
    payload_width_bytes: int
    changed_value_fraction: float
    category_skew: float
    category_cardinality: int
    scope_size_skew: float
    null_fraction: float
    publication_interval_millis: int
    publication_timestamp_origin_utc: str
    extra_sequences: int = 0  # contention cells stage sequences beyond publication_count

    @property
    def total_sequences(self) -> int:
        return self.publication_count + self.extra_sequences

    def rows_for_scope(self, scope_index: int) -> int:
        """Deterministic per-scope snapshot size; Zipf-like when scope_size_skew > 0."""
        if self.scope_size_skew == 0 or self.dataset_scope_count == 1:
            return self.rows_per_table_per_publication
        weights = [1.0 / (i + 1) ** self.scope_size_skew for i in range(self.dataset_scope_count)]
        mean = sum(weights) / len(weights)
        scaled: float = self.rows_per_table_per_publication * weights[scope_index] / mean
        return max(1, int(scaled + 0.5))

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class AttemptRecord:
    run_id: str
    cell_key: str
    writer_id: str
    publication_id: str
    dataset_scope: str
    sequence: int
    attempt_number: int
    attempt_token: str
    attempt_start: str
    attempt_end: str | None = None
    duration_s: float | None = None
    outcome: str = "PENDING"
    category: str | None = None
    error_condition: str | None = None
    sqlstate: str | None = None
    message: str | None = None
    backoff_s: float = 0.0
    reconciled: bool = False
    simulated_fault: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)
