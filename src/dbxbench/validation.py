"""Independent post-run invariant validators.

Validators run against a single stable post-run state (after every writer of a cell has finished),
so in-flight transactions are never mistaken for corruption. Every check is a SQL query whose text
is persisted next to its result, so verification is reproducible from the tables alone.

Check classes
* ``atomicity``   – ghost data, ghost ledger rows, partial table membership, fence/ledger agreement.
* ``idempotency`` – exactly-once ledger rows per publication.
* ``history``     – SCD2 one-active-row, contiguous intervals, inverted boundaries, timestamp sanity.
* ``ordering``    – active publication == highest committed sequence, contiguous sequences.

In ordering mode ``off`` the history/ordering checks are *measurements*: failures are reported as
FLAGGED (out-of-order commits corrupted history, which is what that mode exists to expose).
Atomicity/idempotency failures are always FAIL.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any

from .config import OrderingMode, Strategy
from .executor import SqlExecutor
from .naming import CellTables
from .sqlutil import TableRef, quote_ident, sql_token


class CheckStatus(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    FLAGGED = "FLAGGED"
    SKIPPED = "SKIPPED"
    ERROR = "ERROR"


@dataclass
class CheckResult:
    name: str
    klass: str
    table: str | None
    status: CheckStatus
    violations: int
    sql: str
    sample: list[dict[str, Any]] = field(default_factory=list)
    note: str | None = None

    def to_json(self) -> dict[str, Any]:
        d = asdict(self)
        d["status"] = self.status.value
        return d


@dataclass
class ValidationReport:
    cell_key: str
    checks: list[CheckResult]

    @property
    def verdict(self) -> str:
        sts = {c.status for c in self.checks}
        if CheckStatus.ERROR in sts or CheckStatus.FAIL in sts:
            return "FAIL"
        if CheckStatus.FLAGGED in sts:
            return "PASS_WITH_FLAGGED_ORDERING_ANOMALIES"
        if not self.checks or sts == {CheckStatus.SKIPPED}:
            return "NOT_RUN"
        return "PASS"

    def failed(self, klass: str | None = None) -> list[CheckResult]:
        return [
            c
            for c in self.checks
            if c.status in (CheckStatus.FAIL, CheckStatus.ERROR) and (klass is None or c.klass == klass)
        ]

    def to_json(self) -> dict[str, Any]:
        return {"cell_key": self.cell_key, "verdict": self.verdict, "checks": [c.to_json() for c in self.checks]}


class Validator:
    def __init__(
        self,
        executor: SqlExecutor,
        tables: CellTables,
        staging: tuple[TableRef, ...],
        strategy: Strategy,
        ordering: OrderingMode,
        cell_key: str,
    ) -> None:
        self.x = executor
        self.tables = tables
        self.staging = staging
        self.strategy = strategy
        self.ordering = ordering
        self.cell_key = cell_key
        self.results: list[CheckResult] = []

    # -------------------------------------------------------------------- helpers
    def _check(self, name: str, klass: str, sql: str, table: TableRef | None = None, note: str | None = None) -> None:
        try:
            rows = self.x.execute(sql)
        except Exception as exc:
            self.results.append(
                CheckResult(name, klass, table.name if table else None, CheckStatus.ERROR, -1, sql, note=str(exc)[:500])
            )
            return
        n = len(rows)
        if n == 0:
            status = CheckStatus.PASS
        elif self.ordering is OrderingMode.OFF and klass in ("history", "ordering"):
            status = CheckStatus.FLAGGED
        else:
            status = CheckStatus.FAIL
        self.results.append(
            CheckResult(name, klass, table.name if table else None, status, n, sql, sample=rows[:5], note=note)
        )

    def _ledger_for(self, t: TableRef) -> str:
        return f"(SELECT * FROM {self.tables.ledger.sql} WHERE array_contains(table_names, {sql_token(t.name)}))"

    # -------------------------------------------------------------------- checks
    def run(
        self, expected_committed: Iterable[str] | None = None, expected_not_committed: Iterable[str] | None = None
    ) -> ValidationReport:
        led = self.tables.ledger.sql
        fence = self.tables.fence.sql
        self._check(
            "ledger_exactly_once",
            "idempotency",
            f"SELECT dataset_scope, publication_id, count(*) AS n FROM {led} GROUP BY 1, 2 HAVING count(*) > 1",
        )
        self._check(
            "ledger_rows_have_timestamps",
            "atomicity",
            f"SELECT publication_id FROM {led} WHERE publication_ts IS NULL OR publication_sequence IS NULL",
        )
        # Every committed transaction increments commit_count exactly once and inserts exactly one ledger row.
        self._check(
            "fence_commit_count_matches_ledger",
            "atomicity",
            f"SELECT f.dataset_scope, f.commit_count, coalesce(l.n, 0) AS ledger_rows FROM {fence} f "
            f"LEFT JOIN (SELECT dataset_scope, count(*) AS n FROM {led} GROUP BY 1) l "
            f"ON f.dataset_scope = l.dataset_scope WHERE f.commit_count <> coalesce(l.n, 0)",
        )
        if self.ordering is not OrderingMode.OFF:
            self._check(
                "fence_sequence_matches_ledger_max",
                "ordering",
                f"SELECT f.dataset_scope, f.last_sequence, l.mx FROM {fence} f LEFT JOIN "
                f"(SELECT dataset_scope, max(publication_sequence) AS mx FROM {led} GROUP BY 1) l "
                f"ON f.dataset_scope = l.dataset_scope WHERE f.last_sequence <> coalesce(l.mx, 0)",
            )
        if self.ordering is OrderingMode.SEQUENCED:
            self._check(
                "sequenced_contiguous_sequences",
                "ordering",
                f"SELECT dataset_scope, count(*) AS n, max(publication_sequence) AS mx FROM {led} "
                f"GROUP BY 1 HAVING count(*) <> max(publication_sequence) OR min(publication_sequence) <> 1",
            )
        for t in self.tables.targets:
            self._table_checks(t)
        report = ValidationReport(self.cell_key, list(self.results))
        if expected_committed is not None or expected_not_committed is not None:
            report.checks.append(
                self._ledger_vs_results(set(expected_committed or []), set(expected_not_committed or []))
            )
        return report

    def _ledger_vs_results(self, committed: set[str], not_committed: set[str]) -> CheckResult:
        sql = f"SELECT DISTINCT publication_id FROM {self.tables.ledger.sql}"
        rows = self.x.execute(sql)
        in_ledger = {r["publication_id"] for r in rows}
        missing = sorted(committed - in_ledger)
        ghosts = sorted(in_ledger & not_committed)
        bad = [{"missing_from_ledger": p} for p in missing] + [{"ghost_ledger_row": p} for p in ghosts]
        return CheckResult(
            "ledger_matches_client_outcomes",
            "atomicity",
            None,
            CheckStatus.PASS if not bad else CheckStatus.FAIL,
            len(bad),
            sql,
            sample=bad[:5],
            note="committed per client/reconciliation must be in ledger; rolled-back must not be",
        )

    def _table_checks(self, t: TableRef) -> None:
        q = quote_ident
        led_t = self._ledger_for(t)
        # Atomicity: data rows whose publication has no ledger row (partial commit / rollback leak).
        self._check(
            "no_ghost_data",
            "atomicity",
            f"SELECT DISTINCT d.publication_id FROM {t.sql} d LEFT ANTI JOIN {led_t} l "
            f"ON d.publication_id = l.publication_id AND d.dataset_scope = l.dataset_scope",
            t,
        )
        # Atomicity: every ledger publication that names this table has exactly its rows here.
        self._check(
            "whole_batch_membership",
            "atomicity",
            f"SELECT l.publication_id, l.rows_per_table, coalesce(d.n, 0) AS rows_in_table FROM {led_t} l "
            f"LEFT JOIN (SELECT publication_id, count(*) AS n FROM {t.sql} GROUP BY 1) d "
            f"ON l.publication_id = d.publication_id WHERE coalesce(d.n, 0) <> l.rows_per_table",
            t,
        )
        self._check(
            "publication_ts_matches_ledger",
            "atomicity",
            f"SELECT DISTINCT d.publication_id FROM {t.sql} d JOIN {led_t} l ON d.publication_id = l.publication_id "
            f"WHERE d.publication_ts <> l.publication_ts OR d.publication_sequence <> l.publication_sequence",
            t,
        )
        self._check(
            "no_timestampless_rows",
            "history",
            f"SELECT publication_id, count(*) AS n FROM {t.sql} WHERE effective_from_date IS NULL "
            f"OR publication_ts IS NULL OR effective_from_date <> publication_ts GROUP BY 1",
            t,
        )
        self._check(
            "no_duplicate_versions",
            "idempotency",
            f"SELECT dataset_scope, business_key, publication_id, count(*) AS n FROM {t.sql} "
            f"GROUP BY 1, 2, 3 HAVING count(*) > 1",
            t,
        )
        latest = (
            f"(SELECT dataset_scope, max_by(publication_id, publication_sequence) AS publication_id, "
            f"max(publication_sequence) AS publication_sequence FROM {led_t} GROUP BY dataset_scope)"
        )
        if self.strategy is Strategy.APPEND_ONLY_FULL:
            current = (
                f"(SELECT d.* FROM {t.sql} d JOIN {latest} lt ON d.dataset_scope = lt.dataset_scope "
                f"AND d.publication_id = lt.publication_id)"
            )
        else:
            current = f"(SELECT * FROM {t.sql} WHERE effective_to_date IS NULL)"
            self._check(
                "one_active_row_per_key",
                "history",
                f"SELECT dataset_scope, business_key, count(*) AS n FROM {t.sql} WHERE effective_to_date IS NULL "
                f"GROUP BY 1, 2 HAVING count(*) > 1",
                t,
            )
            if self.strategy is Strategy.SCD2_FULL:
                self._check(
                    "one_active_publication_equals_ledger_latest",
                    "ordering",
                    f"SELECT a.dataset_scope, a.pubs, a.any_pub, lt.publication_id AS ledger_latest FROM "
                    f"(SELECT dataset_scope, count(DISTINCT publication_id) AS pubs, min(publication_id) AS any_pub "
                    f"FROM {t.sql} WHERE effective_to_date IS NULL GROUP BY 1) a FULL OUTER JOIN {latest} lt "
                    f"ON a.dataset_scope = lt.dataset_scope "
                    f"WHERE a.pubs IS NULL OR lt.publication_id IS NULL OR a.pubs <> 1 "
                    f"OR a.any_pub <> lt.publication_id",
                    t,
                )
            self._check(
                "no_inverted_or_empty_intervals",
                "history",
                f"SELECT dataset_scope, business_key, effective_from_date, effective_to_date FROM {t.sql} "
                f"WHERE effective_to_date IS NOT NULL AND effective_to_date <= effective_from_date",
                t,
            )
            self._check(
                "contiguous_validity_intervals",
                "history",
                f"SELECT dataset_scope, business_key, effective_from_date, effective_to_date, nxt FROM ("
                f"SELECT dataset_scope, business_key, effective_from_date, effective_to_date, "
                f"lead(effective_from_date) OVER (PARTITION BY dataset_scope, business_key "
                f"ORDER BY effective_from_date, publication_sequence) AS nxt FROM {t.sql}) w "
                f"WHERE (nxt IS NOT NULL AND (effective_to_date IS NULL OR effective_to_date <> nxt)) "
                f"OR (nxt IS NULL AND effective_to_date IS NOT NULL)",
                t,
                note="full-snapshot key sets are fixed, so the last version of every key must be open",
            )
        # Current snapshot must equal the staged snapshot of the latest committed publication, exactly.
        idx = self.tables.targets.index(t)
        stg = self.staging[idx]
        staged_latest = (
            f"(SELECT s.dataset_scope, s.business_key, s.payload_hash FROM {stg.sql} s JOIN {latest} lt "
            f"ON s.dataset_scope = lt.dataset_scope AND s.publication_sequence = lt.publication_sequence)"
        )
        self._check(
            "current_snapshot_equals_staged_latest",
            "ordering" if self.strategy is not Strategy.APPEND_ONLY_FULL else "atomicity",
            f"SELECT 'missing_or_changed' AS kind, x.dataset_scope, x.business_key FROM {staged_latest} x "
            f"LEFT ANTI JOIN {current} c ON x.dataset_scope = c.dataset_scope AND x.business_key = c.business_key "
            f"AND x.payload_hash = c.payload_hash "
            f"UNION ALL SELECT 'unexpected' AS kind, c.dataset_scope, c.business_key FROM {current} c "
            f"LEFT ANTI JOIN {staged_latest} x ON x.dataset_scope = c.dataset_scope "
            f"AND x.business_key = c.business_key AND x.payload_hash = c.payload_hash",
            t,
        )
        del q


def fingerprint_by_publication(executor: SqlExecutor, t: TableRef) -> dict[str, tuple[int, int]]:
    """Per-publication content fingerprint of the rows a publication inserted."""
    rows = executor.execute(
        f"SELECT publication_id, count(*) AS n, "
        f"sum(CAST(xxhash64(dataset_scope, business_key, payload_hash, publication_sequence) AS DECIMAL(38,0))) AS fp "
        f"FROM {t.sql} GROUP BY publication_id"
    )
    return {r["publication_id"]: (int(r["n"]), int(r["fp"] or 0)) for r in rows}


def parity_check(
    executor: SqlExecutor,
    label: str,
    left: tuple[TableRef, ...],
    right: tuple[TableRef, ...],
    filter_fn: Callable[[str], bool] | None = None,
) -> CheckResult:
    """Deterministic content parity between two cells (e.g. SCD2 vs append-only, Delta vs Iceberg).

    For full-snapshot strategies every publication inserts the complete snapshot, so the rows tagged
    with a publication_id must be identical across strategies and formats."""
    diffs: list[dict[str, Any]] = []
    for a, b in zip(left, right, strict=True):
        fa, fb = fingerprint_by_publication(executor, a), fingerprint_by_publication(executor, b)
        keys = set(fa) | set(fb)
        if filter_fn is not None:
            keys = {k for k in keys if filter_fn(k)}
        for k in sorted(keys):
            if fa.get(k) != fb.get(k):
                diffs.append(
                    {"left": a.name, "right": b.name, "publication_id": k, "left_fp": fa.get(k), "right_fp": fb.get(k)}
                )
    return CheckResult(
        f"parity:{label}",
        "parity",
        None,
        CheckStatus.PASS if not diffs else CheckStatus.FAIL,
        len(diffs),
        "per-publication count + sum(xxhash64(scope, key, payload_hash, seq))",
        sample=diffs[:5],
    )
