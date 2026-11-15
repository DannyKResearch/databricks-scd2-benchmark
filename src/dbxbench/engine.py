"""Atomic publication engine.

Contract (spec phase 03):
* The whole publication is ONE SQL block submitted via ONE ``executor.execute`` (== ``spark.sql``) call.
* Prevalidation runs before (and is timed separately from) the transaction.
* Retries re-run the ENTIRE block (fresh reads inside the transaction), only for classified,
  retryable categories, with full-jitter backoff, an attempt cap and a global deadline.
* After any outcome that is not a clean server acknowledgement, the ledger is consulted before
  deciding anything. Transaction status is never inferred from an exception alone.
"""

from __future__ import annotations

import concurrent.futures as cf
import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from .config import FaultScenario, OrderingMode, Strategy
from .errors import Category, Classified, ClientTimeout, SimulatedLostAck, classify
from .executor import SqlExecutor
from .model import AttemptRecord, Publication, publication_id
from .naming import CellTables
from .retry import Deadline, RetryPolicy
from .sqlgen import Dialect, TxnContext, build_publication_sql, staging_columns, target_columns
from .sqlutil import TableRef, iso, sql_int, sql_token, utc_now
from .telemetry import TelemetrySink

log = logging.getLogger(__name__)


class PublicationStatus(StrEnum):
    COMMITTED = "COMMITTED"
    COMMITTED_RECONCILED = "COMMITTED_RECONCILED"  # our attempt committed but the ack was lost/timed out
    ALREADY_COMMITTED = "ALREADY_COMMITTED"  # idempotent no-op: another attempt/writer committed it
    REJECTED_STALE = "REJECTED_STALE"
    ROLLED_BACK_INJECTED = "ROLLED_BACK_INJECTED"
    PREVALIDATION_FAILED = "PREVALIDATION_FAILED"
    FAILED_NONRETRYABLE = "FAILED_NONRETRYABLE"
    FAILED_RETRIES_EXHAUSTED = "FAILED_RETRIES_EXHAUSTED"
    DEADLINE_EXCEEDED = "DEADLINE_EXCEEDED"
    INTEGRITY_ANOMALY = "INTEGRITY_ANOMALY"  # e.g. injected fault but ledger shows commit, or >1 ledger rows
    OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN"  # an abandoned attempt of ours was still running when we gave up

    @property
    def is_committed(self) -> bool:
        return self in {PublicationStatus.COMMITTED, PublicationStatus.COMMITTED_RECONCILED}


@dataclass(frozen=True)
class EngineFaults:
    in_txn: FaultScenario | None = None
    lost_ack_first_attempt: bool = False
    client_timeout_first_attempt_s: float | None = None
    on_first_submit: Callable[[], None] | None = None  # e.g. schedule process termination
    skip_prevalidation: bool = False


@dataclass(frozen=True)
class LedgerState:
    rows: list[dict[str, Any]]

    @property
    def committed(self) -> bool:
        return len(self.rows) >= 1

    @property
    def duplicated(self) -> bool:
        return len(self.rows) > 1

    def tokens(self) -> set[str]:
        return {str(r.get("attempt_token")) for r in self.rows}


@dataclass
class PrevalidationReport:
    ok: bool
    problems: list[str]
    duration_s: float


@dataclass
class PublicationResult:
    publication: Publication
    writer_id: str
    status: PublicationStatus
    attempts: list[AttemptRecord] = field(default_factory=list)
    prevalidation: PrevalidationReport | None = None
    committed_token: str | None = None
    first_attempt_start: str | None = None
    commit_ack_time: str | None = None
    end_to_end_s: float | None = None
    detail: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "publication": self.publication.to_json(),
            "writer_id": self.writer_id,
            "status": self.status.value,
            "attempt_count": len(self.attempts),
            "committed_token": self.committed_token,
            "first_attempt_start": self.first_attempt_start,
            "commit_ack_time": self.commit_ack_time,
            "end_to_end_s": self.end_to_end_s,
            "prevalidation_s": self.prevalidation.duration_s if self.prevalidation else None,
            "prevalidation_problems": self.prevalidation.problems if self.prevalidation else [],
            "detail": self.detail,
        }


def _normalize_type(t: str) -> str:
    return t.strip().lower().replace(" ", "")


def describe_columns(executor: SqlExecutor, ref: TableRef) -> list[tuple[str, str]]:
    rows = executor.execute(f"DESCRIBE TABLE {ref.sql}")
    cols: list[tuple[str, str]] = []
    for r in rows:
        name = str(r.get("col_name") or "").strip()
        if not name or name.startswith("#"):
            break
        cols.append((name, _normalize_type(str(r.get("data_type")))))
    return cols


class PublicationEngine:
    def __init__(
        self,
        *,
        executor: SqlExecutor,
        dialect: Dialect,
        run_id: str,
        cell_key: str,
        strategy: Strategy,
        ordering: OrderingMode,
        writer_id: str,
        tables: CellTables,
        staging: tuple[TableRef, ...],
        policy: RetryPolicy,
        telemetry: TelemetrySink,
        expected_ts: Callable[[int], Any],
        staging_count_check: bool = True,
        max_ordering_waits: int = 200,
        sql_sink: Callable[[str, str], None] | None = None,
        cancel_on_client_timeout: bool = False,
    ) -> None:
        self.executor = executor
        self.dialect = dialect
        self.run_id = run_id
        self.cell_key = cell_key
        self.strategy = strategy
        self.ordering = ordering
        self.writer_id = writer_id
        self.tables = tables
        self.staging = staging
        self.policy = policy
        self.telemetry = telemetry
        self.expected_ts = expected_ts
        self.staging_count_check = staging_count_check
        self.max_ordering_waits = max_ordering_waits
        self.sql_sink = sql_sink
        # A client timeout means the CLIENT stopped waiting; the server statement keeps running and the ledger
        # decides its outcome. Interrupting an open transaction is a different fault and is opt-in.
        self.cancel_on_client_timeout = cancel_on_client_timeout
        self._prevalidated: dict[tuple[str, int], PrevalidationReport] = {}
        self._pool = cf.ThreadPoolExecutor(max_workers=4, thread_name_prefix="dbxbench-txn")
        self._abandoned: list[cf.Future[Any]] = []

    # ------------------------------------------------------------------ prevalidation
    def prevalidate(self, pub: Publication) -> PrevalidationReport:
        key = (pub.dataset_scope, pub.sequence)
        if key in self._prevalidated:
            return self._prevalidated[key]
        t0 = time.perf_counter()
        problems: list[str] = []
        if pub.publication_id != publication_id(pub.dataset_scope, pub.sequence):
            problems.append(f"publication_id {pub.publication_id} does not match scope/sequence")
        expected = self.expected_ts(pub.sequence)
        if pub.publication_ts != expected:
            problems.append(f"publication_ts {iso(pub.publication_ts)} != expected {iso(expected)}")
        if pub.sequence > 1 and not self.expected_ts(pub.sequence - 1) < pub.publication_ts:
            problems.append("publication_ts is not strictly increasing in sequence order")
        exp_stg = [(n, _normalize_type(t)) for n, t in staging_columns()]
        exp_tgt = [(n, _normalize_type(t)) for n, t in target_columns(self.strategy)]
        for idx in pub.table_indexes:
            stg, tgt = self.staging[idx], self.tables.targets[idx]
            got_stg = describe_columns(self.executor, stg)
            if got_stg != exp_stg:
                problems.append(f"staging schema mismatch for {stg.plain}: {got_stg} != {exp_stg}")
            got_tgt = describe_columns(self.executor, tgt)
            if got_tgt != exp_tgt:
                problems.append(f"target schema mismatch for {tgt.plain}: {got_tgt} != {exp_tgt}")
            rows = self.executor.execute(
                f"SELECT count(*) AS n, count(DISTINCT business_key) AS k, "
                f"sum(CASE WHEN business_key IS NULL OR dataset_scope IS NULL OR payload_hash IS NULL "
                f"THEN 1 ELSE 0 END) AS nulls, count(DISTINCT publication_id) AS pids, "
                f"min(publication_id) AS pid FROM {stg.sql} "
                f"WHERE dataset_scope = {sql_token(pub.dataset_scope)} "
                f"AND publication_sequence = {sql_int(pub.sequence)}"
            )
            r = rows[0] if rows else {}
            n, k = int(r.get("n") or 0), int(r.get("k") or 0)
            if n != pub.expected_rows:
                problems.append(f"{stg.plain}: staged rows {n} != expected {pub.expected_rows}")
            if k != n:
                problems.append(f"{stg.plain}: {n - k} duplicate (dataset_scope, business_key) rows")
            if int(r.get("nulls") or 0):
                problems.append(f"{stg.plain}: {r.get('nulls')} rows with NULL key/hash")
            if n and (int(r.get("pids") or 0) != 1 or r.get("pid") != pub.publication_id):
                problems.append(f"{stg.plain}: staged publication_id does not match {pub.publication_id}")
        report = PrevalidationReport(not problems, problems, time.perf_counter() - t0)
        if report.ok:
            self._prevalidated[key] = report
        return report

    # ------------------------------------------------------------------ reconciliation
    def reconcile(self, pub: Publication) -> LedgerState:
        rows = self.executor.execute(
            f"SELECT attempt_token, writer_id, attempt_number, publication_sequence "
            f"FROM {self.tables.ledger.sql} WHERE dataset_scope = {sql_token(pub.dataset_scope)} "
            f"AND publication_id = {sql_token(pub.publication_id)}"
        )
        return LedgerState(rows)

    # ------------------------------------------------------------------ publish
    def build_sql(self, pub: Publication, attempt_number: int, token: str, fault: FaultScenario | None) -> str:
        ctx = TxnContext(
            run_id=self.run_id,
            cell_key=self.cell_key,
            strategy=self.strategy,
            ordering=self.ordering,
            writer_id=self.writer_id,
            attempt_token=token,
            attempt_number=attempt_number,
            tables=self.tables,
            staging=self.staging,
            fault=fault,
            staging_count_check=self.staging_count_check,
        )
        return build_publication_sql(ctx, pub, self.dialect)

    def _submit(self, sql: str, token: str, timeout_s: float) -> None:
        fut = self._pool.submit(self.executor.execute, sql, tag=token)
        try:
            fut.result(timeout=timeout_s)
        except cf.TimeoutError as exc:
            cancelled = self.executor.cancel(token) if self.cancel_on_client_timeout else False

            def _late(f: cf.Future[Any], token: str = token) -> None:
                err = f.exception()
                self.telemetry.emit(
                    "late_attempt_finished",
                    attempt_token=token,
                    outcome="ERROR" if err else "SUCCEEDED_AFTER_CLIENT_TIMEOUT",
                    error=str(err)[:500] if err else None,
                )

            fut.add_done_callback(_late)
            self._abandoned.append(fut)
            raise ClientTimeout(f"client timeout after {timeout_s:.3f}s (cancel_requested={cancelled})") from exc

    def publish(self, pub: Publication, faults: EngineFaults | None = None) -> PublicationResult:
        faults = faults or EngineFaults()
        result = PublicationResult(pub, self.writer_id, PublicationStatus.FAILED_NONRETRYABLE)
        self.telemetry.emit("publication_start", **self._pub_fields(pub))
        if not faults.skip_prevalidation:
            result.prevalidation = self.prevalidate(pub)
            self.telemetry.emit(
                "prevalidation_done",
                ok=result.prevalidation.ok,
                duration_s=result.prevalidation.duration_s,
                problems=result.prevalidation.problems[:20],
                **self._pub_fields(pub),
            )
            if not result.prevalidation.ok:
                result.status = PublicationStatus.PREVALIDATION_FAILED
                result.detail = "; ".join(result.prevalidation.problems)[:2000]
                return self._finish(result, None)

        deadline = Deadline(self.policy.overall_deadline_seconds)
        t_start = time.perf_counter()
        conflict_attempts = 0
        ordering_waits = 0
        attempt_no = 0
        while True:
            if deadline.expired():
                result.status = PublicationStatus.DEADLINE_EXCEEDED
                break
            attempt_no += 1
            token = f"{self.writer_id}-{uuid.uuid4().hex[:16]}"
            first = attempt_no == 1
            fault = faults.in_txn if first else None
            sql = self.build_sql(pub, attempt_no, token, fault)
            if self.sql_sink is not None:
                self.sql_sink(f"{pub.publication_id}.attempt{attempt_no:03d}", sql)
            rec = AttemptRecord(
                run_id=self.run_id,
                cell_key=self.cell_key,
                writer_id=self.writer_id,
                publication_id=pub.publication_id,
                dataset_scope=pub.dataset_scope,
                sequence=pub.sequence,
                attempt_number=attempt_no,
                attempt_token=token,
                attempt_start=iso(utc_now()),
                simulated_fault=fault.value if fault else None,
            )
            if result.first_attempt_start is None:
                result.first_attempt_start = rec.attempt_start
            result.attempts.append(rec)
            self.telemetry.emit(
                "attempt_start", attempt_number=attempt_no, attempt_token=token, **self._pub_fields(pub)
            )
            timeout = min(self.policy.per_attempt_timeout_seconds, max(deadline.remaining(), 0.001))
            if first and faults.client_timeout_first_attempt_s is not None:
                timeout = faults.client_timeout_first_attempt_s
                rec.simulated_fault = FaultScenario.CLIENT_TIMEOUT.value
            t0 = time.perf_counter()
            try:
                if first and faults.on_first_submit is not None:
                    faults.on_first_submit()
                self._submit(sql, token, timeout)
                if first and faults.lost_ack_first_attempt:
                    rec.simulated_fault = FaultScenario.LOST_ACK.value
                    raise SimulatedLostAck("simulated lost reply after successful commit")
            except Exception as exc:
                c = classify(exc)
                self._close_attempt(rec, t0, "ERROR", c)
                decision = self._handle_error(pub, rec, c, result)
                if decision == "stop":
                    break
                # retry path
                if c.category is Category.ORDERING_NOT_READY:
                    ordering_waits += 1
                    if ordering_waits > self.max_ordering_waits:
                        result.status = PublicationStatus.FAILED_RETRIES_EXHAUSTED
                        result.detail = "ordering wait cap reached"
                        break
                    delay = self.policy.backoff_seconds(min(ordering_waits, 4))
                else:
                    conflict_attempts += 1
                    if conflict_attempts >= self.policy.max_attempts:
                        result.status = PublicationStatus.FAILED_RETRIES_EXHAUSTED
                        result.detail = f"last category {c.category.value}"
                        break
                    delay = self.policy.backoff_seconds(conflict_attempts)
                delay = min(delay, max(deadline.remaining(), 0.0))
                rec.backoff_s = delay
                self.telemetry.emit("backoff", seconds=delay, category=c.category.value, **self._pub_fields(pub))
                time.sleep(delay)
                continue
            # clean acknowledgement from the server
            self._close_attempt(rec, t0, "COMMIT_ACK", None)
            result.commit_ack_time = rec.attempt_end
            state = self.reconcile(pub)  # confirm durable ledger record (outside the timed attempt)
            if state.duplicated:
                result.status = PublicationStatus.INTEGRITY_ANOMALY
                result.detail = f"ledger has {len(state.rows)} rows for {pub.publication_id}"
            elif token in state.tokens():
                result.status = PublicationStatus.COMMITTED
                result.committed_token = token
            else:
                result.status = PublicationStatus.INTEGRITY_ANOMALY
                result.detail = "server acknowledged but ledger has no row for this attempt token"
            break
        if result.status in (
            PublicationStatus.FAILED_RETRIES_EXHAUSTED,
            PublicationStatus.DEADLINE_EXCEEDED,
            PublicationStatus.FAILED_NONRETRYABLE,
            PublicationStatus.REJECTED_STALE,
        ) and any(a.category == Category.UNKNOWN_OUTCOME.value for a in result.attempts):
            # An abandoned attempt with an unknown outcome may still commit: wait for it (bounded), then the
            # ledger decides. Never report a definitive failure while one of our attempts may still commit.
            pending = [f for f in self._abandoned if not f.done()]
            if pending:
                cf.wait(pending, timeout=self.policy.per_attempt_timeout_seconds)
            still_running = any(not f.done() for f in self._abandoned)
            state = self.reconcile(pub)
            mine = {a.attempt_token for a in result.attempts}
            if state.committed and not state.duplicated:
                tok = next(iter(state.tokens()))
                result.detail = f"{result.status.value} superseded by final ledger check: {result.detail or ''}"
                result.status = (
                    PublicationStatus.COMMITTED_RECONCILED if tok in mine else PublicationStatus.ALREADY_COMMITTED
                )
                result.committed_token = tok
            elif still_running:
                result.detail = f"abandoned attempt still running; was {result.status.value}: {result.detail or ''}"
                result.status = PublicationStatus.OUTCOME_UNKNOWN
        result.end_to_end_s = time.perf_counter() - t_start
        return self._finish(result, deadline)

    def _handle_error(self, pub: Publication, rec: AttemptRecord, c: Classified, result: PublicationResult) -> str:
        """Return 'stop' (terminal, result.status set) or 'retry'."""
        cat = c.category
        # Every outcome that could coexist with a commit of THIS publication is checked against the ledger.
        # STALE included: an abandoned (timed-out) attempt of ours may have committed and advanced the fence.
        if cat in (
            Category.DUPLICATE_PUBLICATION,
            Category.UNKNOWN_OUTCOME,
            Category.INJECTED_FAULT,
            Category.REJECTED_STALE,
        ):
            state = self.reconcile(pub)
            rec.reconciled = True
            rec.extra["ledger_rows"] = len(state.rows)
            self.telemetry.emit(
                "reconcile",
                category=cat.value,
                ledger_rows=len(state.rows),
                attempt_token=rec.attempt_token,
                **self._pub_fields(pub),
            )
            if state.duplicated:
                result.status = PublicationStatus.INTEGRITY_ANOMALY
                result.detail = f"ledger has {len(state.rows)} rows for {pub.publication_id}"
                return "stop"
            mine = {a.attempt_token for a in result.attempts}
            if state.committed:
                tok = next(iter(state.tokens()))
                if cat is Category.INJECTED_FAULT:
                    result.status = PublicationStatus.INTEGRITY_ANOMALY
                    result.detail = "injected in-transaction fault but ledger shows the publication committed"
                elif tok in mine:
                    result.status = PublicationStatus.COMMITTED_RECONCILED
                    result.committed_token = tok
                else:
                    result.status = PublicationStatus.ALREADY_COMMITTED
                    result.committed_token = tok
                return "stop"
            if cat is Category.INJECTED_FAULT:
                result.status = PublicationStatus.ROLLED_BACK_INJECTED
                return "stop"
            if cat is Category.REJECTED_STALE:
                result.status = PublicationStatus.REJECTED_STALE
                result.detail = c.message
                return "stop"
            if cat is Category.DUPLICATE_PUBLICATION:
                result.status = PublicationStatus.INTEGRITY_ANOMALY
                result.detail = "transaction reported duplicate but ledger has no row"
                return "stop"
            return "retry"  # UNKNOWN_OUTCOME and provably not committed: safe to retry whole block
        if c.retryable:
            return "retry"
        result.status = PublicationStatus.FAILED_NONRETRYABLE
        result.detail = f"{cat.value}: {c.condition or ''} {c.message}"[:2000]
        return "stop"

    def _close_attempt(self, rec: AttemptRecord, t0: float, outcome: str, c: Classified | None) -> None:
        rec.attempt_end = iso(utc_now())
        rec.duration_s = time.perf_counter() - t0
        rec.outcome = outcome
        if c is not None:
            rec.category = c.category.value
            rec.error_condition = c.condition
            rec.sqlstate = c.sqlstate
            rec.message = c.message[:1000]
        self.telemetry.emit(
            "attempt_end",
            attempt_number=rec.attempt_number,
            attempt_token=rec.attempt_token,
            outcome=outcome,
            category=rec.category,
            error_condition=rec.error_condition,
            sqlstate=rec.sqlstate,
            message=rec.message,
            duration_s=rec.duration_s,
            simulated_fault=rec.simulated_fault,
            attempt_start=rec.attempt_start,
            attempt_end=rec.attempt_end,
            sequence=rec.sequence,
            publication_id=rec.publication_id,
            dataset_scope=rec.dataset_scope,
        )

    def _finish(self, result: PublicationResult, deadline: Deadline | None) -> PublicationResult:
        self.telemetry.emit(
            "publication_end",
            status=result.status.value,
            attempts=len(result.attempts),
            committed_token=result.committed_token,
            commit_ack_time=result.commit_ack_time,
            first_attempt_start=result.first_attempt_start,
            end_to_end_s=result.end_to_end_s,
            detail=result.detail,
            **self._pub_fields(result.publication),
        )
        return result

    def _pub_fields(self, pub: Publication) -> dict[str, Any]:
        return {
            "publication_id": pub.publication_id,
            "dataset_scope": pub.dataset_scope,
            "sequence": pub.sequence,
            "publication_ts": iso(pub.publication_ts),
            "tables": [self.tables.targets[i].name for i in pub.table_indexes],
        }

    def close(self, wait: bool = False) -> None:
        """``wait=True`` blocks until abandoned (timed-out) attempts finish, so state can be inspected."""
        self._pool.shutdown(wait=wait, cancel_futures=not wait)
