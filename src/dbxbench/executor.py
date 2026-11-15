"""SQL executors. The engine only talks to this interface, so the exact SQL text and the number of
submissions are observable and testable (one ``execute`` call == one ``spark.sql`` call)."""

from __future__ import annotations

import concurrent.futures as cf
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

from .sqlutil import iso, utc_now
from .storage import append_text

Row = dict[str, Any]


class SqlExecutor(Protocol):
    def execute(self, sql: str, *, tag: str | None = None) -> list[Row]: ...

    def cancel(self, tag: str) -> bool: ...


class SqlLog:
    """Append-only record of executed SQL (reporting.save_executed_sql)."""

    def __init__(self, path: Path | None) -> None:
        self.path = path
        self._lock = threading.Lock()
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, sql: str, tag: str | None) -> None:
        if self.path is None:
            return
        with self._lock:
            append_text(
                self.path, f"-- [{iso(utc_now())}] tag={tag or '-'}\n{sql.rstrip().rstrip(';')};\n\n", fsync=False
            )


class SessionUnusable(RuntimeError):
    """The Spark session can no longer be used safely (stalled statement or broken transaction context)."""


SESSION_POISON_MARKERS = ("TRANSACTION_CONTEXT_MISMATCH",)


class SparkSqlExecutor:
    """Executes each SQL text with exactly one ``spark.sql`` call.

    Every call for this session runs on ONE dedicated thread, in submission order. Databricks binds a
    multi-statement transaction to the thread that started it ("use a dedicated session and thread for each
    transaction"); a statement issued from another thread while a transaction is still open fails with
    ``TRANSACTION_CONTEXT_MISMATCH`` and breaks the session for every later statement. Serializing on one
    thread means a caller that gave up waiting (client timeout) can never overlap the abandoned transaction:
    its next statement (e.g. ledger reconciliation) simply queues until that transaction has finished.
    """

    def __init__(self, spark: Any, sql_log: SqlLog | None = None, stall_timeout_s: float = 3600.0) -> None:
        self.spark = spark
        self.sql_log = sql_log or SqlLog(None)
        self.submissions = 0
        self.stall_timeout_s = stall_timeout_s
        self._lock = threading.Lock()
        self._session = cf.ThreadPoolExecutor(max_workers=1, thread_name_prefix="dbxbench-session")
        self._poisoned: str | None = None

    @property
    def poisoned(self) -> str | None:
        return self._poisoned

    def execute(self, sql: str, *, tag: str | None = None) -> list[Row]:
        if self._poisoned:
            raise SessionUnusable(f"SESSION_UNUSABLE: {self._poisoned}")
        with self._lock:
            self.submissions += 1
        fut = self._session.submit(self._run, sql, tag)
        try:
            return fut.result(timeout=self.stall_timeout_s)
        except cf.TimeoutError as exc:
            self._poisoned = f"a statement did not finish within {self.stall_timeout_s:.0f}s (tag={tag or '-'})"
            raise SessionUnusable(f"SESSION_UNUSABLE: {self._poisoned}") from exc
        except Exception as exc:
            if any(m in str(exc) for m in SESSION_POISON_MARKERS):
                self._poisoned = f"transaction context broken: {str(exc)[:300]}"
            raise

    def _run(self, sql: str, tag: str | None) -> list[Row]:
        self.sql_log.write(sql, tag)
        if tag:
            self._set_tag(tag)  # Spark Connect tags are thread-local: set them on the session thread
        try:
            df = self.spark.sql(sql)  # the ONE call
            rows = df.collect()
        finally:
            if tag:
                self._clear_tag(tag)
        return [r.asDict(recursive=True) for r in rows]

    # Cancellation is best effort and only used where explicitly requested (writer termination tests).
    def _set_tag(self, tag: str) -> None:
        try:
            if hasattr(self.spark, "addTag"):
                self.spark.addTag(tag)
            else:  # pragma: no cover - classic without tags
                self.spark.sparkContext.setJobGroup(tag, tag, interruptOnCancel=True)
        except Exception:  # noqa: S110 - tagging is diagnostic only
            pass

    def _clear_tag(self, tag: str) -> None:
        try:
            if hasattr(self.spark, "removeTag"):
                self.spark.removeTag(tag)
        except Exception:  # noqa: S110
            pass

    def cancel(self, tag: str) -> bool:
        try:
            if hasattr(self.spark, "interruptTag"):
                self.spark.interruptTag(tag)
                return True
            self.spark.sparkContext.cancelJobGroup(tag)
            return True
        except Exception:
            return False

    def close(self) -> None:
        self._session.shutdown(wait=False, cancel_futures=True)


class RecordingExecutor:
    """Dry-run / unit-test executor. Records SQL; optional ``responder`` scripts results/errors."""

    def __init__(self, responder: Callable[[str], list[Row]] | None = None) -> None:
        self.statements: list[str] = []
        self.tags: list[str | None] = []
        self.cancelled: list[str] = []
        self.responder = responder

    def execute(self, sql: str, *, tag: str | None = None) -> list[Row]:
        self.statements.append(sql)
        self.tags.append(tag)
        if self.responder is None:
            return []
        return self.responder(sql)

    def cancel(self, tag: str) -> bool:
        self.cancelled.append(tag)
        return True
