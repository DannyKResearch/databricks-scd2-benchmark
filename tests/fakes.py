"""Fake SQL backend for engine unit tests: models the ledger + in-transaction duplicate check."""

from __future__ import annotations

import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from dbxbench.config import Strategy
from dbxbench.sqlgen import staging_columns, target_columns


class FakeSparkError(Exception):
    def __init__(self, msg: str, condition: str | None = None, sqlstate: str | None = None):
        super().__init__(msg)
        self._c, self._s = condition, sqlstate

    def getCondition(self):
        return self._c

    def getSqlState(self):
        return self._s


def conflict() -> FakeSparkError:
    return FakeSparkError(
        "[DELTA_CONCURRENT_APPEND] ConcurrentAppendException: files were added", "DELTA_CONCURRENT_APPEND", "2D521"
    )


@dataclass
class FakeStore:
    """In-memory stand-in for the tables a publication touches, used as a ``RecordingExecutor`` responder.

    It answers the engine's prevalidation queries (DESCRIBE TABLE, staging counts) and ledger lookups, and
    simulates each ``BEGIN ...`` block according to the next entry in ``behaviors`` (default ``commit``):

    * ``commit``: record the attempt token in the ledger (the in-block duplicate check runs first);
    * ``conflict`` / ``conflict_first``: raise a retryable OCC conflict (``_first`` ignores the ledger state);
    * ``sleep:<s>``: commit after a delay, with a commit-time conflict if another attempt committed meanwhile;
    * ``stale`` / ``stale_first``: our STALE signal (``_first`` fires before the duplicate check);
    * ``not_ready``: our SEQUENCE_NOT_READY signal (sequenced-mode wait);
    * ``commit_then_lose_reply``: commit, then raise a transport error (outcome unknown, actually committed);
    * ``lost_not_committed``: raise a transport error without committing;
    * ``nonretryable`` / ``unclassified``: a documented non-retryable error / an unknown exception.
    """

    strategy: Strategy = Strategy.SCD2_FULL
    behaviors: list[str] = field(default_factory=list)  # consumed per BEGIN block
    ledger: dict[str, list[str]] = field(default_factory=dict)
    blocks: list[str] = field(default_factory=list)
    staged_rows: int = 10
    staged_distinct: int | None = None
    on_block: Callable[[str], None] | None = None

    def __call__(self, sql: str):
        s = sql.strip()
        if s.startswith("DESCRIBE TABLE"):
            cols = staging_columns() if "`stg_" in s else target_columns(self.strategy)
            return [{"col_name": n, "data_type": t.lower()} for n, t in cols]
        if s.startswith("SELECT count(*) AS n, count(DISTINCT"):
            scope = re.search(r"dataset_scope = '([^']+)'", s).group(1)
            seq = int(re.search(r"publication_sequence = (\d+)", s).group(1))
            pid = f"{scope}_p{seq:06d}"
            k = self.staged_rows if self.staged_distinct is None else self.staged_distinct
            return [{"n": self.staged_rows, "k": k, "nulls": 0, "pids": 1, "pid": pid}]
        if s.startswith("SELECT attempt_token"):
            pid = re.search(r"publication_id = '([^']+)'", s).group(1)
            return [
                {"attempt_token": t, "writer_id": "w", "attempt_number": 1, "publication_sequence": 1}
                for t in self.ledger.get(pid, [])
            ]
        if s.startswith("BEGIN"):
            self.blocks.append(s)
            if self.on_block:
                self.on_block(s)
            beh = self.behaviors.pop(0) if self.behaviors else "commit"
            pid = re.search(r"`publication_id` = '([^']+)'", s).group(1)
            token = re.search(r"`updated_by_attempt` = '([^']+)'", s).group(1)
            if beh == "conflict_first":  # commit-time conflict regardless of ledger state
                import time as _t

                _t.sleep(0.2)
                raise conflict()
            if beh == "stale_first":  # fence already advanced; evaluated before the ledger check
                raise FakeSparkError("[USER_RAISED_EXCEPTION] DBXBENCH_STALE_SEQUENCE: x", None, "45000")
            if pid in self.ledger:  # the in-transaction idempotency check
                raise FakeSparkError(f"[USER_RAISED_EXCEPTION] DBXBENCH_DUPLICATE_PUBLICATION: {pid}", None, "45000")
            if beh.startswith("sleep:"):
                time.sleep(float(beh.split(":")[1]))
                beh = "commit"
                # OCC validation at commit time: both txns updated the same fence row, so the later one
                # conflicts (this is the property the real design relies on; tests/databricks verify it live)
                if pid in self.ledger:
                    raise conflict()
            if beh == "commit" or beh == "commit_then_lose_reply":
                if "DBXBENCH_INJECTED_FAULT" in s:
                    raise FakeSparkError("[USER_RAISED_EXCEPTION] DBXBENCH_INJECTED_FAULT: x", None, "45000")
                self.ledger.setdefault(pid, []).append(token)
                if beh == "commit_then_lose_reply":
                    raise FakeSparkError("py4j Answer from Java side is empty")
                return []
            if beh == "conflict":
                raise conflict()
            if beh == "stale":
                raise FakeSparkError("[USER_RAISED_EXCEPTION] DBXBENCH_STALE_SEQUENCE: x", None, "45000")
            if beh == "not_ready":
                raise FakeSparkError("[USER_RAISED_EXCEPTION] DBXBENCH_SEQUENCE_NOT_READY: x", None, "45000")
            if beh == "lost_not_committed":
                raise FakeSparkError("py4j Answer from Java side is empty")
            if beh == "nonretryable":
                raise FakeSparkError(
                    "[TABLE_OR_VIEW_NOT_FOUND] nope SQLSTATE: 42P01", "TABLE_OR_VIEW_NOT_FOUND", "42P01"
                )
            if beh == "unclassified":
                raise RuntimeError("something odd")
            raise AssertionError(beh)
        return []
