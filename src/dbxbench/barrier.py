"""External, file-based start barrier (UC Volume path on Databricks, local directory locally).

* Writers announce ``ready/<writer>.json`` after all pre-work (session start, prevalidation).
* The coordinator waits (bounded) for all expected writers, then writes ``go.json`` containing a
  wall-clock start instant a few seconds in the future. If the timeout expires first it writes
  ``abort.json``: the barrier FAILS CLOSED - nobody publishes, nobody hangs.
* Writers poll for go/abort with their own bounded timeout, then sleep until ``go_at + offset``.

No Spark executors block, no driver memory is shared, and the barrier is entirely outside the
transactional write set.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .sqlutil import iso, parse_utc, utc_now
from .storage import write_text


class BarrierTimeout(Exception):
    pass


class BarrierAborted(Exception):
    pass


def _write_once(path: Path, data: dict[str, Any]) -> bool:
    # each path has a single writer by construction (ready/<writer>, go/abort by the coordinator)
    return write_text(path, json.dumps(data, sort_keys=True), exclusive=True)


def _read(path: Path) -> dict[str, Any] | None:
    try:
        text = path.read_text(encoding="utf-8")
        data: dict[str, Any] = json.loads(text)
        return data
    except (FileNotFoundError, json.JSONDecodeError):
        return None  # absent, or a partially written file: poll again


@dataclass
class FileBarrier:
    root: Path
    poll_seconds: float = 0.5

    @property
    def ready_dir(self) -> Path:
        return self.root / "ready"

    @property
    def go_path(self) -> Path:
        return self.root / "go.json"

    @property
    def abort_path(self) -> Path:
        return self.root / "abort.json"

    # ------------------------------------------------------------------ writer side
    def mark_ready(self, writer_id: str, **info: Any) -> None:
        _write_once(self.ready_dir / f"{writer_id}.json", {"writer_id": writer_id, "ready_at": iso(utc_now()), **info})

    def wait_go(self, timeout_s: float) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if self.abort_path.exists():
                raise BarrierAborted(str(_read(self.abort_path)))
            go = _read(self.go_path)
            if go is not None:
                return go
            time.sleep(self.poll_seconds)
        raise BarrierTimeout(f"no go/abort signal within {timeout_s}s")

    @staticmethod
    def sleep_until(at: datetime) -> float:
        delay = (at - utc_now()).total_seconds()
        if delay > 0:
            time.sleep(delay)
        return delay

    # ------------------------------------------------------------------ coordinator side
    def ready_writers(self) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        if self.ready_dir.exists():
            for p in self.ready_dir.glob("*.json"):
                d = _read(p)
                if d is not None:
                    out[p.stem] = d
        return out

    def coordinate(self, expected: set[str], timeout_s: float, lead_s: float) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            ready = self.ready_writers()
            if expected <= set(ready):
                go_at = utc_now() + timedelta(seconds=lead_s)
                payload = {"go_at": iso(go_at), "issued_at": iso(utc_now()), "participants": sorted(expected)}
                if not _write_once(self.go_path, payload):
                    existing = _read(self.go_path)
                    return existing or payload
                return payload
            if self.abort_path.exists():
                raise BarrierAborted("abort already signalled")
            time.sleep(self.poll_seconds)
        missing = sorted(expected - set(self.ready_writers()))
        _write_once(self.abort_path, {"aborted_at": iso(utc_now()), "missing": missing, "reason": "barrier timeout"})
        raise BarrierTimeout(f"writers not ready within {timeout_s}s: {missing}")

    @staticmethod
    def go_time(go: dict[str, Any], offset_ms: int) -> datetime:
        return parse_utc(go["go_at"]) + timedelta(milliseconds=offset_ms)
