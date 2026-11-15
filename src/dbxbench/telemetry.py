"""Append-only JSONL telemetry, one file per writer process.

Telemetry is deliberately written OUTSIDE the transactional write set (a plain file per writer on a
UC Volume or local disk), so it cannot add conflicts to the measured transactions and survives a
rollback. Each line is flushed and fsync'ed so that a killed writer leaves a readable prefix.
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from .sqlutil import iso, utc_now
from .storage import append_text


class TelemetrySink:
    def __init__(self, path: Path | None, **static: Any) -> None:
        self.path = path
        self.static = static
        self._lock = threading.Lock()
        self.events: list[dict[str, Any]] = []
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, event: str, **fields: Any) -> dict[str, Any]:
        rec = {"event": event, "ts_utc": iso(utc_now()), "mono_ns": time.monotonic_ns(), **self.static, **fields}
        with self._lock:
            self.events.append(rec)
            if self.path is not None:
                append_text(self.path, json.dumps(rec, default=str, sort_keys=True) + "\n")
        return rec


def read_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    """Read a JSONL file, tolerating a truncated final line from a killed writer."""
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def read_all(directory: Path, pattern: str = "*.jsonl") -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for p in sorted(directory.rglob(pattern)):
        out.extend(read_jsonl(p))
    return out
