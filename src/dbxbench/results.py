"""Results directory layout and immutable/append-only writers.

<results_root>/<run_id>/
    run_manifest.json          immutable (exclusive create), written by `finalize`
    plan.json                  experiment plan (cells, profiles, publications)
    capabilities.json          doctor support matrix used for gating
    environment.json
    local_tables.json          (local mode only) identifier -> path registry
    generation.jsonl           staging generation timings and row/byte counts
    cells/<cell>/status.json   PLANNED | SKIPPED | RUNNING | COMPLETE | FAILED
    cells/<cell>/publications.jsonl, attempts.jsonl, table_metrics.jsonl, queries.jsonl,
                 validation.json, plans/<writer>.json, telemetry/<writer>.jsonl, barrier/, sql/
    summary.json, report.md, charts/*.png
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .sqlutil import iso, utc_now
from .storage import append_text, write_text

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class RunPaths:
    root: Path

    @staticmethod
    def for_run(results_root: str | Path, run_id: str) -> RunPaths:
        return RunPaths(Path(results_root) / run_id)

    @property
    def manifest(self) -> Path:
        return self.root / "run_manifest.json"

    @property
    def plan(self) -> Path:
        return self.root / "plan.json"

    @property
    def capabilities(self) -> Path:
        return self.root / "capabilities.json"

    @property
    def environment(self) -> Path:
        return self.root / "environment.json"

    @property
    def local_tables(self) -> Path:
        return self.root / "local_tables.json"

    @property
    def generation(self) -> Path:
        return self.root / "generation.jsonl"

    @property
    def summary(self) -> Path:
        return self.root / "summary.json"

    @property
    def report(self) -> Path:
        return self.root / "report.md"

    @property
    def charts(self) -> Path:
        return self.root / "charts"

    @property
    def init_sql(self) -> Path:
        return self.root / "sql" / "init.sql"

    def cell(self, cell_key: str) -> CellPaths:
        return CellPaths(self.root / "cells" / cell_key)


@dataclass(frozen=True)
class CellPaths:
    root: Path

    @property
    def status(self) -> Path:
        return self.root / "status.json"

    @property
    def publications(self) -> Path:
        return self.root / "publications.jsonl"

    @property
    def attempts(self) -> Path:
        return self.root / "attempts.jsonl"

    @property
    def table_metrics(self) -> Path:
        return self.root / "table_metrics.jsonl"

    @property
    def queries(self) -> Path:
        return self.root / "queries.jsonl"

    @property
    def validation(self) -> Path:
        return self.root / "validation.json"

    @property
    def plans(self) -> Path:
        return self.root / "plans"

    @property
    def telemetry(self) -> Path:
        return self.root / "telemetry"

    @property
    def barrier(self) -> Path:
        return self.root / "barrier"

    @property
    def sql(self) -> Path:
        return self.root / "sql"

    @property
    def launch(self) -> Path:
        return self.root / "launch.json"


def write_json(path: Path, data: Any, *, exclusive: bool = False) -> None:
    text = json.dumps(data, indent=2, sort_keys=True, default=str)
    if not write_text(path, text, exclusive=exclusive):
        raise FileExistsError(f"{path} already exists (immutable)")


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    append_text(path, json.dumps(record, sort_keys=True, default=str) + "\n", fsync=False)


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def set_status(cp: CellPaths, status: str, **fields: Any) -> None:
    prev = read_json(cp.status) if cp.status.exists() else {}
    history = prev.get("history", [])
    history.append({"status": status, "at": iso(utc_now())})
    write_json(cp.status, {**prev, **fields, "status": status, "history": history, "schema_version": SCHEMA_VERSION})


def get_status(cp: CellPaths) -> str | None:
    if not cp.status.exists():
        return None
    return str(read_json(cp.status).get("status"))
