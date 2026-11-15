"""Best-effort storage / I/O observability.

What is observable and how (documented in docs/methodology.md):
* ``DESCRIBE DETAIL``: numFiles, sizeInBytes, tableFeatures, properties (Delta; probed for Iceberg).
* ``DESCRIBE HISTORY``: per-commit ``operationMetrics`` (files added/removed, rows, DV counts, bytes
  when reported). These are engine-reported counters; they are NOT treated as ground-truth bytes
  rewritten. Object-store level byte counts are not collected (needs storage credentials).
* Commit order of publications: ledger row -> table version, via ``_metadata.row_commit_version``
  when row tracking is available, else by diffing ledger versions with time travel (outside txn).
"""

from __future__ import annotations

import json
import time
from typing import Any

from .executor import SqlExecutor
from .sqlutil import TableRef

NUMERIC_HISTORY_KEYS = (
    "numFiles",
    "numAddedFiles",
    "numRemovedFiles",
    "numOutputRows",
    "numOutputBytes",
    "numAddedBytes",
    "numRemovedBytes",
    "numUpdatedRows",
    "numCopiedRows",
    "numDeletionVectorsAdded",
    "numDeletionVectorsRemoved",
    "numDeletionVectorsUpdated",
    "numAddedChangeFiles",
    "executionTimeMs",
    "rewriteTimeMs",
    "scanTimeMs",
)


def table_detail(x: SqlExecutor, t: TableRef) -> dict[str, Any]:
    try:
        rows = x.execute(f"DESCRIBE DETAIL {t.sql}")
    except Exception as exc:
        return {"observable": False, "error": str(exc)[:300]}
    r = rows[0] if rows else {}
    return {
        "observable": True,
        "format": r.get("format"),
        "numFiles": r.get("numFiles"),
        "sizeInBytes": r.get("sizeInBytes"),
        "tableFeatures": r.get("tableFeatures"),
        "partitionColumns": r.get("partitionColumns"),
        "clusteringColumns": r.get("clusteringColumns"),
        "properties": r.get("properties"),
        "minReaderVersion": r.get("minReaderVersion"),
        "minWriterVersion": r.get("minWriterVersion"),
    }


def table_history(x: SqlExecutor, t: TableRef) -> dict[str, Any]:
    try:
        rows = x.execute(f"DESCRIBE HISTORY {t.sql}")
    except Exception as exc:
        return {"observable": False, "error": str(exc)[:300]}
    totals: dict[str, int] = {}
    ops: dict[str, int] = {}
    commits = []
    for r in rows:
        op = str(r.get("operation"))
        ops[op] = ops.get(op, 0) + 1
        m = r.get("operationMetrics") or {}
        for k in NUMERIC_HISTORY_KEYS:
            v = m.get(k)
            if v is not None:
                try:
                    totals[k] = totals.get(k, 0) + int(v)
                except (TypeError, ValueError):
                    continue
        commits.append(
            {
                "version": r.get("version"),
                "timestamp": str(r.get("timestamp")),
                "operation": op,
                "metrics": {k: m.get(k) for k in NUMERIC_HISTORY_KEYS if m.get(k) is not None},
            }
        )
    return {
        "observable": True,
        "commits": len(rows),
        "operations": ops,
        "totals": totals,
        "per_commit": sorted(commits, key=lambda c: int(c["version"] or 0)),
    }


def maintenance(x: SqlExecutor, t: TableRef) -> dict[str, Any]:
    """Time OPTIMIZE (compaction). VACUUM is not run: the default 7-day retention would make it a no-op
    and disabling the retention check is unsafe; this is recorded as a limitation."""
    t0 = time.perf_counter()
    try:
        rows = x.execute(f"OPTIMIZE {t.sql}")
        return {
            "operation": "OPTIMIZE",
            "seconds": time.perf_counter() - t0,
            "ok": True,
            "result": json.loads(json.dumps(rows[:1], default=str)),
        }
    except Exception as exc:
        return {"operation": "OPTIMIZE", "seconds": time.perf_counter() - t0, "ok": False, "error": str(exc)[:300]}


def ledger_commit_order(x: SqlExecutor, ledger: TableRef) -> dict[str, Any]:
    """Map publication_id -> ledger table version (true serialization order of commits)."""
    try:
        rows = x.execute(f"SELECT publication_id, _metadata.row_commit_version AS v FROM {ledger.sql}")
        if rows and all(r.get("v") is not None for r in rows):
            return {"source": "row_commit_version", "order": {r["publication_id"]: int(r["v"]) for r in rows}}
    except Exception:  # noqa: S110 - fall back below
        pass
    try:
        hist = x.execute(f"DESCRIBE HISTORY {ledger.sql}")
        versions = sorted(int(h["version"]) for h in hist)
        seen: set[str] = set()
        order: dict[str, int] = {}
        for v in versions:
            cur = {r["publication_id"] for r in x.execute(f"SELECT publication_id FROM {ledger.sql} VERSION AS OF {v}")}
            for p in cur - seen:
                order[p] = v
            seen |= cur
        return {"source": "time_travel_version_diff", "order": order}
    except Exception as exc:
        return {"source": "not_observable", "order": {}, "error": str(exc)[:300]}
