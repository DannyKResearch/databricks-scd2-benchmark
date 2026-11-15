"""Offline analysis of persisted results (no Spark needed). Only empirical data is summarised; a
metric that was not observed is reported as null with a reason, never estimated silently."""

from __future__ import annotations

import csv
import json
from collections import Counter
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from .results import RunPaths, read_json, write_json
from .sqlutil import parse_utc

COMMITTED = {"COMMITTED", "COMMITTED_RECONCILED"}


# ----------------------------------------------------------------------------- statistics
def percentile(values: Sequence[float | None], q: float) -> float | None:
    """Linear-interpolated percentile (same definition as numpy's default)."""
    xs = sorted(float(v) for v in values if v is not None)
    if not xs:
        return None
    if len(xs) == 1:
        return xs[0]
    pos = (len(xs) - 1) * q / 100.0
    lo = int(pos)
    hi = min(lo + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (pos - lo)


def latency_summary(values: Sequence[float | None]) -> dict[str, Any]:
    xs = [float(v) for v in values if v is not None]
    return {
        "n": len(xs),
        "mean": sum(xs) / len(xs) if xs else None,
        "p50": percentile(xs, 50),
        "p95": percentile(xs, 95),
        "p99": percentile(xs, 99),
        "max": max(xs) if xs else None,
    }


def kendall_tau_b(x: Sequence[float], y: Sequence[float]) -> float | None:
    """Kendall tau-b between two paired rankings (handles ties). None if undefined."""
    n = len(x)
    if n != len(y) or n < 2:
        return None
    concordant = discordant = ties_x = ties_y = 0
    for i in range(n):
        for j in range(i + 1, n):
            dx = (x[i] > x[j]) - (x[i] < x[j])
            dy = (y[i] > y[j]) - (y[i] < y[j])
            if dx == 0 and dy == 0:
                continue
            if dx == 0:
                ties_x += 1
            elif dy == 0:
                ties_y += 1
            elif dx == dy:
                concordant += 1
            else:
                discordant += 1
    denom = ((concordant + discordant + ties_x) * (concordant + discordant + ties_y)) ** 0.5
    return (concordant - discordant) / denom if denom else None


def inversions(x: Sequence[float], y: Sequence[float]) -> int:
    """Pairs ordered strictly one way by x and strictly the other way by y."""
    n = 0
    for i in range(len(x)):
        for j in range(i + 1, len(x)):
            if (x[i] - x[j]) * (y[i] - y[j]) < 0:
                n += 1
    return n


def max_overlap(intervals: Sequence[tuple[float, float]]) -> int:
    events: list[tuple[float, int]] = []
    for a, b in intervals:
        events += [(a, 1), (b, -1)]
    cur = best = 0
    for _, d in sorted(events, key=lambda e: (e[0], e[1])):
        cur += d
        best = max(best, cur)
    return best


def _ts(s: str | None) -> float | None:
    if not s:
        return None
    try:
        return parse_utc(s).timestamp()
    except ValueError:
        try:
            return datetime.fromisoformat(s).timestamp()
        except ValueError:
            return None


def _jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


# ----------------------------------------------------------------------------- per-cell
def analyse_cell(cell_dir: Path, cell: dict[str, Any]) -> dict[str, Any]:
    status = read_json(cell_dir / "status.json") if (cell_dir / "status.json").exists() else {}
    pubs = [p for p in _jsonl(cell_dir / "publications.jsonl") if p.get("phase") == "measured"]
    measured = [p for p in pubs if not p.get("warmup")]
    attempts = [a for a in _jsonl(cell_dir / "attempts.jsonl") if a.get("phase") == "measured"]
    validation = read_json(cell_dir / "validation.json") if (cell_dir / "validation.json").exists() else None
    statuses = Counter(p.get("status") for p in measured)
    e2e = [p.get("end_to_end_s") for p in measured if p.get("status") in COMMITTED]
    att_dur = [a.get("duration_s") for a in attempts]
    ok_attempt_dur = [a.get("duration_s") for a in attempts if a.get("outcome") == "COMMIT_ACK"]
    cats = Counter(a.get("category") or "OK" for a in attempts)
    per_pub_attempts = Counter(p.get("attempt_count") or p.get("attempts") for p in measured)
    out: dict[str, Any] = {
        "cell_key": cell["cell_key"],
        "kind": cell["kind"],
        "strategy": cell["strategy"],
        "format": cell["format"],
        "ordering": cell["ordering"],
        "profile_key": cell["profile_key"],
        "repetition": cell.get("repetition"),
        "sweep_label": cell.get("sweep_label"),
        "contention": cell.get("contention"),
        "start_mode": cell.get("start_mode"),
        "fault": cell.get("fault"),
        "writers": len(cell.get("writers", [])),
        "table_count": cell["table_count"],
        "status": status.get("status"),
        "skip_reason": status.get("reason"),
        "verdict": status.get("verdict"),
        "validation_verdict": validation.get("verdict") if validation else None,
        "atomicity_evidence": validation.get("atomicity_evidence") if validation else None,
        "publications": len(measured),
        "publication_statuses": dict(statuses),
        "end_to_end_latency_s": latency_summary(e2e),
        "attempt_duration_s": latency_summary(att_dur),
        "committed_attempt_duration_s": latency_summary(ok_attempt_dur),
        "attempt_categories": dict(cats),
        "attempts_total": len(attempts),
        "attempts_per_publication": {str(k): v for k, v in per_pub_attempts.items()},
        "attempt_success_rate": (cats.get("OK", 0) / len(attempts)) if attempts else None,
        "eventual_success_rate": (sum(statuses.get(s, 0) for s in COMMITTED) / len(measured)) if measured else None,
        "prevalidation_s": latency_summary([p.get("prevalidation_s") for p in measured]),
    }
    if cell["kind"] == "contention":
        out["ordering_analysis"] = ordering_analysis(cell_dir, cell)
    out["storage"] = storage_summary(cell_dir)
    out["queries"] = query_summary(cell_dir)
    return out


def ordering_analysis(cell_dir: Path, cell: dict[str, Any]) -> dict[str, Any]:
    events = []
    tdir = cell_dir / "telemetry"
    if tdir.exists():
        for p in sorted(tdir.glob("*.jsonl")):
            events += _jsonl(p)
    launch = read_json(cell_dir / "launch.json") if (cell_dir / "launch.json").exists() else {}
    commit_order = read_json(cell_dir / "commit_order.json") if (cell_dir / "commit_order.json").exists() else {}
    writers: dict[str, dict[str, Any]] = {}
    for w in cell.get("writers", []):
        writers[w["writer_id"]] = {
            "writer_id": w["writer_id"],
            "sequence": min(p["sequence"] for p in w["publications"]),
            "publications": [p["publication_id"] for p in w["publications"]],
        }
    for h in launch.get("handles", []):
        if h["writer_id"] in writers:
            writers[h["writer_id"]]["submit"] = _ts(h.get("submit_start"))
            writers[h["writer_id"]]["external_id"] = h.get("external_id")
            writers[h["writer_id"]]["final_state"] = h.get("final_state")
    intervals: list[tuple[float, float]] = []
    distinct_proc: set[str] = set()
    clusters: set[str] = set()
    for e in events:
        wid = e.get("writer_id")
        if wid not in writers:
            continue
        w = writers[wid]
        ev = e.get("event")
        t = _ts(e.get("ts_utc"))
        if ev == "arrival":
            w["arrival"] = t
            distinct_proc.add(f"{e.get('host')}:{e.get('pid')}:{e.get('job_run_id')}")
        elif ev == "session_ready" and e.get("cluster_id"):
            clusters.add(str(e.get("cluster_id")))
        elif ev == "ready":
            w["ready"] = t
        elif ev == "start":
            w["start"] = t
        elif ev == "attempt_start" and "first_attempt" not in w:
            w["first_attempt"] = t
        elif ev == "attempt_end":
            a, b = _ts(e.get("attempt_start")), _ts(e.get("attempt_end"))
            if a is not None and b is not None:
                intervals.append((a, b))
        elif ev == "publication_end":
            w.setdefault("statuses", []).append(e.get("status"))
            if e.get("status") in COMMITTED:
                w["commit_ack"] = _ts(e.get("commit_ack_time")) or t
            w["attempts"] = w.get("attempts", 0) + int(e.get("attempts") or 0)
        elif ev in ("barrier_failed_closed", "worker_error"):
            w["harness_event"] = ev
    order_map = commit_order.get("order", {})
    for w in writers.values():
        versions = [order_map[p] for p in w["publications"] if p in order_map]
        w["commit_version"] = min(versions) if versions else None
    rows = list(writers.values())
    committed = [w for w in rows if w.get("commit_ack") is not None]

    def pair(a: str, b: str) -> dict[str, Any]:
        sel = [w for w in rows if w.get(a) is not None and w.get(b) is not None]
        xa, xb = [float(w[a]) for w in sel], [float(w[b]) for w in sel]
        return {
            "n": len(sel),
            "kendall_tau_b": kendall_tau_b(xa, xb),
            "inversions": inversions(xa, xb),
            "pairs": len(sel) * (len(sel) - 1) // 2,
        }

    starts = [w["first_attempt"] for w in rows if w.get("first_attempt")]
    acks = [w["commit_ack"] for w in committed]
    window = (max(acks) - min(starts)) if acks and starts else None
    submit = [w["submit"] for w in rows if w.get("submit")]
    return {
        "writers": rows,
        "commit_order_source": commit_order.get("source"),
        "orders_compared": {
            "submission_vs_commit_ack": pair("submit", "commit_ack"),
            "arrival_vs_commit_ack": pair("arrival", "commit_ack"),
            "ready_vs_commit_ack": pair("ready", "commit_ack"),
            "first_attempt_vs_commit_ack": pair("first_attempt", "commit_ack"),
            "sequence_vs_commit_ack": pair("sequence", "commit_ack"),
            "sequence_vs_commit_version": pair("sequence", "commit_version"),
            "first_attempt_vs_commit_version": pair("first_attempt", "commit_version"),
        },
        "fifo_holds_arrival_to_commit": pair("arrival", "commit_ack")["inversions"] == 0 if committed else None,
        "throughput_commits_per_s": (len(committed) / window) if window and window > 0 else None,
        "commit_window_s": window,
        "submission_spread_s": (max(submit) - min(submit)) if len(submit) > 1 else None,
        "max_concurrent_inflight_attempts": max_overlap(intervals) if intervals else 0,
        "distinct_writer_processes": len(distinct_proc),
        "distinct_cluster_ids": len(clusters),
        "starved_writers": [
            w["writer_id"]
            for w in rows
            if not w.get("statuses") or not any(s in COMMITTED or s == "ALREADY_COMMITTED" for s in w["statuses"])
        ],
        "max_attempts_by_writer": max((w.get("attempts", 0) for w in rows), default=0),
        "harness_events": {w["writer_id"]: w["harness_event"] for w in rows if w.get("harness_event")},
        "reader_probe": {k: v for k, v in launch.get("reader_probe", {}).items() if k != "query"} or None,
    }


def storage_summary(cell_dir: Path) -> dict[str, Any]:
    recs = _jsonl(cell_dir / "table_metrics.jsonl")
    after = [
        r
        for r in recs
        if r.get("phase") in ("after_publications", "after_contention")
        and not str(r.get("table", "")).endswith(("_ledger", "_fence"))
    ]
    maint = [r for r in recs if r.get("phase") == "maintenance"]
    size = [r["detail"].get("sizeInBytes") for r in after if r.get("detail", {}).get("observable")]
    files = [r["detail"].get("numFiles") for r in after if r.get("detail", {}).get("observable")]
    totals: Counter[str] = Counter()
    for r in after:
        h = r.get("history", {})
        if h.get("observable"):
            totals.update(h.get("totals", {}))
    maint_size = [r.get("detail_after", {}).get("sizeInBytes") for r in maint]
    return {
        "observable": bool(size),
        "tables": len(after),
        "size_bytes": sum(s for s in size if s is not None) if size else None,
        "num_files": sum(f for f in files if f is not None) if files else None,
        "history_totals": dict(totals),
        "maintenance_seconds": sum(r["maintenance"]["seconds"] for r in maint) if maint else None,
        "maintenance_ok": all(r["maintenance"]["ok"] for r in maint) if maint else None,
        "size_bytes_after_maintenance": sum(s for s in maint_size if s is not None) if maint_size else None,
    }


def query_summary(cell_dir: Path) -> dict[str, Any]:
    recs = _jsonl(cell_dir / "queries.jsonl")
    out: dict[str, Any] = {}
    for q in sorted({r["query"] for r in recs}):
        rs = [r for r in recs if r["query"] == q]
        first = [r["seconds"] for r in rs if r["run"] == "first_after_clear_cache"]
        rep = [r["seconds"] for r in rs if r["run"] == "repeat"]
        out[q] = {
            "first_s": first[0] if first else None,
            "repeat": latency_summary(rep),
            "result_hash": rs[0]["result_hash"],
            "hash_stable": len({r["result_hash"] for r in rs}) == 1,
        }
    return out


# ----------------------------------------------------------------------------- run-level
def analyse_run(paths: RunPaths) -> dict[str, Any]:
    plan = read_json(paths.plan)
    cells = [analyse_cell(paths.cell(c["cell_key"]).root, c) for c in plan["cells"]]
    verification = read_json(paths.root / "verification.json") if (paths.root / "verification.json").exists() else None
    caps = read_json(paths.capabilities) if paths.capabilities.exists() else None
    summary = {
        "run_id": plan["run_id"],
        "atomicity_evidence": next(
            (c["atomicity_evidence"] for c in cells if c["atomicity_evidence"] is not None), None
        ),
        "cells": cells,
        "matched_pairs": matched_pairs(cells),
        "query_equivalence": query_equivalence(cells),
        "verification": verification,
        "capabilities": {k: v["status"] for k, v in caps["capabilities"].items()} if caps else None,
        "correctness": correctness(cells, verification),
    }
    write_json(paths.summary, summary)
    export_csv(paths, cells)
    return summary


def matched_pairs(cells: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Delta vs Iceberg pairs with identical strategy / data profile / repetition / scenario."""
    out = []
    perf = [c for c in cells if c["kind"] == "performance"]

    def keyf(c: dict[str, Any]) -> tuple[Any, ...]:
        return (c["strategy"], c["profile_key"], c["repetition"], c["sweep_label"])

    by: dict[Any, dict[str, dict[str, Any]]] = {}
    for c in perf:
        by.setdefault(keyf(c), {})[c["format"]] = c
    for k, fm in sorted(by.items(), key=lambda kv: str(kv[0])):
        d, i = fm.get("delta"), fm.get("iceberg")
        rec: dict[str, Any] = {"strategy": k[0], "profile": k[1], "repetition": k[2], "sweep": k[3]}
        if not d or not i:
            rec["status"] = "INCOMPLETE_PAIR"
        elif d["status"] != "COMPLETE" or i["status"] != "COMPLETE":
            rec["status"] = "SKIPPED"
            rec["reason"] = {
                "delta": d.get("skip_reason") or d["status"],
                "iceberg": i.get("skip_reason") or i["status"],
            }
        else:
            rec["status"] = "COMPARED"
            rec["delta_p50_s"] = d["end_to_end_latency_s"]["p50"]
            rec["iceberg_p50_s"] = i["end_to_end_latency_s"]["p50"]
            rec["delta_size_bytes"] = d["storage"]["size_bytes"]
            rec["iceberg_size_bytes"] = i["storage"]["size_bytes"]
        out.append(rec)
    return out


def query_equivalence(cells: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    groups: dict[Any, list[dict[str, Any]]] = {}
    for c in cells:
        if c["kind"] == "performance" and c["queries"] and c["strategy"] != "scd2_changed_only":
            groups.setdefault((c["profile_key"], c["repetition"], c["sweep_label"]), []).append(c)
    for k, cs in groups.items():
        for q in sorted({q for c in cs for q in c["queries"]}):
            hashes = {f"{c['strategy']}/{c['format']}": c["queries"].get(q, {}).get("result_hash") for c in cs}
            out.append(
                {
                    "profile": k[0],
                    "repetition": k[1],
                    "query": q,
                    "hashes": hashes,
                    "equivalent": len({h for h in hashes.values() if h}) == 1,
                }
            )
    return out


def correctness(cells: list[dict[str, Any]], verification: dict[str, Any] | None) -> dict[str, Any]:
    ran = [c for c in cells if c["status"] == "COMPLETE"]
    skipped = [c for c in cells if c["status"] == "SKIPPED"]
    failed = [c for c in cells if c["status"] == "FAILED" or str(c.get("verdict") or "").startswith("FAIL")]
    flagged = [c for c in ran if c.get("validation_verdict") == "PASS_WITH_FLAGGED_ORDERING_ANOMALIES"]
    parity_fail = [p for p in (verification or {}).get("parity", []) if p["status"] != "PASS"]
    return {
        "cells_total": len(cells),
        "cells_ran": len(ran),
        "cells_skipped": len(skipped),
        "cells_failed": [c["cell_key"] for c in failed],
        "cells_with_flagged_ordering_anomalies": [c["cell_key"] for c in flagged],
        "parity_failures": len(parity_fail),
        "verdict": "FAIL" if failed or parity_fail else ("NOT_RUN" if not ran else "PASS"),
    }


def export_csv(paths: RunPaths, cells: list[dict[str, Any]]) -> None:
    exp = paths.root / "export"
    exp.mkdir(parents=True, exist_ok=True)
    flat_cols = [
        "cell_key",
        "kind",
        "strategy",
        "format",
        "ordering",
        "contention",
        "start_mode",
        "fault",
        "writers",
        "table_count",
        "profile_key",
        "repetition",
        "sweep_label",
        "status",
        "verdict",
        "validation_verdict",
        "publications",
        "attempts_total",
        "attempt_success_rate",
        "eventual_success_rate",
    ]
    with (exp / "cells.csv").open("w", newline="", encoding="utf-8") as f:
        wr = csv.writer(f)
        wr.writerow([*flat_cols, "e2e_p50_s", "e2e_p95_s", "e2e_p99_s", "size_bytes", "num_files"])
        for c in cells:
            e = c["end_to_end_latency_s"]
            wr.writerow(
                [c.get(k) for k in flat_cols]
                + [e["p50"], e["p95"], e["p99"], c["storage"]["size_bytes"], c["storage"]["num_files"]]
            )
    for name in ("publications", "attempts", "queries", "table_metrics"):
        rows: list[dict[str, Any]] = []
        for c in cells:
            rows += [{**r, "cell_key": c["cell_key"]} for r in _jsonl(paths.cell(c["cell_key"]).root / f"{name}.jsonl")]
        with (exp / f"{name}.jsonl").open("w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, default=str, sort_keys=True) + "\n")
