"""Markdown report + matplotlib charts, generated ONLY from persisted measurements.

If a section has no measured data it says so (NOT_RUN / SKIPPED with reason); no chart is drawn
from absent data and nothing is extrapolated.
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

from .results import RunPaths, read_json

# Validated reference categorical palette (dataviz skill references/palette.md), fixed order, light mode.
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
SURFACE = "#fcfcfb"
TEXT = "#0b0b0b"
TEXT2 = "#52514e"
GRID = "#e4e3df"


def _fmt(v: Any, digits: int = 3) -> str:
    if v is None:
        return "—"
    if isinstance(v, float):
        return f"{v:.{digits}f}"
    return str(v)


def _style(ax: Any, title: str, ylabel: str) -> None:
    ax.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", color=TEXT, fontsize=11)
    ax.set_ylabel(ylabel, color=TEXT2, fontsize=9)
    ax.tick_params(colors=TEXT2, labelsize=8)
    ax.grid(axis="y", color=GRID, linewidth=0.6)
    ax.set_axisbelow(True)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)


def _grouped_bars(
    path: Path, title: str, ylabel: str, groups: list[str], series: dict[str, list[float | None]]
) -> bool:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    if not groups or not series or all(v is None for vs in series.values() for v in vs):
        return False
    fig, ax = plt.subplots(figsize=(7.5, 3.6), dpi=140)
    fig.patch.set_facecolor(SURFACE)
    n = len(series)
    width = 0.8 / n
    for i, (name, vals) in enumerate(series.items()):
        xs = [g + (i - (n - 1) / 2) * width for g in range(len(groups))]
        ys = [v if v is not None else 0 for v in vals]
        ax.bar(xs, ys, width=width * 0.92, color=SERIES[i % len(SERIES)], label=name, edgecolor=SURFACE, linewidth=1)
    ax.set_xticks(range(len(groups)))
    ax.set_xticklabels(groups, rotation=0)
    _style(ax, title, ylabel)
    if n >= 2:
        ax.legend(frameon=False, fontsize=8, labelcolor=TEXT2)
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)
    return True


def _lines(path: Path, title: str, xlabel: str, ylabel: str, series: dict[str, list[tuple[float, float]]]) -> bool:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    series = {k: sorted(v) for k, v in series.items() if v}
    if not series:
        return False
    fig, ax = plt.subplots(figsize=(7.5, 3.6), dpi=140)
    fig.patch.set_facecolor(SURFACE)
    for i, (name, pts) in enumerate(sorted(series.items())):
        ax.plot(
            [p[0] for p in pts],
            [p[1] for p in pts],
            color=SERIES[i % len(SERIES)],
            linewidth=2,
            marker="o",
            markersize=5,
            label=name,
        )
    _style(ax, title, ylabel)
    ax.set_xlabel(xlabel, color=TEXT2, fontsize=9)
    ax.set_xticks(sorted({p[0] for pts in series.values() for p in pts}))  # writer counts are integers
    if len(series) >= 2:
        ax.legend(frameon=False, fontsize=8, labelcolor=TEXT2)
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)
    return True


def build_report(paths: RunPaths, charts: bool = True) -> Path:
    s = read_json(paths.summary)
    manifest = read_json(paths.manifest) if paths.manifest.exists() else {}
    cells: list[dict[str, Any]] = s["cells"]
    L: list[str] = []
    ev = s.get("atomicity_evidence")
    L.append(f"# dbxbench report — run `{s['run_id']}`\n")
    if ev is False:
        L.append(
            "> **NOT ATOMICITY EVIDENCE.** This run used `local_nonatomic_emulation` (OSS Spark, no "
            "`BEGIN ATOMIC`). It validates SQL shape, SCD2 semantics, validators and the harness only. "
            "Timings are from a local machine and say nothing about Databricks performance.\n"
        )
    c = s["correctness"]
    L.append("## Correctness verdict\n")
    L.append(
        f"* Overall: **{c['verdict']}** — {c['cells_ran']} cells ran, {c['cells_skipped']} skipped, "
        f"failed: {c['cells_failed'] or 'none'}"
    )
    L.append(
        f"* Cells with FLAGGED ordering anomalies (expected only in ordering mode `off`): "
        f"{c['cells_with_flagged_ordering_anomalies'] or 'none'}"
    )
    L.append(f"* Cross-cell content parity failures: {c['parity_failures']}")
    if manifest:
        L.append(
            f"* git `{manifest.get('git_sha')}`, config hash `{str(manifest.get('config_hash'))[:12]}`, "
            f"runtime `{manifest.get('runtime')}`"
        )
    L.append("")
    # ---- capabilities
    caps = s.get("capabilities") or {}
    if caps:
        L.append("## Capability matrix (functional probes)\n")
        L.append("| capability | status |\n|---|---|")
        for k, v in sorted(caps.items()):
            L.append(f"| {k} | {v} |")
        L.append("")
    # ---- skipped
    skipped = [x for x in cells if x["status"] == "SKIPPED"]
    if skipped:
        L.append("## SKIPPED cells (never counted as PASS)\n")
        L.append("| cell | kind | strategy | format | reason |\n|---|---|---|---|---|")
        for x in skipped:
            L.append(f"| {x['cell_key']} | {x['kind']} | {x['strategy']} | {x['format']} | {x['skip_reason']} |")
        L.append("")
    # ---- performance
    perf = [x for x in cells if x["kind"] == "performance" and x["status"] == "COMPLETE"]
    L.append("## Publication latency: SCD2 full vs append-only (single writer)\n")
    if not perf:
        L.append("NOT_RUN: no completed performance cells.\n")
    else:
        L.append(
            "End-to-end = first attempt start → acknowledged commit (prevalidation and staging excluded; "
            "warm-up publications excluded).\n"
        )
        L.append(
            "| cell | strategy | format | rep | sweep | n | p50 s | p95 s | p99 s | attempts | size bytes | files | "
            "OPTIMIZE s | verdict |"
        )
        L.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
        for x in sorted(perf, key=lambda r: (r["sweep_label"], r["strategy"], r["format"], r["repetition"])):
            e = x["end_to_end_latency_s"]
            st = x["storage"]
            L.append(
                f"| {x['cell_key']} | {x['strategy']} | {x['format']} | {x['repetition']} | {x['sweep_label']} | "
                f"{e['n']} | {_fmt(e['p50'])} | {_fmt(e['p95'])} | {_fmt(e['p99'])} | {x['attempts_total']} | "
                f"{_fmt(st['size_bytes'])} | {_fmt(st['num_files'])} | {_fmt(st['maintenance_seconds'])} | "
                f"{x['verdict']} |"
            )
        L.append("")
        if charts:
            agg: dict[tuple[str, str], list[float]] = defaultdict(list)
            for x in perf:
                if x["sweep_label"] == "baseline" and x["end_to_end_latency_s"]["p50"] is not None:
                    agg[(x["strategy"], x["format"])].append(x["end_to_end_latency_s"]["p50"])
            fmts = sorted({k[1] for k in agg})
            strats = sorted({k[0] for k in agg})
            ser = {
                st: [(sum(agg[(st, f)]) / len(agg[(st, f)])) if agg.get((st, f)) else None for f in fmts]
                for st in strats
            }
            if _grouped_bars(
                paths.charts / "publication_p50.png",
                "Publication latency p50 (mean over repetitions)",
                "seconds",
                fmts,
                ser,
            ):
                L.append("![publication p50](charts/publication_p50.png)\n")
            sz: dict[tuple[str, str], list[float]] = defaultdict(list)
            for x in perf:
                if x["sweep_label"] == "baseline" and x["storage"]["size_bytes"]:
                    sz[(x["strategy"], x["format"])].append(x["storage"]["size_bytes"] / 1e6)
            ser2 = {
                st: [(sum(sz[(st, f)]) / len(sz[(st, f)])) if sz.get((st, f)) else None for f in fmts] for st in strats
            }
            if _grouped_bars(
                paths.charts / "storage_mb.png",
                "Target table size after publications (DESCRIBE DETAIL)",
                "MB",
                fmts,
                ser2,
            ):
                L.append("![storage](charts/storage_mb.png)\n")
    # ---- queries
    qcells = [x for x in perf if x["queries"]]
    L.append("## Read queries\n")
    if not qcells:
        L.append("NOT_RUN: no query measurements.\n")
    else:
        L.append(
            "Append-only answers select publications via the ledger and derive end dates with LEAD(); that cost "
            "is included. `first` = first execution after CLEAR CACHE (not a guaranteed cold read: OS/disk "
            "caches are not cleared).\n"
        )
        L.append(
            "| cell | strategy | format | query | first s | repeat p50 s | result hash |\n|---|---|---|---|---|---|---|"
        )
        for x in qcells:
            for q, v in x["queries"].items():
                L.append(
                    f"| {x['cell_key']} | {x['strategy']} | {x['format']} | {q} | {_fmt(v['first_s'])} | "
                    f"{_fmt(v['repeat']['p50'])} | `{v['result_hash']}` |"
                )
        L.append("")
        eq = s.get("query_equivalence", [])
        bad = [e for e in eq if not e["equivalent"]]
        L.append(
            f"Answer equivalence across strategies/formats: {len(eq) - len(bad)}/{len(eq)} query groups "
            f"identical.{' Mismatches: ' + str(bad) if bad else ''}\n"
        )
        if charts:
            by_q: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
            for x in qcells:
                for q, v in x["queries"].items():
                    if v["repeat"]["p50"] is not None:
                        by_q[q][f"{x['strategy']}/{x['format']}"].append(v["repeat"]["p50"])
            qs = sorted(by_q)
            names = sorted({n for d in by_q.values() for n in d})[:4]
            ser3 = {n: [(sum(by_q[q][n]) / len(by_q[q][n])) if by_q[q].get(n) else None for q in qs] for n in names}
            if _grouped_bars(
                paths.charts / "query_p50.png",
                "Read query latency, repeat p50",
                "seconds",
                [q.replace("_", "\n") for q in qs],
                ser3,
            ):
                L.append("![queries](charts/query_p50.png)\n")
    # ---- contention
    cont = [x for x in cells if x["kind"] == "contention" and x["status"] == "COMPLETE"]
    L.append("## Concurrent independent writers\n")
    if not cont:
        L.append("NOT_RUN: no completed contention cells.\n")
    else:
        L.append(
            "| cell | writers | mode | start | ordering | committed | statuses | attempts | conflict categories | "
            "max in-flight | distinct procs | Kendall τ (arrival→commit) | inversions | τ (sequence→commit version) | "
            "throughput/s | reader skew (skewed/samples) | verdict |"
        )
        L.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
        for x in sorted(cont, key=lambda r: (r["contention"], r["ordering"], r["writers"])):
            o = x.get("ordering_analysis", {})
            ac = o.get("orders_compared", {}).get("arrival_vs_commit_ack", {})
            sv = o.get("orders_compared", {}).get("sequence_vs_commit_version", {})
            rp = o.get("reader_probe") or {}
            reader = f"{rp.get('skewed_samples')}/{rp.get('samples')}" if rp.get("applicable") else "n/a"
            committed = sum(
                v for k, v in x["publication_statuses"].items() if k in ("COMMITTED", "COMMITTED_RECONCILED")
            )
            L.append(
                f"| {x['cell_key']} | {x['writers']} | {x['contention']} | {x['start_mode']} | {x['ordering']} | "
                f"{committed} | {x['publication_statuses']} | {x['attempts_total']} | {x['attempt_categories']} | "
                f"{o.get('max_concurrent_inflight_attempts')} | {o.get('distinct_writer_processes')} | "
                f"{_fmt(ac.get('kendall_tau_b'))} | {ac.get('inversions')} | {_fmt(sv.get('kendall_tau_b'))} | "
                f"{_fmt(o.get('throughput_commits_per_s'))} | {reader} | "
                f"{x['verdict']} |"
            )
        L.append("")
        L.append(
            "FIFO is never assumed: arrival order and commit order are measured independently. A nonzero "
            "inversion count means a later arrival committed first. Reader skew counts single non-transactional "
            "queries that saw different latest sequences across the tables of one atomic publication set "
            "(a measurement, not an asserted guarantee).\n"
        )
        if charts:
            pts: dict[str, list[tuple[float, float]]] = defaultdict(list)
            for x in cont:
                p95 = x["end_to_end_latency_s"]["p95"]
                if p95 is not None:
                    pts[f"{x['contention']}/{x['ordering']}"].append((x["writers"], p95))
            writer_counts = {w for v in pts.values() for w, _ in v}
            if len(writer_counts) >= 2:
                drawn = _lines(
                    paths.charts / "contention_p95.png",
                    "Publication latency p95 vs writers",
                    "writers",
                    "seconds",
                    dict(list(sorted(pts.items()))[:4]),
                )
            else:  # one writer count: a line would be a single point on a meaningless axis
                modes = sorted(pts)
                drawn = _grouped_bars(
                    paths.charts / "contention_p95.png",
                    f"Publication latency p95 ({next(iter(writer_counts), '?')} writers)",
                    "seconds",
                    [m.replace("/", "\n") for m in modes],
                    {"p95": [max(y for _, y in pts[m]) for m in modes]},
                )
            if drawn:
                L.append("![contention p95](charts/contention_p95.png)\n")
    # ---- faults
    faults = [x for x in cells if x["kind"] == "fault"]
    L.append("## Failure injection\n")
    if not faults:
        L.append("NOT_RUN: no fault cells.\n")
    else:
        L.append("| cell | fault | format | status | verdict | validation |\n|---|---|---|---|---|---|")
        for x in faults:
            L.append(
                f"| {x['cell_key']} | {x['fault']} | {x['format']} | {x['status']} | {x['verdict']} | "
                f"{x['validation_verdict']} |"
            )
        L.append("")
    # ---- matched pairs
    L.append("## Delta vs Iceberg matched pairs\n")
    mp = s.get("matched_pairs", [])
    if not mp:
        L.append("NOT_RUN.\n")
    else:
        L.append(
            "| strategy | profile | rep | status | delta p50 s | iceberg p50 s | delta bytes | iceberg bytes | note |"
        )
        L.append("|---|---|---|---|---|---|---|---|---|")
        for p in mp:
            L.append(
                f"| {p['strategy']} | {p['profile']} | {p['repetition']} | {p['status']} | "
                f"{_fmt(p.get('delta_p50_s'))} | {_fmt(p.get('iceberg_p50_s'))} | {_fmt(p.get('delta_size_bytes'))}"
                f" | {_fmt(p.get('iceberg_size_bytes'))} | {p.get('reason', '')} |"
            )
        L.append("")
    L.append("## Limitations\n")
    L.append(
        "See `docs/limitations.md`. Key points: engine-reported `operationMetrics` are not ground-truth bytes "
        "rewritten; client timestamps across machines are subject to clock skew; small samples make p99 "
        "unreliable (n is shown); no universal winner is implied.\n"
    )
    paths.report.write_text("\n".join(L), encoding="utf-8")
    return paths.report
