"""Expand a validated config into a deterministic experiment plan of *cells*.

A cell is one (strategy, format, scenario) combination with its own fresh table set. Cells never
share target tables, so results are independent and cleanup is per run. Cells using the same
data profile read the same immutable staged data (fairness across strategies/formats).
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Any

from .config import (
    BenchConfig,
    ContentionMode,
    FaultScenario,
    Launcher,
    OrderingMode,
    StartMode,
    Strategy,
    SweepMode,
    TableFormat,
)
from .model import DataProfile, Publication, publication_id, publication_ts, scope_name

STRATEGY_SHORT = {Strategy.SCD2_FULL: "scd2", Strategy.APPEND_ONLY_FULL: "app", Strategy.SCD2_CHANGED_ONLY: "chg"}
FORMAT_SHORT = {TableFormat.DELTA: "dlt", TableFormat.ICEBERG: "ice"}
ROW_OVERHEAD_BYTES = 160  # rough uncompressed per-row overhead for keys/metadata columns


class CellKind(StrEnum):
    PERFORMANCE = "performance"
    CONTENTION = "contention"
    FAULT = "fault"


@dataclass(frozen=True)
class WriterPlan:
    writer_id: str
    publications: tuple[Publication, ...]
    start_offset_ms: int = 0
    faults: tuple[str, ...] = ()


@dataclass(frozen=True)
class Cell:
    cell_key: str
    kind: CellKind
    strategy: Strategy
    fmt: TableFormat
    ordering: OrderingMode
    profile_key: str
    table_count: int
    scopes: tuple[str, ...]
    launcher: Launcher
    setup_publications: tuple[Publication, ...]  # committed inline before the measured phase
    writers: tuple[WriterPlan, ...]
    repetition: int = 1
    contention: ContentionMode | None = None
    start_mode: StartMode | None = None
    fault: FaultScenario | None = None
    warmup_publications: int = 0
    sweep_label: str = "baseline"

    @property
    def writer_count(self) -> int:
        return len(self.writers)

    def to_json(self) -> dict[str, Any]:
        return {
            "cell_key": self.cell_key,
            "kind": self.kind.value,
            "strategy": self.strategy.value,
            "format": self.fmt.value,
            "ordering": self.ordering.value,
            "profile_key": self.profile_key,
            "table_count": self.table_count,
            "scopes": list(self.scopes),
            "launcher": self.launcher.value,
            "repetition": self.repetition,
            "contention": self.contention.value if self.contention else None,
            "start_mode": self.start_mode.value if self.start_mode else None,
            "fault": self.fault.value if self.fault else None,
            "warmup_publications": self.warmup_publications,
            "sweep_label": self.sweep_label,
            "setup_publications": [p.to_json() for p in self.setup_publications],
            "writers": [
                {
                    "writer_id": w.writer_id,
                    "start_offset_ms": w.start_offset_ms,
                    "faults": list(w.faults),
                    "publications": [p.to_json() for p in w.publications],
                }
                for w in self.writers
            ],
        }

    @staticmethod
    def from_json(d: dict[str, Any]) -> Cell:
        return Cell(
            cell_key=d["cell_key"],
            kind=CellKind(d["kind"]),
            strategy=Strategy(d["strategy"]),
            fmt=TableFormat(d["format"]),
            ordering=OrderingMode(d["ordering"]),
            profile_key=d["profile_key"],
            table_count=int(d["table_count"]),
            scopes=tuple(d["scopes"]),
            launcher=Launcher(d["launcher"]),
            repetition=int(d["repetition"]),
            contention=ContentionMode(d["contention"]) if d.get("contention") else None,
            start_mode=StartMode(d["start_mode"]) if d.get("start_mode") else None,
            fault=FaultScenario(d["fault"]) if d.get("fault") else None,
            warmup_publications=int(d.get("warmup_publications", 0)),
            sweep_label=d.get("sweep_label", "baseline"),
            setup_publications=tuple(Publication.from_json(p) for p in d["setup_publications"]),
            writers=tuple(
                WriterPlan(
                    writer_id=w["writer_id"],
                    start_offset_ms=int(w["start_offset_ms"]),
                    faults=tuple(w.get("faults", [])),
                    publications=tuple(Publication.from_json(p) for p in w["publications"]),
                )
                for w in d["writers"]
            ),
        )


@dataclass
class Plan:
    run_id: str
    profiles: dict[str, DataProfile]
    cells: list[Cell]
    required_pairs: dict[str, set[tuple[str, int]]] = field(default_factory=dict)
    estimated_rows: int = 0
    estimated_bytes: int = 0

    def to_json(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "profiles": {k: v.to_json() for k, v in self.profiles.items()},
            "required_pairs": {k: sorted([list(p) for p in v]) for k, v in self.required_pairs.items()},
            "cells": [c.to_json() for c in self.cells],
            "estimated_rows": self.estimated_rows,
            "estimated_bytes": self.estimated_bytes,
        }

    @staticmethod
    def from_json(d: dict[str, Any]) -> Plan:
        return Plan(
            run_id=d["run_id"],
            profiles={k: DataProfile(**v) for k, v in d["profiles"].items()},
            cells=[Cell.from_json(c) for c in d["cells"]],
            required_pairs={k: {(s, int(q)) for s, q in v} for k, v in d["required_pairs"].items()},
            estimated_rows=int(d.get("estimated_rows", 0)),
            estimated_bytes=int(d.get("estimated_bytes", 0)),
        )

    def cell(self, key: str) -> Cell:
        for c in self.cells:
            if c.cell_key == key:
                return c
        raise KeyError(f"no cell {key!r} in plan")


class BudgetExceeded(ValueError):
    pass


def _base_profile(cfg: BenchConfig, key: str) -> DataProfile:
    w = cfg.workload
    return DataProfile(
        profile_key=key,
        seed=cfg.project.seed,
        table_count=w.table_count,
        rows_per_table_per_publication=w.rows_per_table_per_publication,
        publication_count=w.publication_count,
        dataset_scope_count=w.dataset_scope_count,
        payload_width_bytes=w.payload_width_bytes,
        changed_value_fraction=w.changed_value_fraction,
        category_skew=w.category_skew,
        category_cardinality=w.category_cardinality,
        scope_size_skew=w.scope_size_skew,
        null_fraction=w.null_fraction,
        publication_interval_millis=w.publication_interval_millis,
        publication_timestamp_origin_utc=w.publication_timestamp_origin_utc,
    )


def _sweep_profiles(cfg: BenchConfig) -> list[tuple[str, DataProfile]]:
    base = _base_profile(cfg, "d00")
    out: list[tuple[str, DataProfile]] = [("baseline", base)]
    s = cfg.sweeps
    dims: dict[str, list[Any]] = {
        "rows_per_table_per_publication": s.rows_per_table_per_publication,
        "table_count": s.table_count,
        "publication_count": s.publication_count,
        "changed_value_fraction": s.changed_value_fraction,
        "payload_width_bytes": s.payload_width_bytes,
        "dataset_scope_count": s.dataset_scope_count,
    }
    if s.mode is SweepMode.BASELINE_ONLY:
        return out
    variants: list[tuple[str, dict[str, Any]]] = []
    if s.mode is SweepMode.ONE_AT_A_TIME:
        for dim, values in dims.items():
            for v in values:
                if getattr(base, dim) != v:
                    variants.append((f"{dim}={v}", {dim: v}))
    else:
        combos: list[dict[str, Any]] = [{}]
        for dim, values in dims.items():
            if values:
                combos = [{**c, dim: v} for c in combos for v in values]
        variants = [(",".join(f"{k}={v}" for k, v in c.items()), c) for c in combos if c]
    for i, (label, changes) in enumerate(variants, start=1):
        out.append((label, replace(base, profile_key=f"d{i:02d}", **changes)))
    return out


def _pubs_for_scope(p: DataProfile, scope_idx: int, seqs: list[int], tables: tuple[int, ...]) -> list[Publication]:
    scope = scope_name(scope_idx)
    return [
        Publication(
            dataset_scope=scope,
            publication_id=publication_id(scope, q),
            sequence=q,
            publication_ts=publication_ts(p.publication_timestamp_origin_utc, p.publication_interval_millis, q),
            table_indexes=tables,
            expected_rows=p.rows_for_scope(scope_idx),
            profile_key=p.profile_key,
        )
        for q in seqs
    ]


def build_plan(cfg: BenchConfig, run_id: str, *, writers_launcher: Launcher | None = None) -> Plan:
    profiles: dict[str, DataProfile] = {}
    cells: list[Cell] = []
    counter = 0

    def key(strategy: Strategy, fmt: TableFormat) -> str:
        nonlocal counter
        counter += 1
        return f"c{counter:03d}_{STRATEGY_SHORT[strategy]}_{FORMAT_SHORT[fmt]}"

    # ---- performance matrix: single writer, sequential publications, strategy x format x sweep
    for label, prof in _sweep_profiles(cfg):
        profiles[prof.profile_key] = prof
        all_tables = tuple(range(prof.table_count))
        for rep in range(1, cfg.benchmark.repetitions + 1):
            for strategy in cfg.strategies:
                for fmt in cfg.formats:
                    pubs: list[Publication] = []
                    for q in range(1, prof.publication_count + 1):
                        for s in range(prof.dataset_scope_count):
                            pubs.extend(_pubs_for_scope(prof, s, [q], all_tables))
                    cells.append(
                        Cell(
                            cell_key=key(strategy, fmt),
                            kind=CellKind.PERFORMANCE,
                            strategy=strategy,
                            fmt=fmt,
                            ordering=cfg.benchmark.ordering_mode,
                            profile_key=prof.profile_key,
                            table_count=prof.table_count,
                            scopes=tuple(scope_name(s) for s in range(prof.dataset_scope_count)),
                            launcher=Launcher.INLINE,
                            setup_publications=(),
                            writers=(WriterPlan("w000", tuple(pubs)),),
                            repetition=rep,
                            warmup_publications=min(cfg.benchmark.warmup_publications, prof.publication_count - 1)
                            * prof.dataset_scope_count,
                            sweep_label=label,
                        )
                    )

    # ---- contention cells: N independent writers
    cc = cfg.concurrency
    if cc.enabled and cc.writer_counts:
        max_n = max(cc.writer_counts)
        base = _base_profile(cfg, "dc")
        ppw = cc.publications_per_writer
        cprof = replace(
            base,
            dataset_scope_count=max(base.dataset_scope_count, max_n),
            publication_count=1,
            extra_sequences=max_n * ppw + 1,
        )
        profiles["dc"] = cprof
        launcher = writers_launcher or cc.launcher
        for rep in range(1, cc.repetitions + 1):
            for strategy in cc.strategies:
                for fmt in cc.formats:
                    for n in cc.writer_counts:
                        for mode in cc.contention_modes:
                            for start in cc.start_modes:
                                for ordering in cc.strict_ordering_modes:
                                    cells.append(
                                        _contention_cell(
                                            key(strategy, fmt),
                                            cfg,
                                            cprof,
                                            strategy,
                                            fmt,
                                            n,
                                            mode,
                                            start,
                                            ordering,
                                            launcher,
                                            rep,
                                        )
                                    )

    # ---- fault cells: one writer, deterministic scenario
    fi = cfg.fault_injection
    if fi.enabled:
        fprof = profiles["d00"]
        for fmt in fi.formats:
            for fault in fi.scenarios:
                strategy = Strategy.SCD2_FULL
                cells.append(_fault_cell(key(strategy, fmt), cfg, fprof, strategy, fmt, fault))

    if cfg.benchmark.randomize_order:
        rng = random.Random(cfg.project.seed)
        perf = [c for c in cells if c.kind is CellKind.PERFORMANCE]
        rest = [c for c in cells if c.kind is not CellKind.PERFORMANCE]
        rng.shuffle(perf)
        rng.shuffle(rest)
        cells = perf + rest

    plan = Plan(run_id=run_id, profiles=profiles, cells=cells)
    plan.required_pairs = required_pairs(plan)
    plan.estimated_rows, plan.estimated_bytes = estimate(plan)
    enforce_budget(cfg, plan)
    return plan


def _contention_cell(
    cell_key: str,
    cfg: BenchConfig,
    prof: DataProfile,
    strategy: Strategy,
    fmt: TableFormat,
    n: int,
    mode: ContentionMode,
    start: StartMode,
    ordering: OrderingMode,
    launcher: Launcher,
    rep: int,
) -> Cell:
    cc = cfg.concurrency
    ppw = cc.publications_per_writer
    all_tables = tuple(range(prof.table_count))
    rng = random.Random(f"{cfg.project.seed}:{cell_key}")
    writers: list[WriterPlan] = []
    setup: list[Publication] = []
    if mode is ContentionMode.DISJOINT_SCOPES:
        scopes = tuple(range(n))
        for s in scopes:
            setup.extend(_pubs_for_scope(prof, s, [1], all_tables))
    else:
        scopes = (0,)
        setup.extend(_pubs_for_scope(prof, 0, [1], all_tables))
    window = min(cc.overlapping_table_window, prof.table_count)
    for i in range(n):
        if mode is ContentionMode.DISJOINT_SCOPES:
            pubs = _pubs_for_scope(prof, i, [2 + j for j in range(ppw)], all_tables)
        else:
            seqs = [2 + i + j * n for j in range(ppw)]
            tables = all_tables
            if mode is ContentionMode.OVERLAPPING_TABLES:
                tables = tuple(sorted({(i + k) % prof.table_count for k in range(window)}))
            pubs = _pubs_for_scope(prof, 0, seqs, tables)
        if start is StartMode.BARRIER:
            offset = 0
        elif start is StartMode.STAGGERED:
            offset = i * cc.stagger_millis
        else:
            offset = rng.randint(0, cc.random_start_max_millis)
        writers.append(WriterPlan(f"w{i:03d}", tuple(pubs), offset))
    return Cell(
        cell_key=cell_key,
        kind=CellKind.CONTENTION,
        strategy=strategy,
        fmt=fmt,
        ordering=ordering,
        profile_key=prof.profile_key,
        table_count=prof.table_count,
        scopes=tuple(scope_name(s) for s in scopes),
        launcher=launcher if n > 1 or launcher is not Launcher.INLINE else Launcher.INLINE,
        setup_publications=tuple(setup),
        writers=tuple(writers),
        repetition=rep,
        contention=mode,
        start_mode=start,
    )


def _fault_cell(
    cell_key: str, cfg: BenchConfig, prof: DataProfile, strategy: Strategy, fmt: TableFormat, fault: FaultScenario
) -> Cell:
    all_tables = tuple(range(prof.table_count))
    setup = _pubs_for_scope(prof, 0, [1], all_tables)
    ordering = OrderingMode.SEQUENCED
    if fault is FaultScenario.DUPLICATE_DELIVERY:
        # the same publication delivered twice (sequentially): second must be an idempotent no-op
        p2 = _pubs_for_scope(prof, 0, [2], all_tables)
        writers = (WriterPlan("w000", (p2[0], p2[0]), faults=(fault.value,)),)
    elif fault is FaultScenario.LATE_DELIVERY:
        # seq 3 commits, then late seq 2 arrives: must be rejected and must not close seq 3 rows
        p = _pubs_for_scope(prof, 0, [2, 3], all_tables)
        writers = (WriterPlan("w000", (p[1], p[0]), faults=(fault.value,)),)
        ordering = OrderingMode.MONOTONIC
    else:
        p2 = _pubs_for_scope(prof, 0, [2], all_tables)
        # in-txn faults: faulty attempt, then the ENTIRE batch is re-submitted cleanly
        writers = (WriterPlan("w000", (p2[0], p2[0]), faults=(fault.value,)),)
    return Cell(
        cell_key=cell_key,
        kind=CellKind.FAULT,
        strategy=strategy,
        fmt=fmt,
        ordering=ordering,
        profile_key=prof.profile_key,
        table_count=prof.table_count,
        scopes=(scope_name(0),),
        launcher=Launcher.INLINE,
        setup_publications=tuple(setup),
        writers=writers,
        fault=fault,
    )


def required_pairs(plan: Plan) -> dict[str, set[tuple[str, int]]]:
    out: dict[str, set[tuple[str, int]]] = {k: set() for k in plan.profiles}
    for c in plan.cells:
        for p in list(c.setup_publications) + [p for w in c.writers for p in w.publications]:
            out[c.profile_key].add((p.dataset_scope, p.sequence))
    return {k: v for k, v in out.items() if v}


def estimate(plan: Plan) -> tuple[int, int]:
    rows = 0
    bytes_ = 0
    for pk, pairs in plan.required_pairs.items():
        prof = plan.profiles[pk]
        per = sum(prof.rows_for_scope(int(s.split("_")[1]) - 1) for s, _ in pairs) * prof.table_count
        rows += per
        bytes_ += per * (prof.payload_width_bytes + ROW_OVERHEAD_BYTES)
    for c in plan.cells:
        prof = plan.profiles[c.profile_key]
        pubs = list(c.setup_publications) + [p for w in c.writers for p in w.publications]
        r = sum(p.expected_rows * len(p.table_indexes) for p in pubs)
        rows += r
        bytes_ += r * (prof.payload_width_bytes + ROW_OVERHEAD_BYTES)
    return rows, bytes_


def enforce_budget(cfg: BenchConfig, plan: Plan) -> None:
    if plan.estimated_rows > cfg.project.max_estimated_total_rows:
        raise BudgetExceeded(
            f"plan writes ~{plan.estimated_rows:,} rows > project.max_estimated_total_rows="
            f"{cfg.project.max_estimated_total_rows:,}; reduce the workload or raise the guardrail explicitly"
        )
    gb = plan.estimated_bytes / 1e9
    if gb > cfg.project.max_estimated_storage_gb:
        raise BudgetExceeded(
            f"plan writes ~{gb:,.1f} GB (uncompressed estimate) > project.max_estimated_storage_gb="
            f"{cfg.project.max_estimated_storage_gb}"
        )
    for c in plan.cells:
        if c.kind is CellKind.CONTENTION and c.writer_count > cfg.compute.max_parallel_job_runs:
            raise BudgetExceeded(
                f"cell {c.cell_key} needs {c.writer_count} writers > compute.max_parallel_job_runs="
                f"{cfg.compute.max_parallel_job_runs}"
            )
