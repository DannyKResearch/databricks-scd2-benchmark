"""Orchestration of doctor / init / generate / run / verify for one run_id."""

from __future__ import annotations

import json
import logging
import subprocess
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .barrier import BarrierAborted, BarrierTimeout, FileBarrier
from .capabilities import CapabilityMatrix, CapStatus, Prober, collect_environment, gate, requirements_for
from .config import BenchConfig, ContentionMode, FaultScenario, Launcher, Strategy, TableFormat
from .datagen import generate_profile, snapshot_frame, with_duplicate_keys
from .engine import EngineFaults, PublicationEngine, PublicationResult, PublicationStatus
from .executor import RecordingExecutor, SparkSqlExecutor, SqlExecutor, SqlLog
from .launcher import DatabricksJobsLauncher, LaunchHandle, LocalProcessLauncher
from .metrics import ledger_commit_order, maintenance, table_detail, table_history
from .model import FENCE_COLUMNS, LEDGER_COLUMNS, publication_ts
from .naming import (
    OWNER_PROPERTY,
    OWNER_VALUE,
    RUN_PROPERTY,
    CellTables,
    Namespace,
    cell_tables,
    staging_table,
)
from .queries import QueryParams, run_queries
from .results import RunPaths, append_jsonl, get_status, read_json, set_status, write_json
from .retry import RetryPolicy, stable_seed
from .scenarios import Cell, CellKind, Plan, build_plan
from .spark_session import LocalCatalog, get_spark
from .sqldialect import atomicity_evidence, dialect_for
from .sqlgen import create_schema_sql, create_table_sql, fence_seed_sql, staging_columns, target_columns
from .sqlutil import TableRef, iso, quote_ident, sql_string, utc_now
from .telemetry import TelemetrySink, read_all
from .validation import CheckResult, CheckStatus, ValidationReport, Validator, parity_check

log = logging.getLogger(__name__)

COMMITTED_STATUSES = {
    PublicationStatus.COMMITTED.value,
    PublicationStatus.COMMITTED_RECONCILED.value,
    PublicationStatus.ALREADY_COMMITTED.value,
}


class RunError(RuntimeError):
    pass


def git_sha() -> str:
    import os

    if os.environ.get("DBXBENCH_GIT_SHA"):
        return os.environ["DBXBENCH_GIT_SHA"]
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=Path(__file__).parent,
            check=False,
        )
        sha = out.stdout.strip()
        dirty = subprocess.run(
            ["git", "status", "--porcelain"],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=Path(__file__).parent,
            check=False,
        ).stdout.strip()
        return (sha + ("-dirty" if dirty else "")) if sha else "unknown"
    except Exception:
        return "unknown"


@dataclass
class Runtime:
    cfg: BenchConfig
    run_id: str
    dry_run: bool = False
    spark: Any = None
    _executor: SqlExecutor | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        self.ns = Namespace.for_run(self.cfg, self.run_id)
        self.paths = RunPaths.for_run(self.cfg.project.results_root, self.run_id)
        self.dialect = dialect_for(self.cfg)
        self.local_catalog = LocalCatalog(self.paths.local_tables) if self.cfg.is_local else None
        self.ddl: list[str] = []

    @property
    def executor(self) -> SqlExecutor:
        if self._executor is None:
            if self.dry_run:
                self._executor = RecordingExecutor()
            else:
                if self.spark is None:
                    self.spark = get_spark(self.cfg, self.paths.root)
                    if self.local_catalog is not None:
                        self.local_catalog.register_all(self.spark)
                log_path = self.paths.root / "sql" / "executed.sql" if self.cfg.reporting.save_executed_sql else None
                self._executor = SparkSqlExecutor(self.spark, SqlLog(log_path))
        return self._executor

    @property
    def warehouse(self) -> Path:
        return (self.paths.root / "local_warehouse").resolve()

    # ------------------------------------------------------------------ helpers
    def ddl_exec(self, sql: str) -> None:
        self.ddl.append(sql)
        if not self.dry_run:
            self.executor.execute(sql)

    def create_table(
        self, ref: TableRef, columns: Any, fmt: TableFormat, props: dict[str, str], partitioned_by: tuple[str, ...] = ()
    ) -> None:
        location = None
        if self.local_catalog is not None:
            location = self.local_catalog.location_for(self.warehouse, ref)
        self.ddl_exec(create_table_sql(ref, columns, fmt, props, self.run_id, partitioned_by, location))
        if self.local_catalog is not None and location is not None and not self.dry_run:
            self.local_catalog.record(ref, location)

    def table_props(self, fmt: TableFormat, transactional: bool) -> dict[str, str]:
        if fmt is TableFormat.ICEBERG:
            return {}
        props = {"delta.enableDeletionVectors": "true"}
        if transactional and not self.cfg.is_local:
            # Required for multi-table transactions (docs: tables/features/catalog-commits).
            props["delta.feature.catalogManaged"] = "supported"
        return props

    def tables_for(self, cell: Cell) -> CellTables:
        return cell_tables(self.ns.for_cell(cell.cell_key), cell.cell_key, cell.table_count)

    def staging_for(self, cell: Cell) -> tuple[TableRef, ...]:
        return tuple(staging_table(self.ns, cell.profile_key, i) for i in range(cell.table_count))

    def load_plan(self) -> Plan:
        if not self.paths.plan.exists():
            raise RunError(f"no plan at {self.paths.plan}; run `dbxbench init` first")
        return Plan.from_json(read_json(self.paths.plan))

    def load_capabilities(self) -> CapabilityMatrix | None:
        if not self.paths.capabilities.exists():
            return None
        return CapabilityMatrix.from_json(read_json(self.paths.capabilities))

    def engine(
        self, cell: Cell, writer_id: str, telemetry: TelemetrySink, staging: tuple[TableRef, ...] | None = None
    ) -> PublicationEngine:
        cc, w = self.cfg.concurrency, self.cfg.workload
        sql_dir = self.paths.cell(cell.cell_key).sql

        def sink(name: str, sql: str) -> None:
            if self.cfg.reporting.save_executed_sql:
                sql_dir.mkdir(parents=True, exist_ok=True)
                (sql_dir / f"{writer_id}.{name}.sql").write_text(sql + "\n", encoding="utf-8")

        return PublicationEngine(
            executor=self.executor,
            dialect=self.dialect,
            run_id=self.run_id,
            cell_key=cell.cell_key,
            strategy=cell.strategy,
            ordering=cell.ordering,
            writer_id=writer_id,
            tables=self.tables_for(cell),
            staging=staging or self.staging_for(cell),
            policy=RetryPolicy(
                cc.max_attempts,
                cc.initial_backoff_ms,
                cc.max_backoff_ms,
                cc.overall_deadline_seconds,
                cc.per_attempt_timeout_seconds,
                seed=stable_seed(self.cfg.project.seed, cell.cell_key, writer_id),
            ),
            telemetry=telemetry,
            expected_ts=lambda q: publication_ts(w.publication_timestamp_origin_utc, w.publication_interval_millis, q),
            staging_count_check=w.in_transaction_staging_count_check,
            sql_sink=sink,
        )


# =============================================================================== doctor
def doctor(rt: Runtime, write_probes: bool = True) -> CapabilityMatrix:
    env = collect_environment(None if rt.dry_run else (rt.spark or _spark(rt)), rt.cfg)
    matrix = CapabilityMatrix(iso(utc_now()), env)
    if rt.dry_run:
        from .capabilities import Capability

        matrix.add(Capability("doctor", CapStatus.NOT_RUN, "dry-run: no probes executed"))
        return matrix
    Prober(rt.executor, rt.cfg, rt.dialect, write_probes=write_probes, results_root=rt.paths.root).run_all(matrix)
    rt.paths.root.mkdir(parents=True, exist_ok=True)
    matrix.save(rt.paths.capabilities)
    write_json(rt.paths.environment, env)
    return matrix


def _spark(rt: Runtime) -> Any:
    _ = rt.executor  # creates the session
    return rt.spark


# =============================================================================== init
def init(rt: Runtime, resume: bool = False) -> Plan:
    if rt.paths.plan.exists():
        if not resume:
            raise RunError(f"run {rt.run_id} already initialised ({rt.paths.plan}); use a new --run-id or --resume")
        return rt.load_plan()
    plan = build_plan(rt.cfg, rt.run_id)
    matrix = rt.load_capabilities()
    if matrix is None and not rt.dry_run:
        matrix = doctor(rt)
    if not rt.dry_run:
        rt.paths.root.mkdir(parents=True, exist_ok=True)
        write_json(rt.paths.plan, plan.to_json(), exclusive=True)
        write_json(rt.paths.root / "config.resolved.json", rt.cfg.model_dump(mode="json"))
    # schema
    if rt.ns.mode.value == "per_run_schema":
        rt.ddl_exec(create_schema_sql(rt.ns))
    # staging per profile (plain managed Delta; read-only inside transactions)
    for pk in plan.required_pairs:
        prof = plan.profiles[pk]
        for i in range(prof.table_count):
            rt.create_table(
                staging_table(rt.ns, pk, i),
                staging_columns(),
                TableFormat.DELTA,
                rt.table_props(TableFormat.DELTA, transactional=False),
                ("publication_sequence",),
            )
    for cell in plan.cells:
        cp = rt.paths.cell(cell.cell_key)
        ok, reason = (True, "")
        if matrix is not None:
            ok, reason = gate(matrix, requirements_for(rt.cfg, cell.strategy, cell.fmt, cell.launcher))
            if ok and cell.fault is FaultScenario.CONSTRAINT_VIOLATION_REAL and cell.fmt is TableFormat.ICEBERG:
                ok, reason = False, "CHECK constraints are not available on managed Iceberg tables (docs: iceberg)"
        if not ok:
            if not rt.dry_run:
                set_status(cp, "SKIPPED", reason=reason, cell=cell.to_json())
            continue
        tables = rt.tables_for(cell)
        props = rt.table_props(cell.fmt, transactional=True)
        try:
            cell_ns = rt.ns.for_cell(cell.cell_key)
            if cell_ns is not rt.ns:
                rt.ddl_exec(create_schema_sql(cell_ns))
            for t in tables.targets:
                rt.create_table(t, target_columns(cell.strategy), cell.fmt, props)
            rt.create_table(tables.ledger, LEDGER_COLUMNS, cell.fmt, props)
            rt.create_table(tables.fence, FENCE_COLUMNS, cell.fmt, props)
            rt.ddl_exec(fence_seed_sql(tables.fence, list(cell.scopes)))
            if not rt.dry_run and not rt.cfg.is_local:
                _assert_transactional_tables(rt, cell, tables)
        except Exception as exc:  # one cell's DDL must not abort the run; it is recorded, never PASS
            log.exception("DDL failed for cell %s", cell.cell_key)
            set_status(
                cp, "FAILED", error=f"setup DDL failed: {type(exc).__name__}: {str(exc)[:1500]}", cell=cell.to_json()
            )
            continue
        if not rt.dry_run:
            set_status(cp, "PLANNED", cell=cell.to_json(), atomicity_evidence=atomicity_evidence(rt.cfg))
    if not rt.dry_run:
        rt.paths.init_sql.parent.mkdir(parents=True, exist_ok=True)
        rt.paths.init_sql.write_text(";\n\n".join(rt.ddl) + ";\n", encoding="utf-8")
    return plan


def _assert_transactional_tables(rt: Runtime, cell: Cell, tables: CellTables) -> None:
    """Verify every written table is managed and (for Delta) has the catalogManaged feature, as required for
    multi-table transactions (docs: tables/features/catalog-commits; verify via DESCRIBE DETAIL)."""
    for t in (*tables.targets, tables.ledger, tables.fence):
        d = table_detail(rt.executor, t)
        if not d.get("observable"):
            if cell.fmt is TableFormat.DELTA:
                raise RunError(f"cannot DESCRIBE DETAIL {t.plain}: {d.get('error')}")
            continue  # Iceberg detail may be unavailable; the iceberg_in_txn probe gates these cells
        ext = rt.executor.execute(f"DESCRIBE TABLE EXTENDED {t.sql}")
        ttype = next((str(r.get("data_type")).strip() for r in ext if str(r.get("col_name")).strip() == "Type"), None)
        if ttype is not None and ttype.upper() != "MANAGED":
            raise RunError(f"{t.plain} is {ttype}, not a Unity Catalog MANAGED table")
        if cell.fmt is TableFormat.DELTA and "catalogManaged" not in list(d.get("tableFeatures") or []):
            raise RunError(f"{t.plain} lacks the catalogManaged table feature: {d.get('tableFeatures')}")


# =============================================================================== generate
def generate(rt: Runtime) -> list[dict[str, Any]]:
    plan = rt.load_plan()
    out = []
    for pk, pairs in plan.required_pairs.items():
        prof = plan.profiles[pk]
        refs = [staging_table(rt.ns, pk, i) for i in range(prof.table_count)]
        _ = rt.executor
        for st in generate_profile(rt.spark, prof, pairs, refs):
            rec = {**st.to_json(), "at": iso(utc_now())}
            append_jsonl(rt.paths.generation, rec)
            out.append(rec)
    return out


# =============================================================================== run
def run(
    rt: Runtime, only: Iterable[str] | None = None, resume: bool = False, kinds: Iterable[str] | None = None
) -> dict[str, str]:
    plan = rt.load_plan()
    wanted = set(only) if only else None
    kind_set = set(kinds) if kinds else None
    statuses: dict[str, str] = {}
    for cell in plan.cells:
        if wanted is not None and cell.cell_key not in wanted:
            continue
        if kind_set is not None and cell.kind.value not in kind_set:
            continue
        cp = rt.paths.cell(cell.cell_key)
        st = get_status(cp)
        if st == "SKIPPED":
            statuses[cell.cell_key] = "SKIPPED"
            continue
        if st == "FAILED" and "setup DDL failed" in str(read_json(cp.status).get("error", "")):
            statuses[cell.cell_key] = "FAILED"
            continue
        if st in ("COMPLETE", "FAILED") and resume:
            statuses[cell.cell_key] = st
            continue
        if st != "PLANNED":
            raise RunError(
                f"cell {cell.cell_key} is in state {st}; cells can only run once from PLANNED "
                "(a partially executed cell is not resumable: start a new run_id)"
            )
        set_status(cp, "RUNNING", started_at=iso(utc_now()))
        try:
            if cell.kind is CellKind.PERFORMANCE:
                verdict = _run_performance(rt, cell)
            elif cell.kind is CellKind.CONTENTION:
                verdict = _run_contention(rt, cell)
            else:
                verdict = _run_fault(rt, cell)
            final = "COMPLETE"
            set_status(cp, final, verdict=verdict, finished_at=iso(utc_now()))
        except Exception as exc:
            log.exception("cell %s failed", cell.cell_key)
            set_status(cp, "FAILED", error=f"{type(exc).__name__}: {str(exc)[:2000]}", finished_at=iso(utc_now()))
            final = "FAILED"
        statuses[cell.cell_key] = final
    return statuses


def _record_result(rt: Runtime, cell: Cell, res: PublicationResult, phase: str, **extra: Any) -> None:
    cp = rt.paths.cell(cell.cell_key)
    append_jsonl(cp.publications, {**res.to_json(), "cell_key": cell.cell_key, "phase": phase, **extra})
    for a in res.attempts:
        append_jsonl(cp.attempts, {**a.__dict__, "phase": phase})


def _setup(rt: Runtime, cell: Cell) -> None:
    if not cell.setup_publications:
        return
    cp = rt.paths.cell(cell.cell_key)
    eng = rt.engine(
        cell,
        "setup",
        TelemetrySink(cp.telemetry / "setup.jsonl", run_id=rt.run_id, cell_key=cell.cell_key, writer_id="setup"),
    )
    try:
        for p in cell.setup_publications:
            res = eng.publish(p)
            _record_result(rt, cell, res, "setup")
            if not res.status.is_committed:
                raise RunError(f"setup publication {p.publication_id} failed: {res.status} {res.detail}")
    finally:
        eng.close()


def _collect_metrics(rt: Runtime, cell: Cell, phase: str) -> None:
    if not rt.cfg.benchmark.measure_storage_metrics:
        return
    cp = rt.paths.cell(cell.cell_key)
    tables = rt.tables_for(cell)
    for t in (*tables.targets, tables.ledger, tables.fence):
        append_jsonl(
            cp.table_metrics,
            {
                "phase": phase,
                "table": t.name,
                "format": cell.fmt.value,
                "detail": table_detail(rt.executor, t),
                "history": table_history(rt.executor, t),
            },
        )


def _validate(
    rt: Runtime, cell: Cell, committed: set[str], not_committed: set[str], staging: tuple[TableRef, ...] | None = None
) -> ValidationReport:
    v = Validator(
        rt.executor, rt.tables_for(cell), staging or rt.staging_for(cell), cell.strategy, cell.ordering, cell.cell_key
    )
    return v.run(committed, not_committed - committed)


def _outcome_sets(results: Iterable[dict[str, Any]]) -> tuple[set[str], set[str]]:
    committed: set[str] = set()
    not_committed: set[str] = set()
    for r in results:
        pid = r["publication"]["publication_id"] if "publication" in r else r["publication_id"]
        (committed if r["status"] in COMMITTED_STATUSES else not_committed).add(pid)
    return committed, not_committed


def _save_validation(rt: Runtime, cell: Cell, report: ValidationReport, **extra: Any) -> dict[str, Any]:
    data = {
        **report.to_json(),
        "atomicity_evidence": atomicity_evidence(rt.cfg),
        "transaction_mode": rt.cfg.compute.transaction_mode.value,
        **extra,
    }
    write_json(rt.paths.cell(cell.cell_key).validation, data)
    return data


def _run_performance(rt: Runtime, cell: Cell) -> str:
    cp = rt.paths.cell(cell.cell_key)
    _setup(rt, cell)
    writer = cell.writers[0]
    tel = TelemetrySink(
        cp.telemetry / f"{writer.writer_id}.jsonl", run_id=rt.run_id, cell_key=cell.cell_key, writer_id=writer.writer_id
    )
    eng = rt.engine(cell, writer.writer_id, tel)
    results = []
    try:
        for i, p in enumerate(writer.publications):
            res = eng.publish(p)
            _record_result(rt, cell, res, "measured", warmup=i < cell.warmup_publications, ordinal=i)
            results.append(res.to_json())
            if not res.status.is_committed:
                break
    finally:
        eng.close()
    _collect_metrics(rt, cell, "after_publications")
    if rt.cfg.benchmark.include_maintenance_cost:
        for t in rt.tables_for(cell).targets:
            m = maintenance(rt.executor, t)
            append_jsonl(
                cp.table_metrics,
                {
                    "phase": "maintenance",
                    "table": t.name,
                    "format": cell.fmt.value,
                    "maintenance": m,
                    "detail_after": table_detail(rt.executor, t),
                },
            )
    if rt.cfg.benchmark.measure_read_queries and results:
        _run_read_queries(rt, cell)
    committed, not_committed = _outcome_sets(results)
    report = _validate(rt, cell, committed, not_committed)
    _save_validation(rt, cell, report)
    return report.verdict


def _run_read_queries(rt: Runtime, cell: Cell) -> None:
    cp = rt.paths.cell(cell.cell_key)
    tables = rt.tables_for(cell)
    pubs = [p for p in cell.writers[0].publications if p.dataset_scope == cell.scopes[0]]
    if not pubs:
        return
    mid = pubs[len(pubs) // 2]
    params = QueryParams(
        scope=cell.scopes[0],
        business_key="K" + str(max(0, pubs[0].expected_rows // 2)).zfill(10),
        as_of=mid.publication_ts,
        window_start=pubs[0].publication_ts,
        window_end=mid.publication_ts,
    )
    for rec in run_queries(
        rt.executor,
        cell.strategy,
        tables.targets[0],
        tables.ledger,
        params,
        rt.cfg.benchmark.read_query_repetitions,
        rt.cfg.benchmark.clear_cache_before_cold_queries,
    ):
        append_jsonl(
            cp.queries, {**rec, "cell_key": cell.cell_key, "strategy": cell.strategy.value, "format": cell.fmt.value}
        )


# ------------------------------------------------------------------------------- contention
def _writer_plan(rt: Runtime, cell: Cell, writer_idx: int, barrier_root: Path) -> dict[str, Any]:
    w = cell.writers[writer_idx]
    cp = rt.paths.cell(cell.cell_key)
    tables = rt.tables_for(cell)
    cc = rt.cfg.concurrency
    plan: dict[str, Any] = {
        "run_id": rt.run_id,
        "cell_key": cell.cell_key,
        "writer_id": w.writer_id,
        "strategy": cell.strategy.value,
        "ordering": cell.ordering.value,
        "launcher": cell.launcher.value,
        "config": rt.cfg.model_dump(mode="json"),
        "publications": [p.to_json() for p in w.publications],
        "faults": list(w.faults),
        "start_offset_ms": w.start_offset_ms,
        "tables": {
            "targets": [t.plain for t in tables.targets],
            "ledger": tables.ledger.plain,
            "fence": tables.fence.plain,
        },
        "staging": [s.plain for s in rt.staging_for(cell)],
        "barrier": {
            "enabled": True,
            "root": str(barrier_root.resolve()),
            "timeout_seconds": cc.barrier_timeout_seconds,
            "poll_seconds": cc.barrier_poll_seconds,
        },
        "telemetry_path": str((cp.telemetry / f"{w.writer_id}.jsonl").resolve()),
        "sql_log": str((cp.sql / f"{w.writer_id}.executed.sql").resolve())
        if rt.cfg.reporting.save_executed_sql
        else None,
        "submitted_at": iso(utc_now()),
    }
    if rt.cfg.is_local:
        plan["local_warehouse"] = str(rt.warehouse)
        plan["local_catalog"] = str(rt.paths.local_tables.resolve())
    return plan


class ReaderProbe:
    """Measures what a NON-transactional reader observes while writers commit (spec: reader-visible snapshots).

    One query reads max(publication_sequence) of every target table and the ledger. If a multi-table commit
    were not visible atomically to such a reader, values would differ. This is a MEASUREMENT of the observed
    behaviour (recorded in reader_probe.jsonl), not an asserted guarantee: the docs only promise repeatable
    reads inside a transaction.
    """

    def __init__(self, rt: Runtime, cell: Cell, interval_s: float = 0.25) -> None:
        import threading

        self.rt, self.cell, self.interval_s = rt, cell, interval_s
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, name="dbxbench-reader", daemon=True)
        self.samples = 0
        self.skewed = 0
        self.errors = 0
        tables = rt.tables_for(cell)
        scope = sql_string(cell.scopes[0])
        parts = [
            f"(SELECT coalesce(max(publication_sequence), 0) FROM {t.sql} WHERE dataset_scope = {scope}) AS t{i:02d}"
            for i, t in enumerate(tables.targets)
        ]
        parts.append(
            f"(SELECT coalesce(max(publication_sequence), 0) FROM {tables.ledger.sql} "
            f"WHERE dataset_scope = {scope}) AS ledger"
        )
        self.sql = "SELECT " + ", ".join(parts)
        # overlapping_tables publications touch subsets, so equal maxima are not expected there
        self.applicable = cell.contention is not ContentionMode.OVERLAPPING_TABLES

    def start(self) -> None:
        if self.applicable:
            self._thread.start()

    def _loop(self) -> None:
        path = self.rt.paths.cell(self.cell.cell_key).root / "reader_probe.jsonl"
        while not self._stop.is_set():
            try:
                row = self.rt.executor.execute(self.sql)[0]
                vals = {k: int(v) for k, v in row.items()}
                skew = len(set(vals.values())) > 1
                self.samples += 1
                self.skewed += int(skew)
                if skew:
                    append_jsonl(path, {"at": iso(utc_now()), "observed": vals})
            except Exception as exc:
                self.errors += 1
                append_jsonl(path, {"at": iso(utc_now()), "error": str(exc)[:300]})
            self._stop.wait(self.interval_s)

    def stop(self) -> dict[str, Any]:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=30)
        return {
            "applicable": self.applicable,
            "samples": self.samples,
            "skewed_samples": self.skewed,
            "errors": self.errors,
            "query": self.sql,
            "note": "skew = a reader saw different latest sequences across tables/ledger in ONE query",
        }


def launch_writers(rt: Runtime, cell: Cell) -> list[LaunchHandle]:
    cp = rt.paths.cell(cell.cell_key)
    cc = rt.cfg.concurrency
    barrier = FileBarrier(cp.barrier, poll_seconds=cc.barrier_poll_seconds)
    plans: dict[str, Path] = {}
    for i, w in enumerate(cell.writers):
        path = cp.plans / f"{w.writer_id}.json"
        write_json(path, _writer_plan(rt, cell, i, cp.barrier))
        plans[w.writer_id] = path.resolve()
    if cell.launcher is Launcher.DATABRICKS_JOBS:
        if cc.worker_job_id is None:
            raise RunError("concurrency.worker_job_id is required for launcher=databricks_jobs (deploy the bundle)")
        launcher: Any = DatabricksJobsLauncher(cc.worker_job_id, cc.job_run_poll_seconds)
    elif cell.launcher is Launcher.LOCAL_PROCESS:
        launcher = LocalProcessLauncher(cp.root / "worker_logs")
    else:
        raise RunError(f"launcher {cell.launcher} cannot run independent writers")
    t_launch = time.perf_counter()
    handles: list[LaunchHandle] = launcher.launch(plans)
    launch_s = time.perf_counter() - t_launch
    coord: dict[str, Any] = {"launcher": launcher.name, "launch_seconds": launch_s}
    try:
        go = barrier.coordinate(
            {w.writer_id for w in cell.writers}, cc.barrier_timeout_seconds, cc.barrier_go_lead_seconds
        )
        coord["go"] = go
    except (BarrierTimeout, BarrierAborted) as exc:
        coord["barrier_failed_closed"] = str(exc)
    coord["ready"] = barrier.ready_writers()
    reader = ReaderProbe(rt, cell)
    reader.start()
    try:
        handles = list(launcher.wait(handles, cc.overall_deadline_seconds + cc.barrier_timeout_seconds + 120))
    finally:
        coord["reader_probe"] = reader.stop()
    coord["handles"] = [h.to_json() for h in handles]
    write_json(cp.launch, coord)
    return handles


def _run_contention(rt: Runtime, cell: Cell) -> str:
    cp = rt.paths.cell(cell.cell_key)
    _setup(rt, cell)
    if cell.launcher is Launcher.INLINE:
        if cell.writer_count != 1:
            raise RunError("inline launcher cannot run concurrent writers")
        w = cell.writers[0]
        eng = rt.engine(
            cell,
            w.writer_id,
            TelemetrySink(
                cp.telemetry / f"{w.writer_id}.jsonl", run_id=rt.run_id, cell_key=cell.cell_key, writer_id=w.writer_id
            ),
        )
        for p in w.publications:
            _record_result(rt, cell, eng.publish(p), "measured")
        eng.close()
    else:
        handles = launch_writers(rt, cell)
        events = read_all(cp.telemetry)
        for e in events:
            if e.get("event") == "publication_end" and e.get("writer_id") != "setup":
                append_jsonl(cp.publications, {**e, "status": e["status"], "phase": "measured"})
            elif e.get("event") == "attempt_end" and e.get("writer_id") != "setup":
                append_jsonl(cp.attempts, {**e, "phase": "measured"})
        bad = [
            h.writer_id
            for h in handles
            if h.exit_code not in (None, 0) or (h.final_state not in (None, "EXITED", "SUCCESS"))
        ]
        if bad:
            append_jsonl(
                cp.root / "harness_errors.jsonl",
                {"writers_with_errors": bad, "handles": [h.to_json() for h in handles]},
            )
    order = ledger_commit_order(rt.executor, rt.tables_for(cell).ledger)
    write_json(cp.root / "commit_order.json", order)
    _collect_metrics(rt, cell, "after_contention")
    pubs = _read_jsonl(cp.publications)
    committed, not_committed = _outcome_sets(
        [p for p in pubs if p.get("phase") == "measured"] + [p for p in pubs if p.get("phase") == "setup"]
    )
    report = _validate(rt, cell, committed, not_committed)
    harness_errors = (cp.root / "harness_errors.jsonl").exists()
    data = _save_validation(rt, cell, report, harness_errors=harness_errors)
    if harness_errors:
        return "FAIL_HARNESS_ERROR"
    if not atomicity_evidence(rt.cfg) and report.failed():
        # Non-atomic emulation: statements of one publication become visible one by one, so concurrent writers
        # can interleave (a conflict mid-script leaves partial state; in `sequenced` mode a writer can pass the
        # fence before its predecessor's data is visible). The validators detected it. Recorded verbatim in
        # validation.json; never counted as an atomicity or ordering result.
        return "EMULATION_LEAK_DETECTED"
    return str(data["verdict"])


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


# ------------------------------------------------------------------------------- faults
FAULT_EXPECTATIONS: dict[str, str] = {
    "after_first_update": "first attempt ROLLED_BACK_INJECTED with zero partial rows; clean resubmission COMMITTED",
    "after_first_insert": "first attempt ROLLED_BACK_INJECTED with zero partial rows; clean resubmission COMMITTED",
    "last_table": "first attempt ROLLED_BACK_INJECTED with zero partial rows; clean resubmission COMMITTED",
    "after_ledger": "first attempt ROLLED_BACK_INJECTED: no ghost ledger row and no data; resubmission COMMITTED",
    "duplicate_delivery": "first COMMITTED; second delivery ALREADY_COMMITTED (no-op); one ledger row",
    "late_delivery": "seq 3 COMMITTED; late seq 2 REJECTED_STALE; seq 3 rows remain active and unclosed",
    "lost_ack": "attempt 1 commits, reply lost; reconciled from ledger as COMMITTED_RECONCILED; no re-apply",
    "client_timeout": "client gives up; outcome reconciled from ledger; exactly one ledger row and full batch",
    "schema_mismatch": "prevalidation rejects (no submit); forced submit fails mid-block with zero partial rows; "
    "clean resubmission COMMITTED",
    "duplicate_keys_input": "prevalidation rejects duplicate keys; forced submit fails in-txn staging check with zero "
    "partial rows; clean resubmission COMMITTED",
    "constraint_violation_real": "REAL engine error: CHECK (publication_sequence < 2) admits existing rows but rejects "
    "the new publication on the LAST table, so earlier tables must roll back; after dropping the constraint the "
    "resubmission COMMITTED",
    "writer_termination": "writer killed mid-attempt; ledger decides outcome; invariants hold either way",
}


# In NON-atomic local emulation these faults necessarily leave partial state behind; the validators must
# detect it. Under BEGIN ATOMIC the same faults must leave no trace at all.
EXPECTS_LEAK_IN_EMULATION = {
    "after_first_update",
    "after_first_insert",
    "last_table",
    "after_ledger",
    "schema_mismatch",
    "duplicate_keys_input",
    "constraint_violation_real",
}


def _snapshot_state(rt: Runtime, cell: Cell) -> dict[str, Any]:
    t = rt.tables_for(cell)
    x = rt.executor
    state: dict[str, Any] = {
        "ledger": sorted(r["publication_id"] for r in x.execute(f"SELECT publication_id FROM {t.ledger.sql}")),
        "fence": x.execute(f"SELECT dataset_scope, last_sequence, commit_count FROM {t.fence.sql} ORDER BY 1"),
    }
    for tt in t.targets:
        state[tt.name] = x.execute(
            f"SELECT publication_id, count(*) AS n, count(effective_to_date) AS closed FROM {tt.sql} "
            f"GROUP BY 1 ORDER BY 1"
            if cell.strategy is not Strategy.APPEND_ONLY_FULL
            else f"SELECT publication_id, count(*) AS n FROM {tt.sql} GROUP BY 1 ORDER BY 1"
        )
    return state


def _bad_staging(rt: Runtime, cell: Cell, kind: str) -> tuple[TableRef, ...]:
    """Fault-specific staging override for the LAST table (outside any transaction)."""
    base = list(rt.staging_for(cell))
    last = len(base) - 1
    ref = rt.ns.for_cell(cell.cell_key).table(f"stg_{cell.cell_key}_{kind}")
    prof = rt.load_plan().profiles[cell.profile_key]
    pub = cell.writers[0].publications[0]
    df = snapshot_frame(rt.spark, prof, last, {(pub.dataset_scope, pub.sequence)})
    if kind == "dup":
        df = with_duplicate_keys(df, 5)
    else:
        df = df.withColumnRenamed("attr_int", "attr_int_renamed")
    location = None
    if rt.local_catalog is not None:
        location = rt.local_catalog.location_for(rt.warehouse, ref)
        df.write.format("delta").save(location)
        rt.ddl_exec(f"CREATE TABLE {ref.sql} USING DELTA LOCATION {sql_string(location)}")
        rt.local_catalog.record(ref, location)
    else:
        df.write.format("delta").saveAsTable(ref.plain)
        rt.ddl_exec(
            f"ALTER TABLE {ref.sql} SET TBLPROPERTIES ({sql_string(OWNER_PROPERTY)} = "
            f"{sql_string(OWNER_VALUE)}, {sql_string(RUN_PROPERTY)} = {sql_string(rt.run_id)})"
        )
    base[last] = ref
    return tuple(base)


def _run_fault(rt: Runtime, cell: Cell) -> str:
    assert cell.fault is not None
    fault = cell.fault
    cp = rt.paths.cell(cell.cell_key)
    _setup(rt, cell)
    baseline = _snapshot_state(rt, cell)
    tel = TelemetrySink(cp.telemetry / "w000.jsonl", run_id=rt.run_id, cell_key=cell.cell_key, writer_id="w000")
    eng = rt.engine(cell, "w000", tel)
    steps: list[dict[str, Any]] = []
    pubs = cell.writers[0].publications
    fi = rt.cfg.fault_injection

    def step(label: str, res: PublicationResult, expect: set[PublicationStatus]) -> None:
        _record_result(rt, cell, res, "measured", step=label)
        steps.append(
            {
                "step": label,
                "status": res.status.value,
                "expected": sorted(s.value for s in expect),
                "ok": res.status in expect,
                "attempts": len(res.attempts),
                "detail": res.detail,
                "state_after": _snapshot_state(rt, cell),
            }
        )

    unchanged_after_failure: bool | None = None
    try:
        if fault in (
            FaultScenario.AFTER_FIRST_UPDATE,
            FaultScenario.AFTER_FIRST_INSERT,
            FaultScenario.LAST_TABLE,
            FaultScenario.AFTER_LEDGER,
        ):
            step(
                "faulty_attempt",
                eng.publish(pubs[0], EngineFaults(in_txn=fault)),
                {PublicationStatus.ROLLED_BACK_INJECTED},
            )
            unchanged_after_failure = steps[-1]["state_after"] == baseline
            step("entire_batch_resubmitted", eng.publish(pubs[1]), {PublicationStatus.COMMITTED})
        elif fault is FaultScenario.DUPLICATE_DELIVERY:
            step("first_delivery", eng.publish(pubs[0]), {PublicationStatus.COMMITTED})
            step("duplicate_delivery", eng.publish(pubs[1]), {PublicationStatus.ALREADY_COMMITTED})
        elif fault is FaultScenario.LATE_DELIVERY:
            step("newer_seq3", eng.publish(pubs[0]), {PublicationStatus.COMMITTED})
            before = _snapshot_state(rt, cell)
            step("late_seq2", eng.publish(pubs[1]), {PublicationStatus.REJECTED_STALE})
            unchanged_after_failure = steps[-1]["state_after"] == before
        elif fault is FaultScenario.LOST_ACK:
            step(
                "lost_ack",
                eng.publish(pubs[0], EngineFaults(lost_ack_first_attempt=True)),
                {PublicationStatus.COMMITTED_RECONCILED},
            )
            step("redelivery_after_lost_ack", eng.publish(pubs[1]), {PublicationStatus.ALREADY_COMMITTED})
        elif fault is FaultScenario.CLIENT_TIMEOUT:
            res = eng.publish(pubs[0], EngineFaults(client_timeout_first_attempt_s=fi.client_timeout_seconds))
            eng.close(wait=True)  # let any abandoned in-flight attempt finish before inspecting state
            step(
                "client_timeout",
                res,
                {
                    PublicationStatus.COMMITTED,
                    PublicationStatus.COMMITTED_RECONCILED,
                    PublicationStatus.ALREADY_COMMITTED,
                },
            )
            eng = rt.engine(cell, "w000", tel)
            step("redelivery", eng.publish(pubs[1]), {PublicationStatus.ALREADY_COMMITTED})
        elif fault in (FaultScenario.SCHEMA_MISMATCH, FaultScenario.DUPLICATE_KEYS_INPUT):
            bad = _bad_staging(rt, cell, "drift" if fault is FaultScenario.SCHEMA_MISMATCH else "dup")
            bad_eng = rt.engine(cell, "w000", tel, staging=bad)
            step("prevalidation_rejects", bad_eng.publish(pubs[0]), {PublicationStatus.PREVALIDATION_FAILED})
            step(
                "forced_submit_without_prevalidation",
                bad_eng.publish(pubs[0], EngineFaults(skip_prevalidation=True)),
                {PublicationStatus.FAILED_NONRETRYABLE},
            )
            bad_eng.close()
            unchanged_after_failure = steps[-1]["state_after"] == baseline
            step("clean_resubmission", eng.publish(pubs[1]), {PublicationStatus.COMMITTED})
        elif fault is FaultScenario.CONSTRAINT_VIOLATION_REAL:
            last = rt.tables_for(cell).targets[-1]
            rt.ddl_exec(
                f"ALTER TABLE {last.sql} ADD CONSTRAINT {quote_ident('dbxbench_force_fail')} "
                "CHECK (publication_sequence < 2)"
            )
            step("real_error_mid_transaction", eng.publish(pubs[0]), {PublicationStatus.FAILED_NONRETRYABLE})
            unchanged_after_failure = steps[-1]["state_after"] == baseline
            rt.ddl_exec(f"ALTER TABLE {last.sql} DROP CONSTRAINT {quote_ident('dbxbench_force_fail')}")
            step("resubmission_after_fix", eng.publish(pubs[1]), {PublicationStatus.COMMITTED})
        else:
            raise RunError(f"fault {fault} is not runnable inline")
    finally:
        eng.close()
    results = _read_jsonl(cp.publications)
    committed, not_committed = _outcome_sets(results)
    report = _validate(rt, cell, committed, not_committed)
    steps_ok = all(s["ok"] for s in steps)
    if unchanged_after_failure is not None:
        report.checks.append(
            CheckResult(
                "failed_step_left_no_trace",
                "atomicity",
                None,
                CheckStatus.PASS if unchanged_after_failure else CheckStatus.FAIL,
                0 if unchanged_after_failure else 1,
                "row/ledger/fence snapshot after failed step == snapshot before it",
                note="compares per-table publication counts, ledger ids and fence rows",
            )
        )
    evidence = atomicity_evidence(rt.cfg)
    leaked = unchanged_after_failure is False
    if evidence:
        verdict = "PASS" if steps_ok and report.verdict == "PASS" else "FAIL"
    else:
        # Non-atomic emulation: an in-transaction fault is EXPECTED to leak partial state. The harness
        # must DETECT it; detecting it proves validator sensitivity, it never proves atomicity.
        expects_leak = fault.value in EXPECTS_LEAK_IN_EMULATION
        if expects_leak:
            detected = leaked or report.verdict == "FAIL" or not steps_ok
            verdict = "EMULATION_LEAK_DETECTED" if detected else "FAIL_VALIDATOR_MISSED_LEAK"
        else:
            verdict = "PASS_EMULATION" if steps_ok and report.verdict == "PASS" else "FAIL"
    _save_validation(
        rt,
        cell,
        report,
        fault=fault.value,
        expectation=FAULT_EXPECTATIONS.get(fault.value),
        steps=steps,
        steps_ok=steps_ok,
        fault_verdict=verdict,
        simulated=fault.value not in {"constraint_violation_real", "schema_mismatch", "duplicate_keys_input"},
    )
    return verdict


# =============================================================================== verify
def verify(rt: Runtime) -> dict[str, Any]:
    """Re-run every validator from persisted state (tables + plan + client outcomes), plus parity."""
    plan = rt.load_plan()
    out: dict[str, Any] = {"cells": {}, "parity": []}
    complete: list[Cell] = []
    for cell in plan.cells:
        cp = rt.paths.cell(cell.cell_key)
        st = get_status(cp)
        if st != "COMPLETE":
            out["cells"][cell.cell_key] = {"status": st, "verdict": "NOT_VERIFIED"}
            continue
        results = _read_jsonl(cp.publications)
        committed, not_committed = _outcome_sets(results)
        staging = None
        if cell.fault in (FaultScenario.SCHEMA_MISMATCH, FaultScenario.DUPLICATE_KEYS_INPUT):
            staging = rt.staging_for(cell)
        report = _validate(rt, cell, committed, not_committed, staging)
        verdict = report.verdict
        if cell.kind is CellKind.CONTENTION and not atomicity_evidence(rt.cfg) and report.verdict == "FAIL":
            verdict = "EMULATION_LEAK_DETECTED"
        elif (
            cell.kind is CellKind.FAULT
            and not atomicity_evidence(rt.cfg)
            and cell.fault is not None
            and cell.fault.value in EXPECTS_LEAK_IN_EMULATION
        ):
            # A leak is detected either by table invariants or, for faults after the ledger insert (which
            # leave a complete but unauthorised commit), by the persisted ledger reconciliation of the step.
            persisted = read_json(cp.validation) if cp.validation.exists() else {}
            steps_ok = bool(persisted.get("steps_ok", True))
            detected = report.verdict == "FAIL" or not steps_ok
            verdict = "EMULATION_LEAK_DETECTED" if detected else "FAIL_VALIDATOR_MISSED_LEAK"
        out["cells"][cell.cell_key] = {
            "status": st,
            "verdict": verdict,
            "validator_verdict": report.verdict,
            "failed": [c.name for c in report.failed()],
        }
        if cell.kind is CellKind.PERFORMANCE:
            complete.append(cell)
    groups: dict[tuple[str, int, str], list[Cell]] = {}
    for c in complete:
        groups.setdefault((c.profile_key, c.repetition, c.sweep_label), []).append(c)
    for (pk, rep, label), cells in groups.items():
        full = [c for c in cells if c.strategy in (Strategy.SCD2_FULL, Strategy.APPEND_ONLY_FULL)]
        for i in range(len(full)):
            for j in range(i + 1, len(full)):
                a, b = full[i], full[j]
                res = parity_check(
                    rt.executor, f"{a.cell_key}~{b.cell_key}", rt.tables_for(a).targets, rt.tables_for(b).targets
                )
                out["parity"].append(
                    {
                        **res.to_json(),
                        "profile": pk,
                        "repetition": rep,
                        "sweep": label,
                        "left": f"{a.strategy.value}/{a.fmt.value}",
                        "right": f"{b.strategy.value}/{b.fmt.value}",
                    }
                )
    out["verified_at"] = iso(utc_now())
    out["atomicity_evidence"] = atomicity_evidence(rt.cfg)
    write_json(rt.paths.root / "verification.json", out)
    return out


# =============================================================================== manifest
def finalize_manifest(rt: Runtime) -> Path:
    plan = rt.load_plan()
    caps = rt.load_capabilities()
    env = read_json(rt.paths.environment) if rt.paths.environment.exists() else {}
    cells = {}
    for c in plan.cells:
        cp = rt.paths.cell(c.cell_key)
        s = read_json(cp.status) if cp.status.exists() else {}
        cells[c.cell_key] = {
            "kind": c.kind.value,
            "strategy": c.strategy.value,
            "format": c.fmt.value,
            "status": s.get("status"),
            "verdict": s.get("verdict"),
            "reason": s.get("reason"),
            "truly_ran": s.get("status") == "COMPLETE",
        }
    gen = _read_jsonl(rt.paths.generation)
    cc = rt.cfg.concurrency
    manifest = {
        "schema_version": 1,
        "run_id": rt.run_id,
        "created_at": iso(utc_now()),
        "git_sha": git_sha(),
        "config_hash": rt.cfg.config_hash(),
        "config": rt.cfg.model_dump(mode="json"),
        "catalog": rt.ns.catalog,
        "schema": rt.ns.schema,
        "namespace_mode": rt.ns.mode.value,
        "transaction_mode": rt.cfg.compute.transaction_mode.value,
        "atomicity_evidence": atomicity_evidence(rt.cfg),
        "runtime": {
            "databricks_runtime_version": env.get("databricks_runtime_version"),
            "spark_version": env.get("spark_version"),
            "python": env.get("python"),
        },
        "spark_conf": env.get("spark_conf", {}),
        "compute": rt.cfg.compute.model_dump(mode="json"),
        "timezone": rt.cfg.compute.timezone,
        "data_seed": rt.cfg.project.seed,
        "staging": {
            "rows": sum(int(g.get("rows") or 0) for g in gen),
            "bytes": sum(int(g.get("size_bytes") or 0) for g in gen),
            "tables": gen,
        },
        "concurrency": cc.model_dump(mode="json"),
        "retry_settings": {
            "max_attempts": cc.max_attempts,
            "initial_backoff_ms": cc.initial_backoff_ms,
            "max_backoff_ms": cc.max_backoff_ms,
            "jitter": "full",
            "per_attempt_timeout_seconds": cc.per_attempt_timeout_seconds,
            "overall_deadline_seconds": cc.overall_deadline_seconds,
        },
        "capabilities": {k: v.status.value for k, v in caps.capabilities.items()} if caps else None,
        "cells": cells,
    }
    path = rt.paths.manifest
    if path.exists():
        n = len(list(rt.paths.root.glob("run_manifest.*.json"))) + 1
        path = rt.paths.root / f"run_manifest.{n}.json"
    write_json(path, manifest, exclusive=True)
    return path
