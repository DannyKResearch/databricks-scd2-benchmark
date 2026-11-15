"""Live Databricks acceptance tests (Phase 03/06). Run ONLY on DBR 18.0+ / serverless with:

    DBXBENCH_DATABRICKS_TESTS=1 DBXBENCH_CATALOG=<catalog> DBXBENCH_RESULTS_ROOT=/Volumes/<c>/<s>/<v>/runs \
        pytest -m databricks tests/databricks

(the bundle's `dbxbench_live_tests` job does exactly this). Off-platform they are SKIPPED with a
NOT_RUN reason, never reported as passing. Every test uses its own run-owned schema, which is
intentionally NOT dropped automatically (evidence); drop it with `dbxbench cleanup`.
"""

from __future__ import annotations

import os
import threading

import pytest

pytestmark = pytest.mark.databricks


@pytest.fixture(scope="module")
def rt():
    from dbxbench.config import BenchConfig
    from dbxbench.naming import new_run_id
    from dbxbench.runner import Runtime, doctor, generate, init

    catalog = os.environ["DBXBENCH_CATALOG"]
    cfg = BenchConfig.model_validate(
        {
            "project": {
                "catalog": catalog,
                "schema_prefix": "dbxbench_live",
                # one schema per cell: stays under per-schema table quotas (Free Edition: 100 tables/schema)
                "schema_layout": os.environ.get("DBXBENCH_SCHEMA_LAYOUT", "per_cell"),
                "results_root": os.environ["DBXBENCH_RESULTS_ROOT"],
            },
            "compute": {"preferred_mode": os.environ.get("DBXBENCH_COMPUTE_MODE", "serverless")},
            "workload": {"table_count": 3, "rows_per_table_per_publication": 100, "publication_count": 3},
            "strategies": ["scd2_full", "append_only_full"],
            "formats": ["delta", "iceberg"],
            "concurrency": {"enabled": False, "launcher": "local_process", "writer_counts": [1]},
            "benchmark": {"repetitions": 1, "read_query_repetitions": 1},
            "fault_injection": {
                "formats": ["delta", "iceberg"],
                "scenarios": [
                    "after_first_update",
                    "after_first_insert",
                    "last_table",
                    "after_ledger",
                    "duplicate_delivery",
                    "late_delivery",
                    "lost_ack",
                    "client_timeout",
                    "schema_mismatch",
                    "duplicate_keys_input",
                    "constraint_violation_real",
                ],
            },
        }
    )
    r = Runtime(cfg, new_run_id())
    m = doctor(r)
    for cap in ("begin_atomic_multi_table", "begin_atomic_rollback", "catalog_commits_delta"):
        assert m.supported(cap), f"{cap}: {m.capabilities.get(cap)}"
    init(r)
    generate(r)
    return r


def _cells(rt, **kw):
    plan = rt.load_plan()
    return [c for c in plan.cells if all(getattr(c, k) == v for k, v in kw.items())]


def test_doctor_rollback_probe_left_no_rows(rt):
    """The doctor's rollback probe (SIGNAL inside BEGIN ATOMIC) left no inserted or updated rows behind."""
    m = rt.load_capabilities()
    v = m.capabilities["begin_atomic_rollback"].value
    assert v == {"leaked_insert": 0, "leaked_update": 0}


def test_performance_cells_atomic_and_valid(rt):
    """Every supported performance cell completes on real BEGIN ATOMIC with all invariants passing."""
    from dbxbench.runner import run
    from dbxbench.scenarios import CellKind

    statuses = run(rt, kinds=["performance"])
    for c in _cells(rt, kind=CellKind.PERFORMANCE):
        if statuses[c.cell_key] == "SKIPPED":
            continue  # e.g. Iceberg unsupported: SKIPPED is recorded with reason, never PASS
        st = __import__("json").loads(rt.paths.cell(c.cell_key).status.read_text())
        assert st["status"] == "COMPLETE" and st["verdict"] == "PASS", st


def test_every_block_was_one_spark_sql_call(rt):
    """Every recorded attempt is exactly one BEGIN ATOMIC block."""
    sql_dir_files = list((rt.paths.root / "cells").rglob("*.attempt*.sql"))
    assert sql_dir_files
    for f in sql_dir_files:
        text = f.read_text()
        assert text.startswith("BEGIN ATOMIC") and text.count("BEGIN ATOMIC") == 1


def test_fault_matrix(rt):
    """Every fault cell passes on real atomic transactions.

    Failed blocks leave no trace and idempotency faults apply exactly once.
    """
    import json

    from dbxbench.runner import run
    from dbxbench.scenarios import CellKind

    run(rt, kinds=["fault"])
    for c in _cells(rt, kind=CellKind.FAULT):
        st = json.loads(rt.paths.cell(c.cell_key).status.read_text())
        if st["status"] == "SKIPPED":
            continue
        assert st["status"] == "COMPLETE", st
        assert st["verdict"] == "PASS", (c.fault, st)
        v = json.loads(rt.paths.cell(c.cell_key).validation.read_text())
        assert v["atomicity_evidence"] is True


def test_concurrent_duplicate_delivery_is_exactly_once(rt):
    """Two writers deliver the SAME publication at one barrier-synchronised instant. Functional idempotency
    test using threads of one cluster: NOT evidence of independent-job concurrency (contention cells, launched
    as separate job runs by the `dbxbench_suite` job, provide that and race NEW publications on the fence)."""
    from dbxbench.config import OrderingMode, Strategy
    from dbxbench.engine import PublicationStatus
    from dbxbench.model import Publication, publication_id, publication_ts
    from dbxbench.telemetry import TelemetrySink

    cell = next(
        c for c in _cells(rt, strategy=Strategy.SCD2_FULL) if c.kind.value == "performance" and c.fmt.value == "delta"
    )
    w = rt.cfg.workload
    nxt = max(p.sequence for p in cell.writers[0].publications) + 1
    res = {}
    gate = threading.Barrier(2)

    def go(wid, seq):
        eng = rt.engine(cell, wid, TelemetrySink(None))
        eng.ordering = OrderingMode.SEQUENCED
        p = Publication(
            "scope_0001",
            publication_id("scope_0001", seq),
            seq,
            publication_ts(w.publication_timestamp_origin_utc, w.publication_interval_millis, seq),
            tuple(range(cell.table_count)),
            cell.writers[0].publications[0].expected_rows,
            cell.profile_key,
        )
        gate.wait()
        res[wid] = eng.publish(p)

    # the publication is already committed => both must end ALREADY_COMMITTED and the ledger keeps 1 row
    seq = nxt - 1
    t = [threading.Thread(target=go, args=(f"race{i}", seq)) for i in range(2)]
    for x in t:
        x.start()
    for x in t:
        x.join()
    assert {r.status for r in res.values()} == {PublicationStatus.ALREADY_COMMITTED}
    n = rt.executor.execute(
        f"SELECT count(*) AS n FROM {rt.tables_for(cell).ledger.sql} "
        f"WHERE publication_id = '{publication_id('scope_0001', seq)}'"
    )[0]["n"]
    assert n == 1
