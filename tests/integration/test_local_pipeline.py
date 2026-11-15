"""End-to-end local pipeline (OSS Spark + Delta, local_nonatomic_emulation).

What this proves locally: SQL shape executes, SCD2/append semantics, validators, parity, fault
expectations under NON-atomic emulation (leaks must be DETECTED), and the multi-process harness
(barrier, telemetry, ordering analysis). What it can NOT prove: atomicity or Databricks behaviour.
"""

from __future__ import annotations

import json

import pytest
from tests.conftest import LOCAL

from dbxbench.config import load_config

pytestmark = [pytest.mark.spark, pytest.mark.slow]


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    pytest.importorskip("pyspark")
    import os
    from pathlib import Path

    from dbxbench import runner as R

    keep = os.environ.get("DBXBENCH_TEST_RESULTS")  # set to keep the evidence of this run
    root = Path(keep) if keep else tmp_path_factory.mktemp("runs")
    cfg = load_config(
        LOCAL,
        {
            "results_root": str(root),
            "publications": 2,
            "scopes": 1,
            "rows": 50,
            "tables": 2,
            "writers": [3],
            "contention": ["same_scope"],
        },
    )
    rt = R.Runtime(cfg, "r20261114t000000_pytest")
    R.doctor(rt)
    R.init(rt)
    R.generate(rt)
    statuses = R.run(rt)
    verification = R.verify(rt)
    rt.spark.stop()
    return rt, statuses, verification


def status(rt, key):
    return json.loads(rt.paths.cell(key).status.read_text())


def test_iceberg_cells_skipped_with_reason(run):
    """OSS Spark has no managed Iceberg.

    Those cells are SKIPPED with the failing probe as the reason; they are never run on Delta instead.
    """
    rt, statuses, _ = run
    plan = rt.load_plan()
    ice = [c for c in plan.cells if c.fmt.value == "iceberg"]
    assert ice and all(statuses[c.cell_key] == "SKIPPED" for c in ice)
    assert all("managed_iceberg_create" in status(rt, c.cell_key)["reason"] for c in ice)


def test_performance_cells_pass_and_parity(run):
    """SCD2 and append-only cells pass every invariant and hold identical content per publication."""
    rt, _statuses, verification = run
    perf = [c for c in rt.load_plan().cells if c.kind.value == "performance" and c.fmt.value == "delta"]
    for c in perf:
        st = status(rt, c.cell_key)
        assert st["status"] == "COMPLETE" and st["verdict"] == "PASS", st
    assert verification["parity"] and all(p["status"] == "PASS" for p in verification["parity"])


def test_fault_cells_meet_emulation_expectations(run):
    """Without atomicity, mid-transaction faults must leak AND be detected; idempotency faults must pass."""
    rt, _, _ = run
    for c in rt.load_plan().cells:
        if c.kind.value != "fault":
            continue
        st = status(rt, c.cell_key)
        assert st["status"] == "COMPLETE", st
        v = json.loads(rt.paths.cell(c.cell_key).validation.read_text())
        assert v["atomicity_evidence"] is False
        if c.fault.value in {
            "after_first_update",
            "after_first_insert",
            "last_table",
            "after_ledger",
            "schema_mismatch",
            "duplicate_keys_input",
            "constraint_violation_real",
        }:
            assert st["verdict"] == "EMULATION_LEAK_DETECTED", (c.fault, st)
        else:
            assert st["verdict"] == "PASS_EMULATION", (c.fault, st, v["steps"])


def test_contention_cells_ran_as_independent_processes(run):
    """Contention cells really ran 3 separate writer processes, and the ordering analysis saw no harness errors."""
    from dbxbench.analysis import analyse_cell

    rt, _, _ = run
    for c in rt.load_plan().cells:
        if c.kind.value != "contention":
            continue
        a = analyse_cell(rt.paths.cell(c.cell_key).root, c.to_json())
        o = a["ordering_analysis"]
        assert o["distinct_writer_processes"] == 3
        assert not o["harness_events"], o["harness_events"]
        st = status(rt, c.cell_key)
        v = json.loads(rt.paths.cell(c.cell_key).validation.read_text())
        checks = {(x["name"], x["table"]): x["status"] for x in v["checks"]}
        # Holds even WITHOUT atomicity: every publication id is unique per writer and the ledger insert is the
        # last statement, so the ledger can never hold a publication twice.
        assert checks[("ledger_exactly_once", None)] == "PASS"
        # Everything stronger (fence/ledger agreement, contiguous history, commit order == sequence order in
        # `sequenced` mode) depends on the fence and the data becoming visible ATOMICALLY. Under NON-atomic
        # emulation writers can interleave between statements, so those may fail - but any failure must be
        # DETECTED and labelled, never reported as PASS. The guarantees themselves are tested live in
        # tests/databricks and by the contention cells of the dbxbench_suite job.
        assert st["verdict"] in ("PASS", "PASS_WITH_FLAGGED_ORDERING_ANOMALIES", "EMULATION_LEAK_DETECTED")
        if v["verdict"] == "FAIL":
            assert st["verdict"] == "EMULATION_LEAK_DETECTED"
