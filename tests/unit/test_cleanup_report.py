"""Unit tests for the destructive and the reporting ends of a run.

Cleanup must refuse unless every safety condition holds (opt-in config, ``--yes``, one exact run id, ownership
markers on every schema and table) and must only ever drop the run's own schemas. The report must never invent
data: cells that did not run are shown as NOT_RUN, with no charts.
"""

from __future__ import annotations

import json

import pytest
import yaml
from tests.conftest import EXAMPLE

from dbxbench.cleanup import CleanupRefused, plan_cleanup
from dbxbench.config import BenchConfig
from dbxbench.executor import RecordingExecutor
from dbxbench.results import RunPaths, set_status, write_json

RID = "r20261114t220000_abcdef"


def cfg(tmp_path, **project):
    raw = yaml.safe_load(EXAMPLE.read_text())
    raw["project"].update({"results_root": str(tmp_path), "allow_cleanup": True, **project})
    return BenchConfig.model_validate(raw)


def owned_responder(owned_tables=True, owned_schema=True):
    def r(sql):
        if sql.startswith("DESCRIBE SCHEMA EXTENDED"):
            props = "((dbxbench.owner,dbxbench), (dbxbench.run_id,%s))" % (RID if owned_schema else "other")
            return [{"info_name": "Properties", "info_value": props}]
        if sql.startswith("SHOW TABLES"):
            return [{"tableName": "c001_t01", "isTemporary": False}, {"tableName": "c001_ledger", "isTemporary": False}]
        if sql.startswith("SHOW TBLPROPERTIES"):
            rows = [{"key": "dbxbench.owner", "value": "dbxbench"}]
            if owned_tables or "ledger" in sql:
                rows.append({"key": "dbxbench.run_id", "value": RID})
            return rows
        return []

    return r


def test_cleanup_happy_path_drops_only_run_schema(tmp_path):
    """With every safety condition met, cleanup drops exactly the run schema and prints a deletion warning."""
    p = plan_cleanup(cfg(tmp_path), RID, RecordingExecutor(owned_responder()), yes=True)
    assert p.statements == [f"DROP SCHEMA `dbx_research`.`scd2_bench_{RID}` CASCADE"]
    assert any("DELETION WARNING" in w for w in p.warnings)


@pytest.mark.parametrize(
    ("kwargs", "project", "match"),
    [
        ({"yes": False}, {}, "--yes"),
        ({"yes": True}, {"allow_cleanup": False}, "allow_cleanup"),
    ],
)
def test_cleanup_refusals(tmp_path, kwargs, project, match):
    """Cleanup refuses without --yes or without allow_cleanup in the config."""
    with pytest.raises(CleanupRefused, match=match):
        plan_cleanup(cfg(tmp_path, **project), RID, RecordingExecutor(owned_responder()), **kwargs)


def test_cleanup_refuses_ambiguous_run_id(tmp_path):
    """Wildcards or an empty run id are refused."""
    with pytest.raises(CleanupRefused, match="wildcards"):
        plan_cleanup(cfg(tmp_path), "r2026*", RecordingExecutor(owned_responder()), yes=True)
    with pytest.raises(CleanupRefused):
        plan_cleanup(cfg(tmp_path), "", RecordingExecutor(owned_responder()), yes=True)


def test_cleanup_refuses_unowned_schema_or_foreign_table(tmp_path):
    """A schema without our marker, or containing a table we do not own, is never dropped."""
    with pytest.raises(CleanupRefused, match="ownership marker"):
        plan_cleanup(cfg(tmp_path), RID, RecordingExecutor(owned_responder(owned_schema=False)), yes=True)
    with pytest.raises(CleanupRefused, match="not owned"):
        plan_cleanup(cfg(tmp_path), RID, RecordingExecutor(owned_responder(owned_tables=False)), yes=True)


def test_cleanup_retains_failure_evidence(tmp_path):
    """Tables of FAILED cells are kept as evidence unless --discard-failure-evidence is given."""
    c = cfg(tmp_path)
    set_status(RunPaths.for_run(tmp_path, RID).cell("c001"), "FAILED", error="x")
    with pytest.raises(CleanupRefused, match="evidence"):
        plan_cleanup(c, RID, RecordingExecutor(owned_responder()), yes=True)
    p = plan_cleanup(c, RID, RecordingExecutor(owned_responder()), yes=True, discard_failure_evidence=True)
    assert p.statements


def test_fixed_schema_mode_drops_only_prefixed_owned_tables(tmp_path):
    """In a shared schema only tables with the run prefix and our marker are dropped, never the schema."""
    c = cfg(tmp_path, namespace_mode="fixed_schema", fixed_schema="shared")

    def r(sql):
        if sql.startswith("SHOW TABLES"):
            return [{"tableName": f"{RID}__c001_t01"}, {"tableName": "someone_elses"}]
        if sql.startswith("SHOW TBLPROPERTIES"):
            return [{"key": "dbxbench.owner", "value": "dbxbench"}, {"key": "dbxbench.run_id", "value": RID}]
        return []

    p = plan_cleanup(c, RID, RecordingExecutor(r), yes=True)
    assert p.statements == [f"DROP TABLE `dbx_research`.`shared`.`{RID}__c001_t01`"]
    assert not any("SCHEMA" in s for s in p.statements)


def test_report_never_invents_data(tmp_path, example_cfg):
    """A run where nothing ran must produce NOT_RUN sections and no charts."""
    from dbxbench.analysis import analyse_run
    from dbxbench.report import build_report
    from dbxbench.scenarios import build_plan

    paths = RunPaths.for_run(tmp_path, RID)
    plan = build_plan(example_cfg, RID)
    write_json(paths.plan, plan.to_json())
    for c in plan.cells:
        set_status(paths.cell(c.cell_key), "SKIPPED", reason="capability X unsupported")
    s = analyse_run(paths)
    assert s["correctness"]["verdict"] == "NOT_RUN"
    assert all(p["status"] == "SKIPPED" for p in s["matched_pairs"])
    text = build_report(paths, charts=True).read_text()
    assert "NOT_RUN: no completed performance cells." in text
    assert "NOT_RUN: no completed contention cells." in text
    assert not paths.charts.exists()
    assert json.loads(paths.summary.read_text())["correctness"]["cells_ran"] == 0


def test_cleanup_per_cell_layout_drops_cell_schemas_then_run_schema(tmp_path):
    """per_cell layout: the run's cell schemas are dropped, then the run schema.

    A run whose id merely extends ours is ignored.
    """
    base = f"scd2_bench_{RID}"
    inner = owned_responder()

    def r(sql):
        if sql.startswith("SHOW SCHEMAS"):
            # a different run whose id extends ours must never be picked up
            return [
                {"databaseName": base},
                {"databaseName": f"{base}__c001_scd2_dlt"},
                {"databaseName": f"{base}_2"},
            ]
        return inner(sql)

    p = plan_cleanup(cfg(tmp_path), RID, RecordingExecutor(r), yes=True)
    assert p.statements == [
        f"DROP SCHEMA `dbx_research`.`{base}__c001_scd2_dlt` CASCADE",
        f"DROP SCHEMA `dbx_research`.`{base}` CASCADE",
    ]


def test_cleanup_requires_exact_run_id_in_marker(tmp_path):
    """The ownership marker must name exactly this run id, not a longer id that contains it."""

    def r(sql):
        if sql.startswith("DESCRIBE SCHEMA EXTENDED"):
            return [{"info_name": "Properties", "info_value": f"((dbxbench.owner,dbxbench), (dbxbench.run_id,{RID}x))"}]
        return []

    with pytest.raises(CleanupRefused, match="ownership marker"):
        plan_cleanup(cfg(tmp_path), RID, RecordingExecutor(r), yes=True)
