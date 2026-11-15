"""Unit tests for the post-DDL gate: every table a transaction writes must be a Unity Catalog MANAGED table and
(for Delta) carry the ``catalogManaged`` feature that multi-table transactions require.
"""

from __future__ import annotations

import pytest

from dbxbench.executor import RecordingExecutor
from dbxbench.runner import RunError, Runtime, _assert_transactional_tables
from dbxbench.scenarios import build_plan


def _rt(example_cfg, tmp_path, features, ttype="MANAGED"):
    cfg = example_cfg.model_copy(
        update={"project": example_cfg.project.model_copy(update={"results_root": str(tmp_path)})}
    )
    rt = Runtime(cfg, "r20261114t220000_abcdef")

    def r(sql):
        if sql.startswith("DESCRIBE DETAIL"):
            return [{"format": "delta", "tableFeatures": features, "properties": {}}]
        if sql.startswith("DESCRIBE TABLE EXTENDED"):
            return [{"col_name": "Type", "data_type": ttype}]
        return []

    rt._executor = RecordingExecutor(r)
    cell = next(c for c in build_plan(cfg, rt.run_id).cells if c.fmt.value == "delta")
    return rt, cell


def test_gate_accepts_managed_catalog_managed(example_cfg, tmp_path):
    """A MANAGED Delta table with catalogManaged passes the gate."""
    rt, cell = _rt(example_cfg, tmp_path, ["deletionVectors", "catalogManaged"])
    _assert_transactional_tables(rt, cell, rt.tables_for(cell))


def test_gate_rejects_missing_catalog_commits(example_cfg, tmp_path):
    """A Delta table without catalogManaged is rejected before any transaction runs."""
    rt, cell = _rt(example_cfg, tmp_path, ["deletionVectors"])
    with pytest.raises(RunError, match="catalogManaged"):
        _assert_transactional_tables(rt, cell, rt.tables_for(cell))


def test_gate_rejects_external_tables(example_cfg, tmp_path):
    """An EXTERNAL table is rejected: transactions require Unity Catalog managed tables."""
    rt, cell = _rt(example_cfg, tmp_path, ["catalogManaged"], ttype="EXTERNAL")
    with pytest.raises(RunError, match="MANAGED"):
        _assert_transactional_tables(rt, cell, rt.tables_for(cell))
