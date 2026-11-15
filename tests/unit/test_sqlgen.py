"""Unit tests for the generated publication SQL (the exact text sent to Databricks).

They pin the shape the correctness argument relies on: one block, duplicate check before the fence, fence
before any data change, the ledger insert last, one shared timestamp, append-only never updating data tables,
and fault points placed inside the block.
"""

from __future__ import annotations

import re

import pytest

from dbxbench.config import FaultScenario, OrderingMode, Strategy, TableFormat
from dbxbench.model import Publication, publication_id, publication_ts
from dbxbench.naming import Namespace, cell_tables, staging_table
from dbxbench.sqlgen import (
    DATABRICKS,
    LOCAL_EMULATION,
    TxnContext,
    build_publication_sql,
    create_table_sql,
)


def ctx(cfg, strategy=Strategy.SCD2_FULL, ordering=OrderingMode.SEQUENCED, fault=None, n=3):
    ns = Namespace.for_run(cfg, "r20261114t220000_abcdef")
    tables = cell_tables(ns, "c001_x", n)
    stg = tuple(staging_table(ns, "d00", i) for i in range(n))
    return TxnContext(
        "r20261114t220000_abcdef", "c001_x", strategy, ordering, "w000", "w000-tok", 1, tables, stg, fault
    )


def pub(seq=2, tables=(0, 1, 2)):
    return Publication(
        "scope_0001",
        publication_id("scope_0001", seq),
        seq,
        publication_ts("2026-01-01T00:00:00Z", 1000, seq),
        tables,
        10,
        "d00",
    )


def stmts(sql):
    return [ln.strip() for ln in sql.splitlines()[1:-1]]


def test_single_block_scd2_full(example_cfg):
    """SCD2: one block; duplicate check -> fence -> close active rows + insert per table -> ledger last."""
    sql = build_publication_sql(ctx(example_cfg), pub(), DATABRICKS)
    assert sql.startswith("BEGIN ATOMIC\n") and sql.endswith("\nEND;")
    assert sql.count("BEGIN ATOMIC") == 1 and sql.count("END;") == 1
    body = stmts(sql)
    updates = [s for s in body if s.startswith("UPDATE") and "_t0" in s.split(" SET ")[0]]
    inserts = [s for s in body if s.startswith("INSERT INTO") and "_t0" in s.split("(")[0]]
    assert len(updates) == 3 and len(inserts) == 3
    # every UPDATE closes all active rows of the scope with the shared publication timestamp
    ts = "TIMESTAMP'2026-01-01 00:00:02.000000+00:00'"
    for u in updates:
        assert f"`effective_to_date` = {ts}" in u and "`effective_to_date` IS NULL" in u
    for i in inserts:
        assert i.count(ts) == 2 and "CAST(NULL AS TIMESTAMP)" in i
    # ledger insert is the LAST statement, after all data DML
    assert body[-1].startswith("INSERT INTO `dbx_research`") and "_ledger" in body[-1]
    # idempotency check precedes the fence, which precedes any data DML
    idx_dup = next(i for i, s in enumerate(body) if "DUPLICATE_PUBLICATION" in s)
    idx_fence = next(i for i, s in enumerate(body) if s.startswith("UPDATE") and "_fence" in s)
    idx_first_data = next(i for i, s in enumerate(body) if s.startswith("UPDATE") and "_t01" in s)
    assert idx_dup < idx_fence < idx_first_data
    assert "SIGNAL SQLSTATE '45000'" in sql and "raise_error" not in sql


def test_same_timestamp_across_all_tables(example_cfg):
    """Every table in a publication gets the same publication timestamp."""
    sql = build_publication_sql(ctx(example_cfg), pub(), DATABRICKS)
    assert set(re.findall(r"TIMESTAMP'[^']+'", sql)) == {"TIMESTAMP'2026-01-01 00:00:02.000000+00:00'"}


def test_append_only_never_updates_data_tables(example_cfg):
    """Append-only only INSERTs into data tables (the fence row is the only UPDATE)."""
    sql = build_publication_sql(ctx(example_cfg, Strategy.APPEND_ONLY_FULL), pub(), DATABRICKS)
    body = stmts(sql)
    data_updates = [
        s for s in body if s.startswith(("UPDATE", "MERGE", "DELETE")) and "_fence" not in s.split(" SET ")[0]
    ]
    assert data_updates == []
    assert "effective_to_date" not in sql
    assert sum(1 for s in body if s.startswith("INSERT INTO") and "_t0" in s.split("(")[0]) == 3


def test_ordering_modes(example_cfg):
    """off / monotonic / sequenced generate the intended fence checks; all keep the duplicate check."""
    off = build_publication_sql(ctx(example_cfg, ordering=OrderingMode.OFF), pub(), DATABRICKS)
    mono = build_publication_sql(ctx(example_cfg, ordering=OrderingMode.MONOTONIC), pub(), DATABRICKS)
    seqd = build_publication_sql(ctx(example_cfg, ordering=OrderingMode.SEQUENCED), pub(), DATABRICKS)
    assert "STALE_SEQUENCE" not in off and "SEQUENCE_NOT_READY" not in off
    assert "greatest(`last_sequence`, 2)" in off  # still updates the fence row (idempotency serialisation)
    assert "STALE_SEQUENCE" in mono and "SEQUENCE_NOT_READY" not in mono
    assert "STALE_SEQUENCE" in seqd and "IF v_last_seq < 2 - 1 THEN" in seqd
    for s in (off, mono, seqd):
        assert "DUPLICATE_PUBLICATION" in s


@pytest.mark.parametrize(
    ("fault", "after"),
    [
        (FaultScenario.AFTER_FIRST_UPDATE, "UPDATE `dbx_research`.`scd2_bench_r20261114t220000_abcdef`.`c001_x_t01`"),
        (
            FaultScenario.AFTER_FIRST_INSERT,
            "INSERT INTO `dbx_research`.`scd2_bench_r20261114t220000_abcdef`.`c001_x_t01`",
        ),
        (FaultScenario.LAST_TABLE, "INSERT INTO `dbx_research`.`scd2_bench_r20261114t220000_abcdef`.`c001_x_t03`"),
        (FaultScenario.AFTER_LEDGER, "INSERT INTO `dbx_research`.`scd2_bench_r20261114t220000_abcdef`.`c001_x_ledger`"),
    ],
)
def test_fault_is_inside_block_right_after_target_statement(example_cfg, fault, after):
    """Each injected fault sits inside the block, right after the statement it names."""
    body = stmts(build_publication_sql(ctx(example_cfg, fault=fault), pub(), DATABRICKS))
    i = next(i for i, s in enumerate(body) if "INJECTED_FAULT" in s)
    assert body[i - 1].startswith(after)
    assert sum("INJECTED_FAULT" in s for s in body) == 1


def test_table_subset_only_touches_listed_tables(example_cfg):
    """A publication over a subset of tables writes only those tables; unknown table indexes are refused."""
    sql = build_publication_sql(ctx(example_cfg), pub(tables=(1,)), DATABRICKS)
    assert "`c001_x_t02`" in sql and "`c001_x_t01`" not in sql and "`c001_x_t03`" not in sql
    assert "array('c001_x_t02')" in sql
    with pytest.raises(ValueError):
        build_publication_sql(ctx(example_cfg), pub(tables=(5,)), DATABRICKS)


def test_local_dialect_is_labelled_nonatomic(example_cfg):
    """The local dialect is plain BEGIN...END (no ATOMIC) and is flagged non-atomic."""
    sql = build_publication_sql(ctx(example_cfg), pub(), LOCAL_EMULATION)
    assert sql.startswith("BEGIN\n") and "ATOMIC" not in sql and "SIGNAL" not in sql
    assert "raise_error('DBXBENCH_" in sql
    assert LOCAL_EMULATION.atomic is False and DATABRICKS.atomic is True


def test_changed_only_uses_merge_and_is_separate(example_cfg):
    """The optional changed-only strategy uses MERGE with NOT MATCHED BY SOURCE."""
    sql = build_publication_sql(ctx(example_cfg, Strategy.SCD2_CHANGED_ONLY), pub(), DATABRICKS)
    assert "MERGE INTO" in sql and "NOT MATCHED BY SOURCE" in sql and "LEFT ANTI JOIN" in sql


def test_ddl_never_overwrites(example_cfg):
    """CREATE TABLE never uses IF NOT EXISTS or REPLACE, and always sets the ownership marker."""
    ns = Namespace.for_run(example_cfg, "r20261114t220000_abcdef")
    ddl = create_table_sql(ns.table("x"), [("a", "STRING")], TableFormat.ICEBERG, {}, "r20261114t220000_abcdef")
    assert ddl.startswith("CREATE TABLE `") and "IF NOT EXISTS" not in ddl and "REPLACE" not in ddl
    assert "USING ICEBERG" in ddl and "'dbxbench.owner' = 'dbxbench'" in ddl
