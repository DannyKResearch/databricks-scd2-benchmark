"""The Databricks transaction-context rule, reproduced with a fake session.

Databricks binds an open multi-statement transaction to the thread that started it. A statement from any
other thread while it is open fails with TRANSACTION_CONTEXT_MISMATCH and the session stays broken (observed
on serverless, 2026-11-15, in the client_timeout fault cell). These tests make the fake session enforce that
rule and show the engine + SparkSqlExecutor never violate it, even when the client gives up waiting.
"""

from __future__ import annotations

import threading

import pytest
from tests.fakes import FakeStore

from dbxbench.config import OrderingMode, SchemaLayout, Strategy
from dbxbench.engine import EngineFaults, PublicationEngine, PublicationStatus
from dbxbench.errors import Category, classify
from dbxbench.executor import SessionUnusable, SparkSqlExecutor
from dbxbench.model import Publication, publication_id, publication_ts
from dbxbench.naming import Namespace, cell_tables, staging_table
from dbxbench.retry import RetryPolicy
from dbxbench.sqlgen import DATABRICKS
from dbxbench.telemetry import TelemetrySink

ORIGIN = "2026-01-01T00:00:00Z"
RID = "r20261114t220000_abcdef"


class _Row(dict):
    def asDict(self, recursive: bool = False):
        return dict(self)


class _DF:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return [_Row(r) for r in self._rows]


class ThreadBoundSession:
    """Fake Spark session that enforces 'an open transaction belongs to its thread'."""

    def __init__(self, store):
        self.store = store
        self._txn_thread: int | None = None
        self._lock = threading.Lock()
        self.threads: set[int] = set()
        self.violations = 0

    def sql(self, text):
        me = threading.get_ident()
        self.threads.add(me)
        with self._lock:
            if self._txn_thread is not None and self._txn_thread != me:
                self.violations += 1
                raise RuntimeError(
                    "[TRANSACTION_CONTEXT_MISMATCH.ACTIVE_IN_SPARK_SESSION_BUT_NOT_IN_THREAD] transaction active"
                )
            is_txn = text.startswith("BEGIN")
            if is_txn:
                self._txn_thread = me
        try:
            return _DF(self.store(text))
        finally:
            if is_txn:
                with self._lock:
                    self._txn_thread = None


def engine(example_cfg, store, session):
    ns = Namespace.for_run(example_cfg, RID)
    x = SparkSqlExecutor(session)
    eng = PublicationEngine(
        executor=x,
        dialect=DATABRICKS,
        run_id=RID,
        cell_key="c001_x",
        strategy=Strategy.SCD2_FULL,
        ordering=OrderingMode.SEQUENCED,
        writer_id="w000",
        tables=cell_tables(ns, "c001_x", 2),
        staging=tuple(staging_table(ns, "d00", i) for i in range(2)),
        policy=RetryPolicy(5, 1, 5, 30.0, 10.0, seed=1),
        telemetry=TelemetrySink(None),
        expected_ts=lambda q: publication_ts(ORIGIN, 1000, q),
    )
    return eng, x


def pub(seq=2):
    return Publication(
        "scope_0001", publication_id("scope_0001", seq), seq, publication_ts(ORIGIN, 1000, seq), (0, 1), 10, "d00"
    )


def test_client_timeout_never_overlaps_the_open_transaction(example_cfg):
    """The client gives up after 0.05 s; reconciliation still waits for the open transaction on the same thread."""
    store = FakeStore(behaviors=["sleep:0.4"])  # the transaction outlives the 0.05 s client timeout
    session = ThreadBoundSession(store)
    eng, x = engine(example_cfg, store, session)
    res = eng.publish(pub(), EngineFaults(client_timeout_first_attempt_s=0.05))
    eng.close(wait=True)
    assert session.violations == 0
    assert len(session.threads) == 1  # every statement ran on the one session thread
    assert res.status in (PublicationStatus.COMMITTED_RECONCILED, PublicationStatus.ALREADY_COMMITTED)
    assert len(store.ledger[pub().publication_id]) == 1  # applied exactly once
    assert x.poisoned is None


def test_client_timeout_does_not_interrupt_by_default(example_cfg):
    """A client timeout must not interrupt the server statement unless asked to."""
    store = FakeStore(behaviors=["sleep:0.2"])
    session = ThreadBoundSession(store)
    session.interruptTag = lambda tag: pytest.fail("client timeout must not interrupt the transaction")  # type: ignore[attr-defined]
    eng, _ = engine(example_cfg, store, session)
    eng.publish(pub(), EngineFaults(client_timeout_first_attempt_s=0.05))
    eng.close(wait=True)


def test_broken_transaction_context_poisons_the_executor():
    """After TRANSACTION_CONTEXT_MISMATCH, later calls fail fast as SESSION_UNUSABLE with the original cause."""

    class Broken:
        def sql(self, text):
            raise RuntimeError("[TRANSACTION_CONTEXT_MISMATCH.ACTIVE_IN_SPARK_SESSION_BUT_NOT_IN_THREAD] x")

    x = SparkSqlExecutor(Broken())
    with pytest.raises(RuntimeError) as first:
        x.execute("SELECT 1")
    assert classify(first.value).category is Category.SESSION_UNUSABLE
    with pytest.raises(SessionUnusable, match="SESSION_UNUSABLE"):
        x.execute("SELECT 2")  # fails fast with the original cause instead of a misleading per-cell error


def test_stalled_statement_poisons_the_executor():
    """A statement that never returns marks the session unusable instead of hanging every later call."""
    gate = threading.Event()

    class Hang:
        def sql(self, text):
            gate.wait(5)
            return _DF([])

    x = SparkSqlExecutor(Hang(), stall_timeout_s=0.05)
    with pytest.raises(SessionUnusable):
        x.execute("SELECT 1")
    gate.set()
    with pytest.raises(SessionUnusable):
        x.execute("SELECT 2")


def test_per_cell_layout_names(example_cfg):
    """per_cell layout puts each cell's tables in <run schema>__<cell>; staging stays in the run schema."""
    cfg = example_cfg.model_copy(
        update={"project": example_cfg.project.model_copy(update={"schema_layout": SchemaLayout.PER_CELL})}
    )
    ns = Namespace.for_run(cfg, RID)
    cell_ns = ns.for_cell("c001_scd2_dlt")
    assert cell_ns.schema == f"scd2_bench_{RID}__c001_scd2_dlt"
    t = cell_tables(cell_ns, "c001_scd2_dlt", 2)
    assert t.ledger.plain.endswith(f"scd2_bench_{RID}__c001_scd2_dlt.c001_scd2_dlt_ledger")
    assert staging_table(ns, "d00", 0).plain.split(".")[1] == f"scd2_bench_{RID}"  # staging stays in the run schema
    assert Namespace.for_run(example_cfg, RID).for_cell("c001_scd2_dlt").schema == f"scd2_bench_{RID}"
