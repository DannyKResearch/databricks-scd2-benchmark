"""Unit tests for the publication engine (retry, idempotency, reconciliation) against ``tests.fakes.FakeStore``.

The fake models the ledger, the in-transaction duplicate check, OCC conflicts and lost replies, so every
outcome the engine can report is exercised without Spark. The central rules under test:

* a publication is ONE ``BEGIN ATOMIC`` block sent in ONE call, and a retry resends the WHOLE block;
* only documented conflicts and ordering waits are retried;
* any unclear outcome (lost reply, client timeout, stale after our own abandoned attempt) is settled by
  reading the ledger, never by guessing, so nothing is applied twice.
"""

from __future__ import annotations

import pytest
from tests.fakes import FakeStore

from dbxbench.config import FaultScenario, OrderingMode, Strategy
from dbxbench.engine import EngineFaults, PublicationEngine, PublicationStatus
from dbxbench.executor import RecordingExecutor
from dbxbench.model import Publication, publication_id, publication_ts
from dbxbench.naming import Namespace, cell_tables, staging_table
from dbxbench.retry import RetryPolicy
from dbxbench.sqlgen import DATABRICKS
from dbxbench.telemetry import TelemetrySink

ORIGIN = "2026-01-01T00:00:00Z"


def make(example_cfg, store, max_attempts=5, deadline=30.0, attempt_timeout=10.0, ordering=OrderingMode.SEQUENCED):
    ns = Namespace.for_run(example_cfg, "r20261114t220000_abcdef")
    x = RecordingExecutor(store)
    eng = PublicationEngine(
        executor=x,
        dialect=DATABRICKS,
        run_id="r20261114t220000_abcdef",
        cell_key="c001_x",
        strategy=Strategy.SCD2_FULL,
        ordering=ordering,
        writer_id="w000",
        tables=cell_tables(ns, "c001_x", 2),
        staging=tuple(staging_table(ns, "d00", i) for i in range(2)),
        policy=RetryPolicy(max_attempts, 1, 5, deadline, attempt_timeout, seed=1),
        telemetry=TelemetrySink(None),
        expected_ts=lambda q: publication_ts(ORIGIN, 1000, q),
    )
    return eng, x


def pub(seq=2):
    return Publication(
        "scope_0001", publication_id("scope_0001", seq), seq, publication_ts(ORIGIN, 1000, seq), (0, 1), 10, "d00"
    )


def blocks(x):
    return [s for s in x.statements if s.startswith("BEGIN")]


def test_commit_is_one_spark_sql_call_with_whole_block(example_cfg):
    """A clean commit is one BEGIN ATOMIC block containing all table writes and the ledger insert."""
    store = FakeStore()
    eng, x = make(example_cfg, store)
    res = eng.publish(pub())
    assert res.status is PublicationStatus.COMMITTED
    b = blocks(x)
    assert len(b) == 1 and b[0].startswith("BEGIN ATOMIC") and b[0].endswith("END;")
    assert b[0].count("INSERT INTO") == 3 and "_ledger" in b[0]  # 2 tables + ledger in the SAME call
    assert res.committed_token in store.ledger[pub().publication_id]


def test_conflict_retries_entire_block_then_commits(example_cfg):
    """Conflicts are retried by resending the whole block with a new attempt token, never a partial statement."""
    store = FakeStore(behaviors=["conflict", "conflict", "commit"])
    eng, x = make(example_cfg, store)
    res = eng.publish(pub())
    assert res.status is PublicationStatus.COMMITTED and len(res.attempts) == 3
    assert [a.category for a in res.attempts] == ["RETRYABLE_CONFLICT", "RETRYABLE_CONFLICT", None]
    b = blocks(x)
    assert len(b) == 3 and all(s.startswith("BEGIN ATOMIC") for s in b)  # never a partial statement retry
    assert len({a.attempt_token for a in res.attempts}) == 3


def test_attempt_cap(example_cfg):
    """Retries stop at max_attempts and the publication is reported as exhausted."""
    eng, _ = make(example_cfg, FakeStore(behaviors=["conflict"] * 10), max_attempts=3)
    res = eng.publish(pub())
    assert res.status is PublicationStatus.FAILED_RETRIES_EXHAUSTED and len(res.attempts) == 3


@pytest.mark.parametrize("beh", ["nonretryable", "unclassified"])
def test_nonretryable_is_not_retried(example_cfg, beh):
    """Non-retryable and unclassified errors fail after one attempt."""
    eng, x = make(example_cfg, FakeStore(behaviors=[beh]))
    res = eng.publish(pub())
    assert res.status is PublicationStatus.FAILED_NONRETRYABLE and len(blocks(x)) == 1


def test_lost_reply_after_commit_is_reconciled_not_reapplied(example_cfg):
    """The commit succeeded but the reply was lost: the ledger shows it committed, so it is not applied again."""
    store = FakeStore(behaviors=["commit_then_lose_reply"])
    eng, x = make(example_cfg, store)
    res = eng.publish(pub())
    assert res.status is PublicationStatus.COMMITTED_RECONCILED
    assert len(blocks(x)) == 1 and len(store.ledger[pub().publication_id]) == 1
    assert res.attempts[0].reconciled


def test_simulated_lost_ack_fault(example_cfg):
    """The injected lost-acknowledgement fault is reconciled from the ledger in the same way."""
    store = FakeStore()
    eng, x = make(example_cfg, store)
    res = eng.publish(pub(), EngineFaults(lost_ack_first_attempt=True))
    assert res.status is PublicationStatus.COMMITTED_RECONCILED and len(blocks(x)) == 1
    assert res.attempts[0].simulated_fault == "lost_ack"


def test_unknown_outcome_not_committed_is_retried(example_cfg):
    """Reply lost and the ledger shows no commit: the block is safely retried and applied once."""
    store = FakeStore(behaviors=["lost_not_committed", "commit"])
    eng, x = make(example_cfg, store)
    res = eng.publish(pub())
    assert res.status is PublicationStatus.COMMITTED and len(blocks(x)) == 2
    assert len(store.ledger[pub().publication_id]) == 1


def test_duplicate_delivery_is_idempotent(example_cfg):
    """Delivering an already committed publication again reports ALREADY_COMMITTED and writes nothing."""
    store = FakeStore()
    eng, _ = make(example_cfg, store)
    assert eng.publish(pub()).status is PublicationStatus.COMMITTED
    eng2, _ = make(example_cfg, store)
    res = eng2.publish(pub())
    assert res.status is PublicationStatus.ALREADY_COMMITTED
    assert len(store.ledger[pub().publication_id]) == 1


def test_injected_fault_reports_rollback_only_after_ledger_check(example_cfg):
    """After an injected in-transaction failure, rollback is confirmed from the ledger, not assumed."""
    store = FakeStore()
    eng, x = make(example_cfg, store)
    res = eng.publish(pub(), EngineFaults(in_txn=FaultScenario.AFTER_FIRST_UPDATE))
    assert res.status is PublicationStatus.ROLLED_BACK_INJECTED
    assert any(s.startswith("SELECT attempt_token") for s in x.statements)  # reconciled, not inferred
    assert pub().publication_id not in store.ledger


def test_stale_is_terminal(example_cfg):
    """A publication older than the fence (STALE) is rejected without retrying."""
    eng, x = make(example_cfg, FakeStore(behaviors=["stale"]))
    assert eng.publish(pub()).status is PublicationStatus.REJECTED_STALE and len(blocks(x)) == 1


def test_ordering_wait_retries_without_consuming_conflict_budget(example_cfg):
    """Waiting for an earlier sequence (sequenced mode) does not use up the conflict retry budget."""
    eng, _ = make(example_cfg, FakeStore(behaviors=["not_ready"] * 4 + ["commit"]), max_attempts=2)
    res = eng.publish(pub())
    assert res.status is PublicationStatus.COMMITTED and len(res.attempts) == 5


def test_prevalidation_failure_submits_nothing(example_cfg):
    """Duplicate keys in staging are caught before any transaction is sent."""
    eng, x = make(example_cfg, FakeStore(staged_rows=10, staged_distinct=8))
    res = eng.publish(pub())
    assert res.status is PublicationStatus.PREVALIDATION_FAILED
    assert "duplicate" in (res.detail or "")
    assert blocks(x) == []


def test_prevalidation_rejects_wrong_timestamp(example_cfg):
    """A publication timestamp that does not match its sequence is caught before any transaction is sent."""
    eng, x = make(example_cfg, FakeStore())
    bad = Publication(
        "scope_0001", publication_id("scope_0001", 2), 2, publication_ts(ORIGIN, 1000, 3), (0, 1), 10, "d00"
    )
    assert eng.publish(bad).status is PublicationStatus.PREVALIDATION_FAILED and blocks(x) == []


def test_client_timeout_reconciles_then_reports_late_commit(example_cfg):
    """The client stops waiting but the transaction commits later: the publication ends committed exactly once."""
    store = FakeStore(behaviors=["sleep:0.3"])
    eng, _x = make(example_cfg, store)
    res = eng.publish(pub(), EngineFaults(client_timeout_first_attempt_s=0.05))
    eng.close(wait=True)
    # attempt 1 timed out client-side; attempt 2 either saw the late commit as DUPLICATE or committed itself
    assert res.status in (
        PublicationStatus.COMMITTED,
        PublicationStatus.COMMITTED_RECONCILED,
        PublicationStatus.ALREADY_COMMITTED,
    )
    assert len(store.ledger[pub().publication_id]) == 1
    assert res.attempts[0].category == "UNKNOWN_OUTCOME"


def test_deadline(example_cfg):
    """An overall deadline stops endless ordering waits."""
    eng, _ = make(example_cfg, FakeStore(behaviors=["not_ready"] * 1000), deadline=0.2, attempt_timeout=0.1)
    res = eng.publish(pub())
    assert res.status in (PublicationStatus.DEADLINE_EXCEEDED, PublicationStatus.FAILED_RETRIES_EXHAUSTED)


def test_stale_after_own_abandoned_attempt_committed_is_reconciled(example_cfg):
    """Attempt 1 times out client-side but commits later; attempt 2 sees the advanced fence (STALE).
    The engine must consult the ledger and report the publication as committed, not stale."""
    store = FakeStore(behaviors=["sleep:0.3", "stale_first"])
    eng, _ = make(example_cfg, store)

    real = store.__call__

    def responder(sql):
        if sql.startswith("BEGIN") and len(store.blocks) == 1:
            import time as _t

            _t.sleep(0.4)  # make sure the abandoned attempt 1 has committed before attempt 2 is evaluated
        return real(sql)

    eng.executor.responder = responder
    res = eng.publish(pub(), EngineFaults(client_timeout_first_attempt_s=0.05))
    eng.close(wait=True)
    assert res.status is PublicationStatus.COMMITTED_RECONCILED
    assert len(store.ledger[pub().publication_id]) == 1


def test_exhausted_after_unknown_outcome_rechecks_ledger(example_cfg):
    """attempt 1 times out but commits late; attempts 2..n conflict until the cap. The final ledger check must
    report the publication as committed instead of FAILED_RETRIES_EXHAUSTED."""
    store = FakeStore(behaviors=["sleep:0.3", "conflict_first", "conflict_first"])
    eng, _ = make(example_cfg, store, max_attempts=2)
    res = eng.publish(pub(), EngineFaults(client_timeout_first_attempt_s=0.05))
    eng.close(wait=True)
    assert res.status is PublicationStatus.COMMITTED_RECONCILED, res.detail
    assert "superseded by final ledger check" in (res.detail or "")


def test_stale_waits_for_abandoned_attempt_before_final_verdict(example_cfg):
    """Abandoned attempt still running when the retry reports STALE: the engine waits for it, then the
    ledger decides (here: the abandoned attempt committed => COMMITTED_RECONCILED, not REJECTED_STALE)."""
    store = FakeStore(behaviors=["sleep:0.5", "stale_first"])
    eng, _ = make(example_cfg, store)
    res = eng.publish(pub(), EngineFaults(client_timeout_first_attempt_s=0.05))
    assert res.status is PublicationStatus.COMMITTED_RECONCILED, (res.status, res.detail)
    eng.close(wait=True)
