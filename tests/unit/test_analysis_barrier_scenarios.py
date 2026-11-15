"""Unit tests for the pure helpers behind the concurrency experiments.

* statistics used in reports (percentiles, Kendall tau-b, inversions, maximum in-flight overlap);
* retry backoff (bounded, full jitter, reproducible from a seed);
* the file barrier that starts independent writers together (and fails closed if one is missing);
* the experiment plan (deterministic, correct sequence assignment, shared data profiles, budget guard).
"""

from __future__ import annotations

import threading
import time

import pytest
from tests.conftest import EXAMPLE

from dbxbench.analysis import inversions, kendall_tau_b, latency_summary, max_overlap, percentile
from dbxbench.barrier import BarrierAborted, BarrierTimeout, FileBarrier
from dbxbench.config import ContentionMode, load_config
from dbxbench.retry import RetryPolicy, stable_seed
from dbxbench.scenarios import BudgetExceeded, CellKind, Plan, build_plan


def test_percentile_and_summary():
    """Percentiles interpolate, handle a single value and empty input, and summaries skip missing values."""
    assert percentile([1, 2, 3, 4], 50) == 2.5
    assert percentile([5], 99) == 5
    assert percentile([], 50) is None
    s = latency_summary([1.0, None, 3.0])
    assert s["n"] == 2 and s["p50"] == 2.0


def test_kendall_and_inversions():
    """Kendall tau-b is +1/-1 for identical/reversed orders and undefined for one item; inversions are counted."""
    assert kendall_tau_b([1, 2, 3], [1, 2, 3]) == pytest.approx(1.0)
    assert kendall_tau_b([1, 2, 3], [3, 2, 1]) == pytest.approx(-1.0)
    assert inversions([1, 2, 3], [1, 3, 2]) == 1
    assert kendall_tau_b([1], [1]) is None


def test_max_overlap():
    """Maximum number of simultaneously open intervals (used as evidence that attempts really overlapped)."""
    assert max_overlap([(0, 10), (1, 2), (3, 4), (5, 11)]) == 2
    assert max_overlap([(0, 1), (2, 3)]) == 1


def test_backoff_is_bounded_full_jitter_and_reproducible():
    """Same seed => same backoff sequence; every delay stays within the cap."""
    a = RetryPolicy(10, 100, 1000, 60, 10, seed=7)
    b = RetryPolicy(10, 100, 1000, 60, 10, seed=7)
    xs = [a.backoff_seconds(i) for i in range(1, 10)]
    assert xs == [b.backoff_seconds(i) for i in range(1, 10)]
    assert all(0 <= x <= 1.0 for x in xs)
    assert stable_seed(1, "c", "w") == stable_seed(1, "c", "w")


def test_barrier_go(tmp_path):
    """All writers that marked ready receive the same go time from the coordinator."""
    b = FileBarrier(tmp_path, poll_seconds=0.01)
    got = {}

    def writer(w):
        b.mark_ready(w)
        got[w] = b.wait_go(5)

    ts = [threading.Thread(target=writer, args=(f"w{i}",)) for i in range(3)]
    for t in ts:
        t.start()
    go = b.coordinate({"w0", "w1", "w2"}, 5, 0.1)
    for t in ts:
        t.join()
    assert all(v["go_at"] == go["go_at"] for v in got.values())


def test_barrier_fails_closed_when_writer_missing(tmp_path):
    """A missing writer aborts the barrier for everyone, including late stragglers, instead of hanging."""
    b = FileBarrier(tmp_path, poll_seconds=0.01)
    b.mark_ready("w0")
    with pytest.raises(BarrierTimeout):
        b.coordinate({"w0", "w1"}, 0.2, 0.1)
    assert b.abort_path.exists()
    with pytest.raises(BarrierAborted):
        b.wait_go(1)
    # a straggler arriving later also aborts immediately instead of hanging
    t0 = time.monotonic()
    with pytest.raises(BarrierAborted):
        FileBarrier(tmp_path, 0.01).wait_go(5)
    assert time.monotonic() - t0 < 1


def test_writer_wait_is_bounded(tmp_path):
    """A writer waiting for a go signal that never comes times out."""
    with pytest.raises(BarrierTimeout):
        FileBarrier(tmp_path, 0.01).wait_go(0.1)


def test_plan_deterministic_and_roundtrips(example_cfg):
    """The same config and run id always produce the same plan, and the plan survives a JSON round trip."""
    a = build_plan(example_cfg, "r20261114t220000_abcdef")
    b = build_plan(example_cfg, "r20261114t220000_abcdef")
    assert a.to_json() == b.to_json()
    assert Plan.from_json(a.to_json()).to_json() == a.to_json()


def test_contention_sequence_assignment(example_cfg):
    """Contention cells assign sequences and scopes as designed.

    Covers same scope, disjoint scopes and overlapping table subsets.
    """
    plan = build_plan(example_cfg, "r20261114t220000_abcdef")
    cells = [c for c in plan.cells if c.kind is CellKind.CONTENTION and c.writer_count == 5]
    same = next(c for c in cells if c.contention is ContentionMode.SAME_SCOPE)
    seqs = sorted(p.sequence for w in same.writers for p in w.publications)
    assert seqs == [2, 3, 4, 5, 6] and {p.dataset_scope for w in same.writers for p in w.publications} == {"scope_0001"}
    assert [p.sequence for p in same.setup_publications] == [1]
    disj = next(c for c in cells if c.contention is ContentionMode.DISJOINT_SCOPES)
    assert len({w.publications[0].dataset_scope for w in disj.writers}) == 5
    ovl = next(c for c in cells if c.contention is ContentionMode.OVERLAPPING_TABLES)
    subsets = [w.publications[0].table_indexes for w in ovl.writers]
    assert all(len(s) == 2 for s in subsets) and len(set(subsets)) > 1


def test_same_data_profile_for_strategies_and_formats(example_cfg):
    """All four strategy x format performance cells read the same data profile, so they are comparable."""
    plan = build_plan(example_cfg, "r20261114t220000_abcdef")
    perf = [c for c in plan.cells if c.kind is CellKind.PERFORMANCE]
    assert {c.profile_key for c in perf} == {"d00"}
    assert {(c.strategy.value, c.fmt.value) for c in perf} == {
        ("scd2_full", "delta"),
        ("scd2_full", "iceberg"),
        ("append_only_full", "delta"),
        ("append_only_full", "iceberg"),
    }


def test_budget_guardrail():
    """A plan whose estimated size exceeds the configured budget is refused before anything is created."""
    cfg = load_config(EXAMPLE, {"rows": 10_000_000, "publications": 50})
    with pytest.raises(BudgetExceeded):
        build_plan(cfg, "r20261114t220000_abcdef")
