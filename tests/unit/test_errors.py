"""Unit tests for the error classifier that decides whether a failed attempt is retried, reconciled or terminal.
Conditions and SQLSTATEs come from the Databricks error reference; our own ``SIGNAL`` markers take priority.
"""

from __future__ import annotations

import pytest
from tests.fakes import FakeSparkError

from dbxbench.errors import Category, ClientTimeout, SimulatedLostAck, classify


@pytest.mark.parametrize(
    ("exc", "cat"),
    [
        (FakeSparkError("x", "DELTA_CONCURRENT_APPEND", "2D521"), Category.RETRYABLE_CONFLICT),
        (FakeSparkError("x", "DELTA_CONCURRENT_DELETE_READ", "2D521"), Category.RETRYABLE_CONFLICT),
        (FakeSparkError("x", "UC_WRITE_CONFLICT", "40001"), Category.RETRYABLE_CONFLICT),
        (FakeSparkError("x", "UC_ICEBERG_COMMIT_CONFLICT", "40001"), Category.RETRYABLE_CONFLICT),
        (FakeSparkError("x", "DELTA_METADATA_CHANGED", "2D521"), Category.RETRYABLE_METADATA_CONFLICT),
        (
            FakeSparkError("x", "TRANSACTION_CONCURRENT_CATALOG_METADATA_CHANGE", "40000"),
            Category.RETRYABLE_METADATA_CONFLICT,
        ),
        (FakeSparkError("io.delta.exceptions.ConcurrentAppendException: blah"), Category.RETRYABLE_CONFLICT),
        (
            FakeSparkError("[MULTI_STATEMENT_TRANSACTION_NOT_SUPPORTED.WRITE_NON_DELTA_TABLE] x SQLSTATE: 0A000"),
            Category.UNSUPPORTED,
        ),
        (FakeSparkError("x", "PERMISSION_DENIED", "42501"), Category.PERMISSION),
        (FakeSparkError("x", "TABLE_OR_VIEW_NOT_FOUND", "42P01"), Category.SCHEMA_OR_SQL),
        (FakeSparkError("x", "DELTA_VIOLATE_CONSTRAINT_WITH_VALUES", "23001"), Category.DATA_ERROR),
        (FakeSparkError("DBXBENCH_DUPLICATE_PUBLICATION: s", None, "45000"), Category.DUPLICATE_PUBLICATION),
        (FakeSparkError("DBXBENCH_STALE_SEQUENCE: s", None, "45000"), Category.REJECTED_STALE),
        (FakeSparkError("DBXBENCH_SEQUENCE_NOT_READY: s", None, "45000"), Category.ORDERING_NOT_READY),
        (FakeSparkError("DBXBENCH_INJECTED_FAULT: s", None, "45000"), Category.INJECTED_FAULT),
        (FakeSparkError("DBXBENCH_STAGING_COUNT_MISMATCH: s", None, "45000"), Category.DATA_ERROR),
        (FakeSparkError("py4j: Answer from Java side is empty"), Category.UNKNOWN_OUTCOME),
        (SimulatedLostAck("x"), Category.UNKNOWN_OUTCOME),
        (ClientTimeout("x"), Category.UNKNOWN_OUTCOME),
        (RuntimeError("weird"), Category.UNCLASSIFIED),
    ],
)
def test_classification(exc, cat):
    """Each documented error condition, SQLSTATE or our own SIGNAL marker maps to the intended category."""
    assert classify(exc).category is cat


def test_only_conflicts_and_ordering_waits_are_retryable():
    """Only conflicts and ordering waits are retryable; everything else needs reconciliation or is terminal."""
    retryable = {c for c in Category if c.retryable}
    assert retryable == {Category.RETRYABLE_CONFLICT, Category.RETRYABLE_METADATA_CONFLICT, Category.ORDERING_NOT_READY}


def test_marker_beats_sqlstate_and_condition_parsed_from_message():
    """The condition and SQLSTATE are recovered from the message text when the exception does not expose them."""
    c = classify(FakeSparkError("[DELTA_CONCURRENT_WRITE] ConcurrentWriteException SQLSTATE: 2D521"))
    assert c.category is Category.RETRYABLE_CONFLICT and c.condition == "DELTA_CONCURRENT_WRITE"
    assert c.sqlstate == "2D521"
    assert classify(SimulatedLostAck("x")).simulated


def test_raw_py4j_error_is_unwrapped():
    """A raw Py4J error is unwrapped to its real condition (here a CHECK constraint violation => DATA_ERROR)."""
    msg = (
        "An error occurred while calling o58.sql.\n"
        ": org.apache.spark.sql.delta.schema.DeltaInvariantViolationException: [DELTA_VIOLATE_CONSTRAINT_WITH_VALUES] "
        "CHECK constraint c (s < 2) violated by row with values:\n - s : 5\n\tat org.apache..."
    )
    c = classify(FakeSparkError(msg))
    assert c.condition == "DELTA_VIOLATE_CONSTRAINT_WITH_VALUES"
    assert c.category is Category.DATA_ERROR
    assert c.message.startswith("org.apache.spark.sql.delta.schema.DeltaInvariantViolationException")
