"""Unit tests for SQL safety helpers (identifier allowlist, escaping, UTC timestamp literals) and run naming."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from dbxbench.naming import Namespace, cell_tables, new_run_id, validate_run_id
from dbxbench.sqlutil import (
    TableRef,
    UnsafeSqlError,
    quote_ident,
    sql_int,
    sql_string,
    sql_timestamp,
    sql_token,
)


@pytest.mark.parametrize("bad", ["a`b", "a b", "a;b", "1abc", "", "a.b", "x" * 129, "drop table--"])
def test_identifier_allowlist(bad):
    """Identifiers outside the allowlist (quotes, spaces, dots, too long) are refused."""
    with pytest.raises(UnsafeSqlError):
        quote_ident(bad)


def test_quoting_and_refs():
    """Table references are quoted consistently and parse back; unsafe parts are refused."""
    assert quote_ident("abc_1") == "`abc_1`"
    r = TableRef("cat", "sch", "t01")
    assert r.sql == "`cat`.`sch`.`t01`"
    assert TableRef.parse("cat.sch.t01") == r
    with pytest.raises(UnsafeSqlError):
        TableRef("cat", "sch", "t`; DROP")


def test_literals_are_escaped():
    """String literals are escaped; newlines, injection-shaped tokens and booleans-as-ints are refused."""
    assert sql_string("it's") == "'it\\'s'"
    assert sql_string("a\\b") == "'a\\\\b'"
    with pytest.raises(UnsafeSqlError):
        sql_string("line\nbreak")
    with pytest.raises(UnsafeSqlError):
        sql_token("x' OR '1'='1")
    with pytest.raises(UnsafeSqlError):
        sql_int(True)  # bools are not ints here
    assert sql_int(42) == "42"


def test_timestamp_literal_is_explicit_utc_with_micros():
    """Timestamps render as explicit UTC with microseconds; naive datetimes are refused."""
    dt = datetime(2026, 1, 1, 5, 0, 0, 123456, tzinfo=timezone(timedelta(hours=5)))
    assert sql_timestamp(dt) == "TIMESTAMP'2026-01-01 00:00:00.123456+00:00'"
    with pytest.raises(UnsafeSqlError):
        sql_timestamp(datetime(2026, 1, 1))  # naive


def test_run_ids_and_namespace(example_cfg):
    """Run ids are well formed and the run schema and table names follow the documented pattern."""
    rid = new_run_id(datetime(2026, 11, 14, 22, 0, 0, tzinfo=UTC))
    assert validate_run_id(rid).startswith("r20261114t220000_")
    with pytest.raises(ValueError):
        validate_run_id("*")
    ns = Namespace.for_run(example_cfg, rid)
    assert ns.schema == f"scd2_bench_{rid}" and ns.catalog == "dbx_research"
    ct = cell_tables(ns, "c001_scd2_dlt", 3)
    assert [t.name for t in ct.targets] == ["c001_scd2_dlt_t01", "c001_scd2_dlt_t02", "c001_scd2_dlt_t03"]
    assert ct.ledger.name.endswith("_ledger") and ct.fence.name.endswith("_fence")
