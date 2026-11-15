"""Phase 02 acceptance: deterministic contents, full snapshot counts, unique keys, meaningful invalid inputs."""

from __future__ import annotations

from dataclasses import replace

import pytest

from dbxbench.datagen import snapshot_frame, with_duplicate_keys
from dbxbench.model import DataProfile

pytestmark = pytest.mark.spark

BASE = DataProfile(
    profile_key="d00",
    seed=42,
    table_count=2,
    rows_per_table_per_publication=300,
    publication_count=4,
    dataset_scope_count=2,
    payload_width_bytes=40,
    changed_value_fraction=0.25,
    category_skew=0.0,
    category_cardinality=20,
    scope_size_skew=0.0,
    null_fraction=0.1,
    publication_interval_millis=1000,
    publication_timestamp_origin_utc="2026-01-01T00:00:00Z",
)
PAIRS = {("scope_0001", q) for q in range(1, 5)} | {("scope_0002", q) for q in range(1, 5)}


def fp(df):
    from pyspark.sql import functions as F

    r = df.select(F.count("*").alias("n"), F.sum(F.xxhash64(*df.columns).cast("decimal(38,0)")).alias("h")).collect()[0]
    return r["n"], r["h"]


def test_deterministic_regardless_of_partitioning_and_pair_subset(spark):
    """Contents depend only on the inputs.

    Neither partitioning nor which (scope, sequence) pairs are generated together changes any row.
    """
    a = snapshot_frame(spark, BASE, 0, PAIRS)
    b = snapshot_frame(spark, BASE, 0, PAIRS).repartition(7)
    assert fp(a) == fp(b)
    # generating a single pair yields exactly the same rows as that pair inside the full set
    only = snapshot_frame(spark, BASE, 0, {("scope_0002", 3)})
    sub = a.where("dataset_scope = 'scope_0002' AND publication_sequence = 3")
    assert fp(only) == fp(sub)
    # a different seed or table index changes content
    assert fp(snapshot_frame(spark, replace(BASE, seed=43), 0, PAIRS)) != fp(a)
    assert fp(snapshot_frame(spark, BASE, 1, PAIRS)) != fp(a)


def test_full_snapshot_counts_and_unique_keys(spark):
    """Every snapshot has exactly the configured row count, unique keys and no null key columns."""
    df = snapshot_frame(spark, BASE, 0, PAIRS)
    rows = (
        df.groupBy("dataset_scope", "publication_sequence")
        .agg({"*": "count", "business_key": "approx_count_distinct"})
        .collect()
    )
    assert len(rows) == 8
    for r in rows:
        assert r["count(1)"] == 300
    dups = df.groupBy("dataset_scope", "publication_sequence", "business_key").count().where("count > 1").count()
    assert dups == 0
    assert df.where("business_key IS NULL OR payload_hash IS NULL OR dataset_scope IS NULL").count() == 0


def test_change_fraction_semantics(spark):
    """changed_value_fraction controls how many keys change between publications (0, all, about 25%)."""

    def changed_share(f):
        df = snapshot_frame(spark, replace(BASE, changed_value_fraction=f), 0, {("scope_0001", 1), ("scope_0001", 2)})
        p1 = (
            df.where("publication_sequence = 1")
            .select("business_key", "payload_hash")
            .withColumnRenamed("payload_hash", "h1")
        )
        p2 = df.where("publication_sequence = 2").select("business_key", "payload_hash")
        j = p1.join(p2, "business_key")
        return j.where("h1 <> payload_hash").count() / j.count()

    assert changed_share(0.0) == 0.0
    assert changed_share(1.0) == 1.0
    assert 0.15 < changed_share(0.25) < 0.35


def test_nulls_skew_and_width(spark):
    """null_fraction, category_skew and payload_width_bytes have the intended effect."""
    df = snapshot_frame(spark, replace(BASE, null_fraction=0.2, category_skew=0.9), 0, {("scope_0001", 1)})
    share_null = df.where("attr_int IS NULL").count() / df.count()
    assert 0.1 < share_null < 0.3
    top = (
        df.where("attr_category IS NOT NULL").groupBy("attr_category").count().orderBy("count", ascending=False).first()
    )
    assert top["count"] / df.where("attr_category IS NOT NULL").count() > 0.3  # skew concentrates categories
    lens = {r[0] for r in df.where("attr_text IS NOT NULL").selectExpr("length(attr_text)").distinct().collect()}
    assert lens == {40}


def test_scope_size_skew_is_deterministic():
    """Zipf scope sizes are deterministic and decreasing."""
    p = replace(BASE, scope_size_skew=1.0, dataset_scope_count=4)
    sizes = [p.rows_for_scope(i) for i in range(4)]
    assert sizes == [p.rows_for_scope(i) for i in range(4)] and sizes[0] > sizes[3]


def test_invalid_input_duplicate_keys(spark):
    """The negative-input generator adds exactly the requested number of duplicate keys."""
    df = with_duplicate_keys(snapshot_frame(spark, BASE, 0, {("scope_0001", 1)}), 5)
    assert df.count() == 305
    assert df.groupBy("business_key").count().where("count > 1").count() == 5
