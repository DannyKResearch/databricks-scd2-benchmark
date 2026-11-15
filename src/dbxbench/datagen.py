"""Deterministic synthetic full-snapshot data (PySpark DataFrames, no pandas).

Determinism: every generated value is a pure function of (seed, table_index, scope, key id, change
epoch) computed with ``xxhash64``/``sha2`` expressions, so the content is identical regardless of
partitioning, executor count, or which (scope, sequence) pairs are generated together.

Change model: in publication ``q >= 2`` key ``k`` changes iff ``u(k, q) < changed_value_fraction``.
The payload of ``k`` at sequence ``q`` is derived from its *change epoch* (the latest sequence
``<= q`` at which it changed, 1 for the initial publication). Unchanged keys therefore carry
byte-identical payloads (and payload_hash) across publications, which is what strategy C detects.

Full-snapshot key set: every publication of a (table, scope) contains the same keys
``K0000000000 .. K{n-1}`` (n = per-scope snapshot size). Key churn is out of scope (see limitations).
"""

from __future__ import annotations

import math
import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from .model import DataProfile, publication_id, scope_name
from .sqlutil import TableRef

PRIME = 1_000_000_007


@dataclass
class GenerationStat:
    profile_key: str
    table: str
    pairs: int
    rows: int
    seconds: float
    size_bytes: int | None
    num_files: int | None

    def to_json(self) -> dict[str, Any]:
        return self.__dict__.copy()


def _u(F: Any, seed: int, table_index: int, *parts: Any) -> Any:
    """Uniform [0,1) from a deterministic hash of the given columns/literals."""
    cols = [F.lit(seed), F.lit(table_index), *[p if hasattr(p, "alias") else F.lit(p) for p in parts]]
    return F.pmod(F.xxhash64(*cols), F.lit(PRIME)) / F.lit(float(PRIME))


def snapshot_frame(spark: Any, prof: DataProfile, table_index: int, pairs: Iterable[tuple[str, int]]) -> Any:
    """DataFrame of staged snapshot rows for the given (scope, sequence) pairs of one table."""
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    pair_rows = []
    for scope, seq in sorted(set(pairs)):
        idx = int(scope.split("_")[1]) - 1
        pair_rows.append((scope, int(seq), int(prof.rows_for_scope(idx)), publication_id(scope, int(seq))))
    if not pair_rows:
        raise ValueError("no (scope, sequence) pairs to generate")
    schema = T.StructType(
        [
            T.StructField("dataset_scope", T.StringType()),
            T.StructField("publication_sequence", T.LongType()),
            T.StructField("n_rows", T.LongType()),
            T.StructField("publication_id", T.StringType()),
        ]
    )
    pairs_df = spark.createDataFrame(pair_rows, schema)
    max_rows = max(r[2] for r in pair_rows)
    max_seq = max(r[1] for r in pair_rows)
    keys = spark.range(0, max_rows).withColumnRenamed("id", "kid")
    df = pairs_df.crossJoin(keys).where(F.col("kid") < F.col("n_rows"))

    seed, f = prof.seed, prof.changed_value_fraction
    scope_c, kid, seq_c = F.col("dataset_scope"), F.col("kid"), F.col("publication_sequence")
    # change epoch = greatest q in [1, seq] with (q == 1 or changed at q)
    terms = [F.lit(1).cast("long")]
    for q in range(2, max_seq + 1):
        changed = _u(F, seed, table_index, scope_c, kid, F.lit(q), F.lit("chg")) < F.lit(f)
        terms.append(F.when((seq_c >= F.lit(q)) & changed, F.lit(q).cast("long")))
    epoch = F.greatest(*terms) if len(terms) > 1 else terms[0]
    df = df.withColumn("epoch", epoch)
    ep = F.col("epoch")

    def h(salt: str) -> Any:
        return F.xxhash64(F.lit(seed), F.lit(table_index), scope_c, kid, ep, F.lit(salt))

    def uu(salt: str) -> Any:
        return _u(F, seed, table_index, scope_c, kid, ep, F.lit(salt))

    def nullable(col: Any, salt: str) -> Any:
        if prof.null_fraction <= 0:
            return col
        return F.when(uu("null_" + salt) < F.lit(prof.null_fraction), F.lit(None)).otherwise(col)

    cat_exp = 1.0 + 9.0 * prof.category_skew
    cat_idx = F.least(
        F.floor(F.lit(prof.category_cardinality) * F.pow(uu("cat"), F.lit(cat_exp))),
        F.lit(prof.category_cardinality - 1),
    )
    reps = max(1, math.ceil(prof.payload_width_bytes / 64))
    text = F.substring(
        F.repeat(F.sha2(F.concat_ws("|", F.lit("txt"), h("txt").cast("string")), 256), reps),
        1,
        prof.payload_width_bytes,
    )
    df = df.select(
        scope_c.alias("dataset_scope"),
        F.concat(F.lit("K"), F.lpad(kid.cast("string"), 10, "0")).alias("business_key"),
        nullable(F.pmod(h("int"), F.lit(1_000_000_000)), "int").cast("bigint").alias("attr_int"),
        nullable((F.pmod(h("dec"), F.lit(100_000_000)) / F.lit(100.0)), "dec")
        .cast("decimal(18,4)")
        .alias("attr_decimal"),
        nullable(F.format_string("cat_%05d", cat_idx.cast("int")), "cat").alias("attr_category"),
        nullable(F.pmod(h("flag"), F.lit(2)) == F.lit(0), "flag").alias("attr_flag"),
        nullable(text, "txt").alias("attr_text"),
        F.col("publication_id"),
        seq_c.cast("bigint").alias("publication_sequence"),
    )
    payload_hash = F.sha2(
        F.concat_ws(
            "|",
            *[
                F.coalesce(F.col(c).cast("string"), F.lit("␀"))
                for c in ("attr_int", "attr_decimal", "attr_category", "attr_flag", "attr_text")
            ],
        ),
        256,
    )
    return df.select(
        "dataset_scope",
        "business_key",
        "attr_int",
        "attr_decimal",
        "attr_category",
        "attr_flag",
        "attr_text",
        payload_hash.alias("payload_hash"),
        "publication_id",
        "publication_sequence",
    )


def with_duplicate_keys(df: Any, n: int) -> Any:
    """Negative case: append n rows whose (dataset_scope, business_key) already exist."""
    return df.unionByName(df.limit(n))


def existing_pair_counts(spark: Any, ref: TableRef) -> dict[tuple[str, int], int]:
    rows = spark.sql(
        f"SELECT dataset_scope, publication_sequence, count(*) AS n FROM {ref.sql} GROUP BY 1, 2"
    ).collect()
    return {(r["dataset_scope"], int(r["publication_sequence"])): int(r["n"]) for r in rows}


def generate_profile(
    spark: Any, prof: DataProfile, pairs: set[tuple[str, int]], staging: list[TableRef]
) -> list[GenerationStat]:
    """Materialize staging for every table of the profile. Idempotent: pairs already fully present
    are skipped; a partially present pair is an error (staging must be immutable)."""
    stats: list[GenerationStat] = []
    for ti, ref in enumerate(staging):
        existing = existing_pair_counts(spark, ref)
        todo: set[tuple[str, int]] = set()
        for scope, seq in pairs:
            expected = prof.rows_for_scope(int(scope.split("_")[1]) - 1)
            have = existing.get((scope, seq), 0)
            if have == 0:
                todo.add((scope, seq))
            elif have != expected:
                raise RuntimeError(
                    f"staging {ref.plain} has {have} rows for ({scope}, {seq}), expected {expected}; "
                    "staging is immutable - start a new run_id"
                )
        t0 = time.perf_counter()
        rows = 0
        if todo:
            df = snapshot_frame(spark, prof, ti, todo)
            df.write.insertInto(ref.plain)
            rows = sum(prof.rows_for_scope(int(s.split("_")[1]) - 1) for s, _ in todo)
        secs = time.perf_counter() - t0
        size = files = None
        try:
            d = spark.sql(f"DESCRIBE DETAIL {ref.sql}").collect()[0].asDict()
            size, files = d.get("sizeInBytes"), d.get("numFiles")
        except Exception:  # noqa: S110 - metrics are best effort
            pass
        stats.append(GenerationStat(prof.profile_key, ref.plain, len(todo), rows, secs, size, files))
    return stats


def content_fingerprint_sql(ref: TableRef, where: str = "TRUE") -> str:
    """Order-independent content fingerprint (count + sum of row hashes).

    Summed as DECIMAL(38,0): a BIGINT sum of 64-bit hashes overflows under ANSI mode (default in Spark 4).
    """
    h = "CAST(xxhash64(dataset_scope, business_key, payload_hash, publication_sequence) AS DECIMAL(38,0))"
    return f"SELECT count(*) AS n, sum({h}) AS fp FROM {ref.sql} WHERE {where}"


__all__ = [
    "GenerationStat",
    "content_fingerprint_sql",
    "generate_profile",
    "scope_name",
    "snapshot_frame",
    "with_duplicate_keys",
]
