"""Read-query benchmark: equivalent questions answered over SCD2 and append-only layouts.

Append-only variants select the latest/as-of publication through the ledger and DERIVE validity end
timestamps with LEAD() over publication order at query time; that derivation cost is part of the
measured latency by design. Each query returns a small aggregated/ordered result whose hash is
recorded, so answer equivalence across strategies and formats is checked, not assumed.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .config import Strategy
from .executor import SqlExecutor
from .sqlutil import TableRef, sql_timestamp, sql_token


@dataclass(frozen=True)
class QueryParams:
    scope: str
    business_key: str
    as_of: datetime
    window_start: datetime
    window_end: datetime


def queries_for(strategy: Strategy, t: TableRef, ledger: TableRef, p: QueryParams) -> dict[str, str]:
    s = sql_token(p.scope)
    k = sql_token(p.business_key)
    at = sql_timestamp(p.as_of)
    w0, w1 = sql_timestamp(p.window_start), sql_timestamp(p.window_end)
    agg = "count(*) AS n, sum(attr_int) AS s_int, sum(attr_decimal) AS s_dec, count(DISTINCT payload_hash) AS h"
    if strategy is not Strategy.APPEND_ONLY_FULL:
        return {
            "latest_snapshot": f"SELECT {agg} FROM {t.sql} WHERE dataset_scope = {s} AND effective_to_date IS NULL",
            "point_in_time": (
                f"SELECT {agg} FROM {t.sql} WHERE dataset_scope = {s} AND effective_from_date <= {at} "
                f"AND (effective_to_date IS NULL OR effective_to_date > {at})"
            ),
            "key_history": (
                f"SELECT effective_from_date, effective_to_date, payload_hash FROM {t.sql} "
                f"WHERE dataset_scope = {s} AND business_key = {k} ORDER BY effective_from_date"
            ),
            "latest_aggregate": (
                f"SELECT attr_category, count(*) AS n, sum(attr_int) AS s FROM {t.sql} "
                f"WHERE dataset_scope = {s} AND effective_to_date IS NULL GROUP BY attr_category ORDER BY attr_category"
            ),
            "interval_validity": (
                f"SELECT count(*) AS n, count(DISTINCT business_key) AS keys FROM {t.sql} WHERE dataset_scope = {s} "
                f"AND effective_from_date < {w1} AND (effective_to_date IS NULL OR effective_to_date > {w0})"
            ),
        }
    latest = (
        f"(SELECT max(publication_sequence) AS seq FROM {ledger.sql} WHERE dataset_scope = {s} "
        f"AND array_contains(table_names, {sql_token(t.name)}))"
    )
    as_of = (
        f"(SELECT max(publication_sequence) AS seq FROM {ledger.sql} WHERE dataset_scope = {s} "
        f"AND array_contains(table_names, {sql_token(t.name)}) AND publication_ts <= {at})"
    )
    derived = (
        f"(SELECT business_key, payload_hash, effective_from_date, "
        f"lead(publication_ts) OVER (PARTITION BY dataset_scope, business_key ORDER BY publication_sequence) "
        f"AS effective_to_date FROM {t.sql} WHERE dataset_scope = {s})"
    )
    return {
        "latest_snapshot": (
            f"SELECT {agg} FROM {t.sql} d JOIN {latest} l ON d.publication_sequence = l.seq WHERE d.dataset_scope = {s}"
        ),
        "point_in_time": (
            f"SELECT {agg} FROM {t.sql} d JOIN {as_of} l ON d.publication_sequence = l.seq WHERE d.dataset_scope = {s}"
        ),
        "key_history": (
            f"SELECT effective_from_date, effective_to_date, payload_hash FROM {derived} h "
            f"WHERE business_key = {k} ORDER BY effective_from_date"
        ),
        "latest_aggregate": (
            f"SELECT attr_category, count(*) AS n, sum(attr_int) AS s FROM {t.sql} d JOIN {latest} l "
            f"ON d.publication_sequence = l.seq WHERE d.dataset_scope = {s} GROUP BY attr_category "
            f"ORDER BY attr_category"
        ),
        "interval_validity": (
            f"SELECT count(*) AS n, count(DISTINCT business_key) AS keys FROM {derived} h "
            f"WHERE effective_from_date < {w1} AND (effective_to_date IS NULL OR effective_to_date > {w0})"
        ),
    }


def result_hash(rows: list[dict[str, Any]]) -> str:
    return hashlib.sha256(json.dumps(rows, default=str, sort_keys=True).encode()).hexdigest()[:16]


def run_queries(
    x: SqlExecutor,
    strategy: Strategy,
    t: TableRef,
    ledger: TableRef,
    p: QueryParams,
    repetitions: int,
    clear_cache: bool,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for name, sql in queries_for(strategy, t, ledger, p).items():
        if clear_cache:
            try:
                x.execute("CLEAR CACHE")
            except Exception:  # noqa: S110 - not supported everywhere (e.g. some serverless)
                pass
        for rep in range(repetitions + 1):
            t0 = time.perf_counter()
            rows = x.execute(sql)
            secs = time.perf_counter() - t0
            out.append(
                {
                    "query": name,
                    "table": t.name,
                    "run": "first_after_clear_cache" if rep == 0 else "repeat",
                    "repetition": rep,
                    "seconds": secs,
                    "rows": len(rows),
                    "result_hash": result_hash(rows),
                    "sql": sql,
                }
            )
    return out
