"""SQL text generation: DDL (always outside transactions) and the single publication block.

The publication block is ONE SQL compound statement, submitted with ONE ``spark.sql`` call:

    BEGIN ATOMIC
      DECLARE ...;                         -- local variables
      <idempotency check against ledger>   -- duplicate => SIGNAL, nothing written
      <order fence check on fence row>     -- stale/not-ready => SIGNAL, nothing written
      UPDATE fence ...;                    -- the per-scope row every same-scope writer must modify
      <per-table strategy DML>             -- UPDATE active rows + INSERT snapshot (scd2_full) / INSERT only
      INSERT INTO ledger ...;              -- durable publication record, same transaction
    END;

Concurrency-safety argument (verified empirically by tests/databricks, NOT assumed):
two transactions publishing to the same scope both UPDATE the same fence row. Databricks uses
optimistic concurrency with row-level conflict detection; "UPDATE/DELETE/MERGE pairs can conflict
when modifying the same row" (docs: optimizations/isolation/row-level-concurrency). Therefore at
most one of them commits; the loser fails with a classified concurrency error and the engine retries
the *entire* block, whose fresh reads then observe the winner's ledger row and fence sequence.
No reliance is placed on (non-enforced) UC primary-key constraints.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from .config import FaultScenario, OrderingMode, Strategy, TableFormat
from .model import (
    APPEND_EXTRA,
    FENCE_COLUMNS,
    KEY_COLUMNS,
    LEDGER_COLUMNS,
    PAYLOAD_COLUMNS,
    SCD2_EXTRA,
    STAGING_EXTRA,
    Publication,
)
from .naming import OWNER_PROPERTY, OWNER_VALUE, RUN_PROPERTY, CellTables, Namespace
from .sqlutil import (
    TableRef,
    quote_ident,
    sql_int,
    sql_string,
    sql_string_array,
    sql_timestamp,
    sql_token,
)

MARKER = "DBXBENCH_"
# Codes raised from inside the block. The engine parses "DBXBENCH_<CODE>" from the error message.
CODE_DUPLICATE = "DUPLICATE_PUBLICATION"
CODE_STALE = "STALE_SEQUENCE"
CODE_NOT_READY = "SEQUENCE_NOT_READY"
CODE_FENCE_MISSING = "FENCE_MISSING"
CODE_FENCE_LOST = "FENCE_UPDATE_NOT_VISIBLE"
CODE_STAGING = "STAGING_COUNT_MISMATCH"
CODE_INJECTED = "INJECTED_FAULT"
USER_SQLSTATE = "45000"


@dataclass(frozen=True)
class Dialect:
    name: str
    begin: str
    atomic: bool
    local_locations: bool

    def fail(self, code: str, detail: str) -> str:
        msg = f"{MARKER}{code}: {detail}"
        if self.atomic or self.name == "databricks":
            # Documented in the BEGIN ATOMIC tutorial: SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '...'
            return f"SIGNAL SQLSTATE '{USER_SQLSTATE}' SET MESSAGE_TEXT = {sql_string(msg)};"
        # OSS Spark 4.x scripting has no SIGNAL; raise_error() is the deterministic equivalent.
        return f"SELECT raise_error({sql_string(msg)});"


DATABRICKS = Dialect(name="databricks", begin="BEGIN ATOMIC", atomic=True, local_locations=False)
LOCAL_EMULATION = Dialect(name="local_nonatomic_emulation", begin="BEGIN", atomic=False, local_locations=True)


def _cols(columns: Sequence[tuple[str, str]]) -> str:
    return ",\n  ".join(f"{quote_ident(n)} {t}" for n, t in columns)


def _names(columns: Sequence[tuple[str, str]]) -> str:
    return ", ".join(quote_ident(n) for n, _ in columns)


def _props(props: dict[str, str]) -> str:
    return ", ".join(f"{sql_string(k)} = {sql_string(v)}" for k, v in sorted(props.items()))


def target_columns(strategy: Strategy) -> tuple[tuple[str, str], ...]:
    extra = APPEND_EXTRA if strategy is Strategy.APPEND_ONLY_FULL else SCD2_EXTRA
    return KEY_COLUMNS + PAYLOAD_COLUMNS + extra


def staging_columns() -> tuple[tuple[str, str], ...]:
    return KEY_COLUMNS + PAYLOAD_COLUMNS + STAGING_EXTRA


# ------------------------------------------------------------------------------------------ DDL
def create_schema_sql(ns: Namespace) -> str:
    # Deliberately NOT "IF NOT EXISTS": a run never adopts a pre-existing schema.
    props = {OWNER_PROPERTY: OWNER_VALUE, RUN_PROPERTY: ns.run_id}
    return (
        f"CREATE SCHEMA {ns.schema_sql} COMMENT {sql_string('dbxbench run-owned benchmark schema')} "
        f"WITH DBPROPERTIES ({_props(props)})"
    )


def create_table_sql(
    ref: TableRef,
    columns: Sequence[tuple[str, str]],
    fmt: TableFormat,
    properties: dict[str, str],
    run_id: str,
    partitioned_by: Sequence[str] = (),
    location: str | None = None,
) -> str:
    # Deliberately NOT "IF NOT EXISTS" / "OR REPLACE": never overwrite an existing table.
    props = {**properties, OWNER_PROPERTY: OWNER_VALUE, RUN_PROPERTY: run_id}
    using = "ICEBERG" if fmt is TableFormat.ICEBERG else "DELTA"
    parts = [f"CREATE TABLE {ref.sql} (\n  {_cols(columns)}\n) USING {using}"]
    if partitioned_by:
        parts.append("PARTITIONED BY (" + ", ".join(quote_ident(c) for c in partitioned_by) + ")")
    if location is not None:
        parts.append(f"LOCATION {sql_string(location)}")
    parts.append(f"TBLPROPERTIES ({_props(props)})")
    return "\n".join(parts)


def fence_seed_sql(fence: TableRef, scopes: Sequence[str]) -> str:
    rows = ", ".join(
        f"({sql_token(s)}, 0, CAST(NULL AS STRING), CAST(NULL AS TIMESTAMP), 0, CAST(NULL AS STRING))" for s in scopes
    )
    return f"INSERT INTO {fence.sql} ({_names(FENCE_COLUMNS)}) VALUES {rows}"


# ------------------------------------------------------------------------- transaction block
@dataclass(frozen=True)
class TxnContext:
    run_id: str
    cell_key: str
    strategy: Strategy
    ordering: OrderingMode
    writer_id: str
    attempt_token: str
    attempt_number: int
    tables: CellTables
    staging: tuple[TableRef, ...]  # indexed by table index, aligned with tables.targets
    fault: FaultScenario | None = None
    staging_count_check: bool = True


IN_TXN_FAULTS = {
    FaultScenario.AFTER_FIRST_UPDATE,
    FaultScenario.AFTER_FIRST_INSERT,
    FaultScenario.LAST_TABLE,
    FaultScenario.AFTER_LEDGER,
}


def _staging_filter(pub: Publication) -> str:
    return (
        f"{quote_ident('dataset_scope')} = {sql_token(pub.dataset_scope)} "
        f"AND {quote_ident('publication_sequence')} = {sql_int(pub.sequence)}"
    )


def _scd2_full_statements(target: TableRef, stg: TableRef, pub: Publication) -> list[str]:
    ts = sql_timestamp(pub.publication_ts)
    scope = sql_token(pub.dataset_scope)
    insert_cols = KEY_COLUMNS + PAYLOAD_COLUMNS + SCD2_EXTRA
    select = ", ".join(quote_ident(n) for n, _ in KEY_COLUMNS + PAYLOAD_COLUMNS)
    return [
        # Close ALL currently active rows for the scope (full-snapshot semantics).
        f"UPDATE {target.sql} SET {quote_ident('effective_to_date')} = {ts} "
        f"WHERE {quote_ident('dataset_scope')} = {scope} AND {quote_ident('effective_to_date')} IS NULL;",
        # Insert the ENTIRE immutable staged snapshot as new active versions.
        f"INSERT INTO {target.sql} ({_names(insert_cols)}) "
        f"SELECT {select}, {ts}, CAST(NULL AS TIMESTAMP), {sql_token(pub.publication_id)}, "
        f"{sql_int(pub.sequence)}, {ts} FROM {stg.sql} WHERE {_staging_filter(pub)};",
    ]


def _append_only_statements(target: TableRef, stg: TableRef, pub: Publication) -> list[str]:
    ts = sql_timestamp(pub.publication_ts)
    insert_cols = KEY_COLUMNS + PAYLOAD_COLUMNS + APPEND_EXTRA
    select = ", ".join(quote_ident(n) for n, _ in KEY_COLUMNS + PAYLOAD_COLUMNS)
    return [
        f"INSERT INTO {target.sql} ({_names(insert_cols)}) "
        f"SELECT {select}, {ts}, {sql_token(pub.publication_id)}, {sql_int(pub.sequence)}, {ts} "
        f"FROM {stg.sql} WHERE {_staging_filter(pub)};"
    ]


def _scd2_changed_only_statements(target: TableRef, stg: TableRef, pub: Publication) -> list[str]:
    """OPTIONAL strategy C: version only changed/new keys, close changed and vanished keys.

    This is NOT the mandated full-snapshot semantics and is reported separately.
    """
    ts = sql_timestamp(pub.publication_ts)
    scope = sql_token(pub.dataset_scope)
    q = quote_ident
    insert_cols = KEY_COLUMNS + PAYLOAD_COLUMNS + SCD2_EXTRA
    select_s = ", ".join(f"s.{q(n)}" for n, _ in KEY_COLUMNS + PAYLOAD_COLUMNS)
    return [
        f"MERGE INTO {target.sql} AS t USING (SELECT * FROM {stg.sql} WHERE {_staging_filter(pub)}) AS s "
        f"ON t.{q('dataset_scope')} = s.{q('dataset_scope')} AND t.{q('business_key')} = s.{q('business_key')} "
        f"AND t.{q('effective_to_date')} IS NULL "
        f"WHEN MATCHED AND t.{q('payload_hash')} <> s.{q('payload_hash')} "
        f"THEN UPDATE SET t.{q('effective_to_date')} = {ts} "
        f"WHEN NOT MATCHED BY SOURCE AND t.{q('dataset_scope')} = {scope} AND t.{q('effective_to_date')} IS NULL "
        f"THEN UPDATE SET t.{q('effective_to_date')} = {ts};",
        f"INSERT INTO {target.sql} ({_names(insert_cols)}) "
        f"SELECT {select_s}, {ts}, CAST(NULL AS TIMESTAMP), {sql_token(pub.publication_id)}, "
        f"{sql_int(pub.sequence)}, {ts} FROM {stg.sql} AS s LEFT ANTI JOIN "
        f"(SELECT {q('business_key')} FROM {target.sql} WHERE {q('dataset_scope')} = {scope} "
        f"AND {q('effective_to_date')} IS NULL) AS a ON s.{q('business_key')} = a.{q('business_key')} "
        f"WHERE s.{q('dataset_scope')} = {scope} AND s.{q('publication_sequence')} = {sql_int(pub.sequence)};",
    ]


STRATEGY_BUILDERS = {
    Strategy.SCD2_FULL: _scd2_full_statements,
    Strategy.APPEND_ONLY_FULL: _append_only_statements,
    Strategy.SCD2_CHANGED_ONLY: _scd2_changed_only_statements,
}


def build_publication_sql(ctx: TxnContext, pub: Publication, dialect: Dialect) -> str:
    """Return the complete transaction text. It must be submitted with exactly one spark.sql call."""
    if len(ctx.staging) != len(ctx.tables.targets):
        raise ValueError("staging refs must align with target refs")
    for i in pub.table_indexes:
        if not 0 <= i < len(ctx.tables.targets):
            raise ValueError(f"publication references table index {i} outside the cell")
    q = quote_ident
    scope = sql_token(pub.dataset_scope)
    pid = sql_token(pub.publication_id)
    seq = sql_int(pub.sequence)
    fence = ctx.tables.fence.sql
    ledger = ctx.tables.ledger.sql
    token = sql_token(ctx.attempt_token)
    detail = f"scope={pub.dataset_scope} publication={pub.publication_id} seq={pub.sequence}"
    body: list[str] = [
        "DECLARE v_existing BIGINT DEFAULT 0;",
        "DECLARE v_last_seq BIGINT DEFAULT NULL;",
        "DECLARE v_fence_ok BIGINT DEFAULT 0;",
    ]
    if ctx.staging_count_check:
        body.append("DECLARE v_staged BIGINT DEFAULT 0;")
    # 1) Idempotency: a publication already in the ledger is never applied twice.
    body += [
        f"SET v_existing = (SELECT count(*) FROM {ledger} WHERE {q('dataset_scope')} = {scope} "
        f"AND {q('publication_id')} = {pid});",
        f"IF v_existing > 0 THEN {dialect.fail(CODE_DUPLICATE, detail)} END IF;",
    ]
    # 2) Order fence, read inside the transaction.
    body += [
        f"SET v_last_seq = (SELECT max({q('last_sequence')}) FROM {fence} WHERE {q('dataset_scope')} = {scope});",
        f"IF v_last_seq IS NULL THEN {dialect.fail(CODE_FENCE_MISSING, detail)} END IF;",
    ]
    if ctx.ordering in (OrderingMode.MONOTONIC, OrderingMode.SEQUENCED):
        body.append(f"IF v_last_seq >= {seq} THEN {dialect.fail(CODE_STALE, detail)} END IF;")
    if ctx.ordering is OrderingMode.SEQUENCED:
        body.append(f"IF v_last_seq < {seq} - 1 THEN {dialect.fail(CODE_NOT_READY, detail)} END IF;")
    new_last = seq if ctx.ordering is not OrderingMode.OFF else f"greatest({q('last_sequence')}, {seq})"
    body += [
        # Every same-scope writer modifies this one row => guaranteed write-write conflict between them.
        f"UPDATE {fence} SET {q('last_sequence')} = {new_last}, {q('last_publication_id')} = {pid}, "
        f"{q('last_publication_ts')} = {sql_timestamp(pub.publication_ts)}, "
        f"{q('commit_count')} = {q('commit_count')} + 1, {q('updated_by_attempt')} = {token} "
        f"WHERE {q('dataset_scope')} = {scope};",
        f"SET v_fence_ok = (SELECT count(*) FROM {fence} WHERE {q('dataset_scope')} = {scope} "
        f"AND {q('updated_by_attempt')} = {token});",
        f"IF v_fence_ok <> 1 THEN {dialect.fail(CODE_FENCE_LOST, detail)} END IF;",
    ]
    builder = STRATEGY_BUILDERS[ctx.strategy]
    first_update_done = False
    first_insert_done = False
    if ctx.fault is FaultScenario.AFTER_FIRST_UPDATE and ctx.strategy is Strategy.APPEND_ONLY_FULL:
        # Append-only has no data-table UPDATE; the fence UPDATE is its first UPDATE.
        body.append(dialect.fail(CODE_INJECTED, f"after_first_update(fence) {detail}"))
        first_update_done = True
    for n, idx in enumerate(pub.table_indexes):
        target = ctx.tables.targets[idx]
        stg = ctx.staging[idx]
        if ctx.staging_count_check:
            body += [
                f"SET v_staged = (SELECT count(*) FROM {stg.sql} WHERE {_staging_filter(pub)});",
                f"IF v_staged <> {sql_int(pub.expected_rows)} THEN "
                f"{dialect.fail(CODE_STAGING, f'{detail} table={target.name}')} END IF;",
            ]
        for stmt in builder(target, stg, pub):
            body.append(stmt)
            is_update = stmt.startswith(("UPDATE", "MERGE"))
            if is_update and not first_update_done:
                first_update_done = True
                if ctx.fault is FaultScenario.AFTER_FIRST_UPDATE:
                    body.append(dialect.fail(CODE_INJECTED, f"after_first_update {detail}"))
            if stmt.startswith("INSERT") and not first_insert_done:
                first_insert_done = True
                if ctx.fault is FaultScenario.AFTER_FIRST_INSERT:
                    body.append(dialect.fail(CODE_INJECTED, f"after_first_insert {detail}"))
        if n == len(pub.table_indexes) - 1 and ctx.fault is FaultScenario.LAST_TABLE:
            body.append(dialect.fail(CODE_INJECTED, f"last_table {detail}"))
    table_names = [ctx.tables.targets[i].name for i in pub.table_indexes]
    values = ", ".join(
        [
            sql_token(ctx.run_id),
            sql_token(ctx.cell_key),
            scope,
            pid,
            seq,
            sql_timestamp(pub.publication_ts),
            sql_token(ctx.strategy.value),
            sql_token(ctx.ordering.value),
            sql_token(ctx.writer_id),
            token,
            sql_int(ctx.attempt_number),
            sql_string_array(table_names),
            sql_int(pub.expected_rows),
            "current_timestamp()",
        ]
    )
    body.append(f"INSERT INTO {ledger} ({_names(LEDGER_COLUMNS)}) VALUES ({values});")
    if ctx.fault is FaultScenario.AFTER_LEDGER:
        body.append(dialect.fail(CODE_INJECTED, f"after_ledger {detail}"))
    if ctx.fault is not None and ctx.fault in IN_TXN_FAULTS:
        assert any(MARKER + CODE_INJECTED in s for s in body), "fault statement was not placed"
    inner = "\n  ".join(body)
    return f"{dialect.begin}\n  {inner}\nEND;"


def count_statements(block: str) -> int:
    """Rough count of top-level DML statements in a block (for reporting only)."""
    return sum(1 for line in block.splitlines() if line.strip().startswith(("UPDATE", "INSERT", "MERGE", "DELETE")))
