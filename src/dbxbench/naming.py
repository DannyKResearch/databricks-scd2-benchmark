"""Run identifiers and the run-owned namespace.

Every benchmark run owns exactly one namespace:

* ``per_run_schema`` (default): ``<catalog>.<schema_prefix>_<run_id>`` — cleanup may drop the schema.
  With ``schema_layout: per_cell`` each cell's tables live in ``<that schema>__<cell_key>`` (same ownership
  markers), staging stays in the run schema. This keeps every schema under per-schema table quotas.
* ``fixed_schema``: ``<catalog>.<fixed_schema>`` with every table prefixed ``<run_id>__`` — cleanup
  drops only tables that carry both the prefix and the ownership property.
"""

from __future__ import annotations

import re
import secrets
from dataclasses import dataclass
from datetime import datetime

from .config import BenchConfig, NamespaceMode, SchemaLayout
from .sqlutil import TableRef, check_ident, utc_now

RUN_ID_RE = re.compile(r"^r[0-9]{8}t[0-9]{6}_[0-9a-f]{6}$|^[a-z][a-z0-9_]{2,39}$")
OWNER_PROPERTY = "dbxbench.owner"
OWNER_VALUE = "dbxbench"
RUN_PROPERTY = "dbxbench.run_id"


def new_run_id(now: datetime | None = None) -> str:
    now = now or utc_now()
    return f"r{now:%Y%m%dt%H%M%S}_{secrets.token_hex(3)}"


def validate_run_id(run_id: str) -> str:
    if not RUN_ID_RE.match(run_id):
        raise ValueError(f"run_id {run_id!r} must match {RUN_ID_RE.pattern}")
    return run_id


@dataclass(frozen=True)
class Namespace:
    catalog: str
    schema: str
    table_prefix: str
    run_id: str
    mode: NamespaceMode
    layout: SchemaLayout = SchemaLayout.SINGLE

    @staticmethod
    def for_run(cfg: BenchConfig, run_id: str) -> Namespace:
        validate_run_id(run_id)
        p = cfg.project
        catalog = "spark_catalog" if cfg.is_local else p.catalog
        if p.namespace_mode is NamespaceMode.PER_RUN_SCHEMA:
            schema = check_ident(f"{p.schema_prefix}_{run_id}")
            prefix = ""
        else:
            assert p.fixed_schema is not None
            schema = p.fixed_schema
            prefix = f"{run_id}__"
        return Namespace(catalog, schema, prefix, run_id, p.namespace_mode, p.schema_layout)

    def for_cell(self, cell_key: str) -> Namespace:
        """Namespace that holds one cell's tables (the run namespace itself unless layout is per_cell)."""
        if self.layout is not SchemaLayout.PER_CELL:
            return self
        return Namespace(
            self.catalog,
            check_ident(f"{self.schema}__{check_ident(cell_key)}"),
            "",
            self.run_id,
            self.mode,
            self.layout,
        )

    @property
    def cell_schema_prefix(self) -> str:
        return f"{self.schema}__"

    def table(self, name: str) -> TableRef:
        return TableRef(self.catalog, self.schema, check_ident(f"{self.table_prefix}{name}"))

    @property
    def schema_sql(self) -> str:
        return f"`{self.catalog}`.`{self.schema}`"

    def owns_table_name(self, table_name: str) -> bool:
        return table_name.startswith(self.table_prefix) if self.table_prefix else True


@dataclass(frozen=True)
class CellTables:
    """All tables belonging to one experiment cell (strategy x format x scenario)."""

    cell_key: str
    targets: tuple[TableRef, ...]
    ledger: TableRef
    fence: TableRef

    def target_short_names(self) -> list[str]:
        return [f"t{i + 1:02d}" for i in range(len(self.targets))]


def cell_tables(ns: Namespace, cell_key: str, table_count: int) -> CellTables:
    check_ident(cell_key)
    targets = tuple(ns.table(f"{cell_key}_t{i + 1:02d}") for i in range(table_count))
    return CellTables(cell_key, targets, ns.table(f"{cell_key}_ledger"), ns.table(f"{cell_key}_fence"))


def staging_table(ns: Namespace, profile_key: str, table_index: int) -> TableRef:
    return ns.table(f"stg_{profile_key}_t{table_index + 1:02d}")
