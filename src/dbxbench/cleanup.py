"""Explicit, ownership-checked cleanup. Never automatic, never outside the run-owned namespace.

Refuses unless ALL hold:
* ``project.allow_cleanup: true`` in config AND ``--yes`` on the command line;
* an explicit, well-formed ``--run-id`` (no wildcards / ambiguity);
* per_run_schema: the schema name is exactly ``<schema_prefix>_<run_id>`` (plus, for the per_cell layout,
  ``<that>__<cell_key>`` schemas) and each carries the dbxbench ownership property and EXACTLY this run id;
  every table inside carries them too (no foreign objects);
* fixed_schema: only tables named ``<run_id>__*`` that carry the ownership + run-id properties;
* no FAILED cells/verdicts unless ``--discard-failure-evidence`` is given (failures retain evidence).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .config import BenchConfig, NamespaceMode
from .executor import SqlExecutor
from .naming import OWNER_PROPERTY, OWNER_VALUE, RUN_PROPERTY, Namespace, validate_run_id
from .results import RunPaths, read_json
from .sqlutil import quote_ident, sql_string


class CleanupRefused(RuntimeError):
    pass


@dataclass
class CleanupPlan:
    statements: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def _props_from_rows(rows: list[dict[str, Any]]) -> str:
    return json.dumps(rows, default=str)


def _owned(text: str, run_id: str) -> bool:
    if OWNER_PROPERTY not in text or OWNER_VALUE not in text:
        return False
    # exact run id: "free_x" must not match a schema owned by run "free_x_2"
    pat = re.escape(RUN_PROPERTY) + r"[^A-Za-z0-9_]+(?:value[^A-Za-z0-9_]+)?" + re.escape(run_id) + r"(?![A-Za-z0-9_])"
    return re.search(pat, text) is not None


def _schema_names(rows: list[dict[str, Any]]) -> list[str]:
    out = []
    for r in rows:
        v = r.get("databaseName", r.get("namespace", r.get("schema_name")))
        if v is None and r:
            v = next(iter(r.values()))
        if v is not None:
            out.append(str(v))
    return out


def failure_evidence(paths: RunPaths) -> list[str]:
    bad = []
    cells = paths.root / "cells"
    if cells.exists():
        for st in cells.glob("*/status.json"):
            d = read_json(st)
            if d.get("status") == "FAILED" or str(d.get("verdict", "")).startswith("FAIL"):
                bad.append(st.parent.name)
    return sorted(bad)


def plan_cleanup(
    cfg: BenchConfig, run_id: str, x: SqlExecutor, *, yes: bool, discard_failure_evidence: bool = False
) -> CleanupPlan:
    if not run_id or any(c in run_id for c in "*%?"):
        raise CleanupRefused("cleanup requires one explicit --run-id (no wildcards)")
    validate_run_id(run_id)
    if not cfg.project.allow_cleanup:
        raise CleanupRefused("project.allow_cleanup is false in the config")
    if not yes:
        raise CleanupRefused("cleanup requires --yes")
    ns = Namespace.for_run(cfg, run_id)
    paths = RunPaths.for_run(cfg.project.results_root, run_id)
    plan = CleanupPlan()
    bad = failure_evidence(paths)
    if bad and not discard_failure_evidence:
        raise CleanupRefused(
            f"cells {bad} FAILED; their tables are retained as evidence. Re-run with --discard-failure-evidence "
            "to delete them anyway"
        )
    plan.warnings.append(
        f"DELETION WARNING: this permanently drops run-owned objects in {ns.catalog}.{ns.schema}"
        + (f" with prefix {ns.table_prefix}" if ns.table_prefix else "")
        + ". Local/Volume result files are NOT deleted."
    )
    if ns.mode is NamespaceMode.PER_RUN_SCHEMA:
        expected = f"{cfg.project.schema_prefix}_{run_id}"
        if ns.schema != expected:
            raise CleanupRefused(f"schema {ns.schema} != expected {expected}")
        listed = _schema_names(
            x.execute(f"SHOW SCHEMAS IN {quote_ident(ns.catalog)} LIKE {sql_string(expected + '*')}")
        )
        cell_schemas = sorted(n for n in listed if n.startswith(ns.cell_schema_prefix))
        schemas = cell_schemas + ([expected] if (not listed or expected in listed) else [])
        if not schemas:
            plan.warnings.append(f"no schemas found for run {run_id}")
        for name in schemas:
            schema_sql = f"{quote_ident(ns.catalog)}.{quote_ident(name)}"
            rows = x.execute(f"DESCRIBE SCHEMA EXTENDED {schema_sql}")
            if cfg.project.require_ownership_marker and not _owned(_props_from_rows(rows), run_id):
                raise CleanupRefused(f"schema {name} lacks the dbxbench ownership marker for {run_id}")
            for t in x.execute(f"SHOW TABLES IN {schema_sql}"):
                tname = str(t.get("tableName"))
                if t.get("isTemporary"):
                    continue
                props = x.execute(f"SHOW TBLPROPERTIES {schema_sql}.{quote_ident(tname)}")
                if cfg.project.require_ownership_marker and not _owned(_props_from_rows(props), run_id):
                    raise CleanupRefused(f"table {name}.{tname} is not owned by run {run_id}; refusing schema drop")
            plan.statements.append(f"DROP SCHEMA {schema_sql} CASCADE")
    else:
        rows = x.execute(f"SHOW TABLES IN {ns.schema_sql} LIKE {sql_string(ns.table_prefix + '*')}")
        for t in rows:
            name = str(t.get("tableName"))
            if not name.startswith(ns.table_prefix):
                continue
            props = x.execute(f"SHOW TBLPROPERTIES {ns.schema_sql}.{quote_ident(name)}")
            if not _owned(_props_from_rows(props), run_id):
                plan.warnings.append(f"skipping {name}: no ownership marker for {run_id}")
                continue
            plan.statements.append(f"DROP TABLE {ns.schema_sql}.{quote_ident(name)}")
    return plan


def execute_cleanup(plan: CleanupPlan, x: SqlExecutor, dry_run: bool) -> list[str]:
    done = []
    for s in plan.statements:
        if not dry_run:
            x.execute(s)
        done.append(s)
    return done


def local_paths_note(paths: RunPaths) -> Path:
    return paths.root
