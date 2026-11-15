"""Environment capability probes (``dbxbench doctor``) and capability gating of cells.

Probes are *functional*: they execute the feature (e.g. a real two-table BEGIN ATOMIC with a
SIGNAL-triggered rollback) inside a dedicated, run-owned probe schema and inspect persistent
state afterwards. Version strings are recorded but never treated as proof of support.
"""

from __future__ import annotations

import json
import os
import re
import secrets
import sys
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

from .config import BenchConfig, Launcher, Strategy, TableFormat, TransactionMode
from .errors import classify
from .executor import SqlExecutor
from .naming import OWNER_PROPERTY, OWNER_VALUE, RUN_PROPERTY
from .sqlgen import DATABRICKS, LOCAL_EMULATION, Dialect
from .sqlutil import iso, quote_ident, sql_string, utc_now

DOCS = {
    "transactions": "https://docs.databricks.com/aws/en/transactions",
    "begin_atomic": "https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-txn-begin-atomic",
    "catalog_commits": "https://docs.databricks.com/aws/en/tables/features/catalog-commits",
    "iceberg": "https://docs.databricks.com/aws/en/iceberg/",
    "managed_tables": "https://docs.databricks.com/aws/en/tables/managed",
    "row_level_concurrency": "https://docs.databricks.com/aws/en/optimizations/isolation/row-level-concurrency",
    "mst_errors": "https://docs.databricks.com/aws/en/error-messages/multi-statement-transaction-not-supported-error-class",
}


class CapStatus(StrEnum):
    SUPPORTED = "SUPPORTED"
    UNSUPPORTED = "UNSUPPORTED"
    ERROR = "ERROR"  # probe itself failed in an unexpected way => treated as not supported
    NOT_RUN = "NOT_RUN"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    INFO = "INFO"


@dataclass
class Capability:
    name: str
    status: CapStatus
    evidence: str = ""
    error_condition: str | None = None
    sqlstate: str | None = None
    docs: str | None = None
    duration_s: float = 0.0
    value: Any = None

    def to_json(self) -> dict[str, Any]:
        d = asdict(self)
        d["status"] = self.status.value
        return d


@dataclass
class CapabilityMatrix:
    generated_at: str
    environment: dict[str, Any]
    capabilities: dict[str, Capability] = field(default_factory=dict)

    def add(self, cap: Capability) -> None:
        self.capabilities[cap.name] = cap

    def status(self, name: str) -> CapStatus:
        c = self.capabilities.get(name)
        return c.status if c else CapStatus.NOT_RUN

    def supported(self, name: str) -> bool:
        return self.status(name) is CapStatus.SUPPORTED

    def to_json(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "environment": self.environment,
            "capabilities": {k: v.to_json() for k, v in sorted(self.capabilities.items())},
        }

    @staticmethod
    def from_json(d: dict[str, Any]) -> CapabilityMatrix:
        m = CapabilityMatrix(d["generated_at"], d.get("environment", {}))
        for k, v in d["capabilities"].items():
            v = dict(v)
            v["status"] = CapStatus(v["status"])
            m.capabilities[k] = Capability(**v)
        return m

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_json(), indent=2, default=str), encoding="utf-8")


# ----------------------------------------------------------------------------- environment
def parse_dbr(version: str | None) -> tuple[int, int] | None:
    if not version:
        return None
    m = re.match(r"^(\d+)\.(\d+)", version)
    return (int(m.group(1)), int(m.group(2))) if m else None


SAFE_CONF_PREFIXES = (
    "spark.sql.session.timeZone",
    "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.",
    "spark.sql.ansi.enabled",
    "spark.sql.sources.",
    "spark.databricks.delta.",
    "spark.databricks.io.cache.",
    "spark.databricks.clusterUsageTags.sparkVersion",
    "spark.databricks.clusterUsageTags.clusterNodeType",
    "spark.databricks.clusterUsageTags.clusterWorkers",
    "spark.databricks.clusterUsageTags.clusterId",
    "spark.databricks.clusterUsageTags.clusterName",
    "spark.databricks.photon.enabled",
    "spark.master",
    "spark.executor.memory",
    "spark.driver.memory",
    "spark.app.name",
)
_REDACT = re.compile(r"(token|secret|password|credential|key)", re.I)


def safe_spark_conf(spark: Any) -> dict[str, str]:
    out: dict[str, str] = {}
    try:
        items = spark.sparkContext.getConf().getAll()
    except Exception:
        items = []
    for k, v in items:
        if k.startswith(SAFE_CONF_PREFIXES) and not _REDACT.search(k):
            out[k] = str(v)
    for k in ("spark.sql.session.timeZone", "spark.sql.ansi.enabled", "spark.sql.shuffle.partitions"):
        try:
            out[k] = str(spark.conf.get(k))
        except Exception:  # noqa: S112
            continue
    return dict(sorted(out.items()))


def collect_environment(spark: Any | None, cfg: BenchConfig) -> dict[str, Any]:
    env: dict[str, Any] = {
        "collected_at": iso(utc_now()),
        "databricks_runtime_version": os.environ.get("DATABRICKS_RUNTIME_VERSION"),
        "on_databricks": "DATABRICKS_RUNTIME_VERSION" in os.environ,
        "python": sys.version.split()[0],
        "compute_mode": cfg.compute.preferred_mode.value,
        "transaction_mode": cfg.compute.transaction_mode.value,
        "catalog": cfg.project.catalog,
    }
    if spark is not None:
        env["spark_version"] = getattr(spark, "version", None)
        env["spark_conf"] = safe_spark_conf(spark)
    return env


# ----------------------------------------------------------------------------- probes
class Prober:
    def __init__(
        self,
        executor: SqlExecutor,
        cfg: BenchConfig,
        dialect: Dialect,
        write_probes: bool = True,
        results_root: Path | None = None,
    ) -> None:
        self.x = executor
        self.cfg = cfg
        self.dialect = dialect
        self.write_probes = write_probes
        self.results_root = results_root
        suffix = secrets.token_hex(3)
        self.catalog = "spark_catalog" if cfg.is_local else cfg.project.catalog
        self.schema = f"{cfg.project.schema_prefix}_probe_{suffix}"
        self.probe_run_id = f"probe_{suffix}"
        self.schema_created = False

    def t(self, name: str) -> str:
        return f"{quote_ident(self.catalog)}.{quote_ident(self.schema)}.{quote_ident(name)}"

    def _run(self, name: str, fn: Callable[[], tuple[CapStatus, str, Any]], docs: str | None = None) -> Capability:
        t0 = time.perf_counter()
        try:
            status, evidence, value = fn()
            return Capability(name, status, evidence, docs=docs, duration_s=time.perf_counter() - t0, value=value)
        except Exception as exc:
            c = classify(exc)
            st = CapStatus.UNSUPPORTED if c.category.value in {"UNSUPPORTED", "SCHEMA_OR_SQL"} else CapStatus.ERROR
            if c.category.value == "PERMISSION":
                st = CapStatus.UNSUPPORTED
            return Capability(
                name,
                st,
                f"{c.category.value}: {c.message[:600]}",
                error_condition=c.condition,
                sqlstate=c.sqlstate,
                docs=docs,
                duration_s=time.perf_counter() - t0,
            )

    def _props(self, extra: dict[str, str] | None = None) -> str:
        props = {OWNER_PROPERTY: OWNER_VALUE, RUN_PROPERTY: self.probe_run_id, **(extra or {})}
        return ", ".join(f"{sql_string(k)} = {sql_string(v)}" for k, v in sorted(props.items()))

    def _count(self, sql: str) -> int:
        rows = self.x.execute(sql)
        return int(next(iter(rows[0].values()))) if rows else 0

    def run_all(self, matrix: CapabilityMatrix) -> CapabilityMatrix:
        local = self.cfg.is_local
        add = matrix.add
        add(self._runtime(matrix.environment))
        add(self._run("spark_sql", lambda: (CapStatus.SUPPORTED, "SELECT 1 ok", self._count("SELECT 1 AS one"))))
        add(self._run("session_timezone_utc", self._tz))
        if local:
            for n in ("unity_catalog", "catalog_commits_delta", "jobs_api"):
                add(Capability(n, CapStatus.NOT_APPLICABLE, "local OSS Spark"))
        else:
            add(self._run("unity_catalog", self._uc, DOCS["managed_tables"]))
        add(self._results_root())
        if not self.write_probes:
            for n in (
                "create_schema",
                "managed_delta_create",
                "begin_atomic_multi_table",
                "begin_atomic_rollback",
                "local_sql_scripting",
                "managed_iceberg_create",
                "iceberg_dml_update",
                "iceberg_in_txn",
                "mixed_format_txn",
                "merge_in_txn_delta",
                "staging_read_in_txn",
                "table_history_delta",
                "row_commit_version",
            ):
                add(Capability(n, CapStatus.NOT_RUN, "write probes disabled (--no-write-probes)"))
            return matrix
        add(self._run("create_schema", self._create_schema))
        if not self.schema_created:
            return matrix
        try:
            delta_props = {} if local else {"delta.feature.catalogManaged": "supported"}
            add(self._run("managed_delta_create", lambda: self._create("pa", "DELTA", delta_props)))
            add(self._run("managed_delta_create_b", lambda: self._create("pb", "DELTA", delta_props)))
            add(self._run("staging_table_create", lambda: self._create("ps", "DELTA", {})))
            if not local:
                add(
                    self._run(
                        "catalog_commits_delta", lambda: self._features("pa", "catalogManaged"), DOCS["catalog_commits"]
                    )
                )
            add(self._run("table_history_delta", lambda: self._history("pa")))
            add(self._run("row_commit_version", lambda: self._row_commit_version("pa")))
            # Transactions
            add(
                self._run(
                    "begin_atomic_multi_table", lambda: self._txn_multi("pa", "pb", DATABRICKS), DOCS["begin_atomic"]
                )
            )
            if matrix.supported("begin_atomic_multi_table"):
                add(
                    self._run(
                        "begin_atomic_rollback",
                        lambda: self._txn_rollback("pa", "pb", DATABRICKS),
                        DOCS["begin_atomic"],
                    )
                )
                add(self._run("staging_read_in_txn", lambda: self._txn_staging_read("pa", "ps", DATABRICKS)))
                add(self._run("merge_in_txn_delta", lambda: self._txn_merge("pa", DATABRICKS)))
            else:
                for n in ("begin_atomic_rollback", "staging_read_in_txn", "merge_in_txn_delta"):
                    add(Capability(n, CapStatus.NOT_RUN, "begin_atomic_multi_table not supported"))
            if local:
                add(self._run("local_sql_scripting", lambda: self._txn_multi("pa", "pb", LOCAL_EMULATION)))
            # Iceberg
            ice_props: dict[str, str] = {}
            add(self._run("managed_iceberg_create", lambda: self._create("pi", "ICEBERG", ice_props), DOCS["iceberg"]))
            if matrix.supported("managed_iceberg_create"):
                add(self._run("managed_iceberg_create_b", lambda: self._create("pj", "ICEBERG", ice_props)))
                add(self._run("iceberg_describe_detail", lambda: self._detail("pi")))
                add(self._run("table_history_iceberg", lambda: self._history("pi")))
                add(self._run("iceberg_dml_update", lambda: self._dml_update("pi")))
                add(self._run("iceberg_catalog_commits_property", self._iceberg_cc_property, DOCS["catalog_commits"]))
                add(self._run("iceberg_in_txn", lambda: self._txn_multi("pi", "pj", DATABRICKS), DOCS["transactions"]))
                if matrix.supported("iceberg_in_txn"):
                    add(self._run("iceberg_in_txn_rollback", lambda: self._txn_rollback("pi", "pj", DATABRICKS)))
                    add(self._run("merge_in_txn_iceberg", lambda: self._txn_merge("pi", DATABRICKS)))
                add(self._run("mixed_format_txn", lambda: self._txn_multi("pa", "pi", DATABRICKS)))
            else:
                for n in ("iceberg_dml_update", "iceberg_in_txn", "mixed_format_txn"):
                    add(Capability(n, CapStatus.NOT_RUN, "managed_iceberg_create not supported"))
        finally:
            self.drop_probe_schema()
        if not local:
            add(self._jobs_api())
        return matrix

    # --- individual probes
    def _runtime(self, env: dict[str, Any]) -> Capability:
        raw = env.get("databricks_runtime_version")
        if self.cfg.is_local:
            return Capability("runtime_version", CapStatus.INFO, f"local Spark {env.get('spark_version')}", value=raw)
        parsed = parse_dbr(raw)
        need = parse_dbr(self.cfg.compute.required_min_dbr)
        if parsed is None:
            return Capability(
                "runtime_version",
                CapStatus.INFO,
                f"DATABRICKS_RUNTIME_VERSION={raw!r} not parseable (serverless/SQL?); functional probes decide",
                value=raw,
            )
        ok = need is None or parsed >= need
        return Capability(
            "runtime_version",
            CapStatus.SUPPORTED if ok else CapStatus.UNSUPPORTED,
            f"DBR {raw}; required >= {self.cfg.compute.required_min_dbr}",
            docs=DOCS["begin_atomic"],
            value=raw,
        )

    def _tz(self) -> tuple[CapStatus, str, Any]:
        rows = self.x.execute("SELECT current_timezone() AS tz")
        tz = rows[0]["tz"] if rows else None
        ok = tz in ("UTC", "Etc/UTC", "+00:00", "Z")
        return (CapStatus.SUPPORTED if ok else CapStatus.UNSUPPORTED, f"session time zone = {tz}", tz)

    def _uc(self) -> tuple[CapStatus, str, Any]:
        rows = self.x.execute("SELECT current_metastore() AS m")
        self.x.execute(f"DESCRIBE CATALOG {quote_ident(self.catalog)}")
        return CapStatus.SUPPORTED, f"metastore={rows[0]['m'] if rows else '?'}; catalog {self.catalog} visible", None

    def _results_root(self) -> Capability:
        if self.results_root is None:
            return Capability("results_root_writable", CapStatus.NOT_RUN, "no results root")
        try:
            self.results_root.mkdir(parents=True, exist_ok=True)
            p = self.results_root / f".doctor_{secrets.token_hex(4)}"
            p.write_text("ok", encoding="utf-8")
            ok = p.read_text(encoding="utf-8") == "ok"
            p.unlink()
            return Capability(
                "results_root_writable", CapStatus.SUPPORTED if ok else CapStatus.ERROR, str(self.results_root)
            )
        except OSError as exc:
            return Capability("results_root_writable", CapStatus.UNSUPPORTED, f"{self.results_root}: {exc}")

    def _create_schema(self) -> tuple[CapStatus, str, Any]:
        props = ", ".join(
            f"{sql_string(k)} = {sql_string(v)}"
            for k, v in sorted({OWNER_PROPERTY: OWNER_VALUE, RUN_PROPERTY: self.probe_run_id}.items())
        )
        self.x.execute(
            f"CREATE SCHEMA {quote_ident(self.catalog)}.{quote_ident(self.schema)} WITH DBPROPERTIES ({props})"
        )
        self.schema_created = True
        return CapStatus.SUPPORTED, f"created probe schema {self.catalog}.{self.schema}", self.schema

    def _create(self, name: str, using: str, props: dict[str, str]) -> tuple[CapStatus, str, Any]:
        self.x.execute(
            f"CREATE TABLE {self.t(name)} (id INT, v STRING) USING {using} TBLPROPERTIES ({self._props(props)})"
        )
        self.x.execute(f"INSERT INTO {self.t(name)} VALUES (1, 'seed')")
        return CapStatus.SUPPORTED, f"CREATE TABLE ... USING {using} + INSERT ok", None

    def _detail(self, name: str) -> tuple[CapStatus, str, Any]:
        rows = self.x.execute(f"DESCRIBE DETAIL {self.t(name)}")
        r = rows[0] if rows else {}
        keep = {k: r.get(k) for k in ("format", "tableFeatures", "numFiles", "sizeInBytes", "properties")}
        return CapStatus.SUPPORTED, json.dumps(keep, default=str)[:800], keep

    def _features(self, name: str, feature: str) -> tuple[CapStatus, str, Any]:
        _, _, detail = self._detail(name)
        feats = list(detail.get("tableFeatures") or [])
        ok = feature in feats
        return (CapStatus.SUPPORTED if ok else CapStatus.UNSUPPORTED, f"tableFeatures={feats}", feats)

    def _history(self, name: str) -> tuple[CapStatus, str, Any]:
        rows = self.x.execute(f"DESCRIBE HISTORY {self.t(name)}")
        has_metrics = any(r.get("operationMetrics") for r in rows)
        return (
            CapStatus.SUPPORTED if rows else CapStatus.UNSUPPORTED,
            f"{len(rows)} history rows; operationMetrics present={has_metrics}",
            {"rows": len(rows), "operation_metrics": has_metrics},
        )

    def _row_commit_version(self, name: str) -> tuple[CapStatus, str, Any]:
        rows = self.x.execute(f"SELECT _metadata.row_commit_version AS v FROM {self.t(name)} LIMIT 1")
        return CapStatus.SUPPORTED, f"row_commit_version={rows[0]['v'] if rows else None}", None

    def _dml_update(self, name: str) -> tuple[CapStatus, str, Any]:
        self.x.execute(f"UPDATE {self.t(name)} SET v = 'upd' WHERE id = 1")
        n = self._count(f"SELECT count(*) FROM {self.t(name)} WHERE v = 'upd'")
        return (CapStatus.SUPPORTED if n == 1 else CapStatus.UNSUPPORTED, f"UPDATE affected visible rows={n}", n)

    def _iceberg_cc_property(self) -> tuple[CapStatus, str, Any]:
        self.x.execute(
            f"CREATE TABLE {self.t('pk')} (id INT) USING ICEBERG "
            f"TBLPROPERTIES ({self._props({'delta.feature.catalogManaged': 'supported'})})"
        )
        return CapStatus.INFO, "Iceberg table accepted delta.feature.catalogManaged property", None

    def _txn_multi(self, a: str, b: str, dialect: Dialect) -> tuple[CapStatus, str, Any]:
        before_a = self._count(f"SELECT count(*) FROM {self.t(a)}")
        before_b = self._count(f"SELECT count(*) FROM {self.t(b)}")
        tag = 100 + secrets.randbelow(10_000)
        self.x.execute(
            f"{dialect.begin}\n"
            f"  INSERT INTO {self.t(a)} VALUES ({tag}, 'txn');\n"
            f"  UPDATE {self.t(a)} SET v = 'txn-upd' WHERE id = {tag};\n"
            f"  INSERT INTO {self.t(b)} VALUES ({tag}, 'txn');\n"
            f"END;"
        )
        da = self._count(f"SELECT count(*) FROM {self.t(a)}") - before_a
        db = self._count(f"SELECT count(*) FROM {self.t(b)}") - before_b
        upd = self._count(f"SELECT count(*) FROM {self.t(a)} WHERE id = {tag} AND v = 'txn-upd'")
        ok = da == 1 and db == 1 and upd == 1
        return (
            CapStatus.SUPPORTED if ok else CapStatus.ERROR,
            f"{dialect.begin} INSERT+UPDATE+INSERT over 2 tables in ONE spark.sql call: "
            f"delta_a={da} delta_b={db} updated={upd}",
            None,
        )

    def _txn_rollback(self, a: str, b: str, dialect: Dialect) -> tuple[CapStatus, str, Any]:
        tag = 20_000 + secrets.randbelow(10_000)
        raised = False
        try:
            self.x.execute(
                f"{dialect.begin}\n"
                f"  INSERT INTO {self.t(a)} VALUES ({tag}, 'must-rollback');\n"
                f"  UPDATE {self.t(b)} SET v = 'must-rollback' WHERE id = 1;\n"
                f"  {dialect.fail('INJECTED_FAULT', 'doctor rollback probe')}\n"
                f"END;"
            )
        except Exception as exc:
            raised = classify(exc).category.value == "INJECTED_FAULT"
        leaked_a = self._count(f"SELECT count(*) FROM {self.t(a)} WHERE id = {tag}")
        leaked_b = self._count(f"SELECT count(*) FROM {self.t(b)} WHERE v = 'must-rollback'")
        if not raised:
            return CapStatus.ERROR, "injected SIGNAL did not surface as an error", None
        ok = leaked_a == 0 and leaked_b == 0
        return (
            CapStatus.SUPPORTED if ok else CapStatus.UNSUPPORTED,
            f"after SIGNAL: leaked INSERT rows={leaked_a}, leaked UPDATE rows={leaked_b} (both must be 0)",
            {"leaked_insert": leaked_a, "leaked_update": leaked_b},
        )

    def _txn_staging_read(self, a: str, stg: str, dialect: Dialect) -> tuple[CapStatus, str, Any]:
        before = self._count(f"SELECT count(*) FROM {self.t(a)}")
        self.x.execute(f"{dialect.begin}\n  INSERT INTO {self.t(a)} SELECT id + 50000, v FROM {self.t(stg)};\nEND;")
        d = self._count(f"SELECT count(*) FROM {self.t(a)}") - before
        return (
            CapStatus.SUPPORTED if d >= 1 else CapStatus.ERROR,
            f"read non-catalog-managed staging inside txn; inserted {d}",
            None,
        )

    def _txn_merge(self, a: str, dialect: Dialect) -> tuple[CapStatus, str, Any]:
        self.x.execute(
            f"{dialect.begin}\n  MERGE INTO {self.t(a)} AS t USING (SELECT 1 AS id, 'merged' AS v) AS s "
            f"ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = s.v "
            f"WHEN NOT MATCHED BY SOURCE AND t.id = -1 THEN UPDATE SET t.v = 'x';\nEND;"
        )
        n = self._count(f"SELECT count(*) FROM {self.t(a)} WHERE v = 'merged'")
        return (
            CapStatus.SUPPORTED if n >= 1 else CapStatus.ERROR,
            f"MERGE (incl. NOT MATCHED BY SOURCE) in txn: {n}",
            None,
        )

    def _jobs_api(self) -> Capability:
        try:
            from databricks.sdk import WorkspaceClient
        except ImportError:
            return Capability(
                "jobs_api", CapStatus.UNSUPPORTED, "databricks-sdk not installed (pip install dbxbench[jobs])"
            )
        try:
            w = WorkspaceClient()
            me = w.current_user.me()
            ev = f"authenticated as {'service principal' if not me.emails else 'user'}"
            job_id = self.cfg.concurrency.worker_job_id
            if job_id is not None:
                job = w.jobs.get(job_id)
                mcr = job.settings.max_concurrent_runs if job.settings else None
                ev += f"; worker job {job_id} max_concurrent_runs={mcr}"
                if mcr is not None and mcr < max(self.cfg.concurrency.writer_counts):
                    return Capability(
                        "jobs_api", CapStatus.UNSUPPORTED, ev + " < max writer count", docs=DOCS["transactions"]
                    )
            else:
                ev += "; concurrency.worker_job_id not configured"
            return Capability("jobs_api", CapStatus.SUPPORTED, ev)
        except Exception as exc:
            return Capability("jobs_api", CapStatus.UNSUPPORTED, f"{type(exc).__name__}: {str(exc)[:300]}")

    def drop_probe_schema(self) -> None:
        """Drop ONLY the probe schema created by this Prober (name has a random suffix + owner marker)."""
        if not self.schema_created:
            return
        rows = self.x.execute(f"DESCRIBE SCHEMA EXTENDED {quote_ident(self.catalog)}.{quote_ident(self.schema)}")
        text = json.dumps(rows, default=str)
        if self.probe_run_id not in text:
            raise RuntimeError(f"refusing to drop {self.schema}: ownership marker not found")
        self.x.execute(f"DROP SCHEMA {quote_ident(self.catalog)}.{quote_ident(self.schema)} CASCADE")
        self.schema_created = False


# ----------------------------------------------------------------------------- gating
def requirements_for(cfg: BenchConfig, strategy: Strategy, fmt: TableFormat, launcher: Launcher) -> list[str]:
    req: list[str] = ["spark_sql", "results_root_writable"]
    if cfg.compute.transaction_mode is TransactionMode.BEGIN_ATOMIC:
        req += ["begin_atomic_multi_table", "begin_atomic_rollback", "staging_read_in_txn"]
        if fmt is TableFormat.DELTA:
            req += ["managed_delta_create", "catalog_commits_delta"]
            if strategy is Strategy.SCD2_CHANGED_ONLY:
                req.append("merge_in_txn_delta")
        else:
            req += ["managed_iceberg_create", "iceberg_in_txn", "iceberg_in_txn_rollback"]
            if strategy is not Strategy.APPEND_ONLY_FULL:
                req.append("iceberg_dml_update")
            if strategy is Strategy.SCD2_CHANGED_ONLY:
                req.append("merge_in_txn_iceberg")
    else:
        req.append("local_sql_scripting")
        req.append("managed_delta_create" if fmt is TableFormat.DELTA else "managed_iceberg_create")
    if launcher is Launcher.DATABRICKS_JOBS:
        req.append("jobs_api")
    return req


def gate(matrix: CapabilityMatrix, requirements: list[str]) -> tuple[bool, str]:
    missing = [f"{r}={matrix.status(r).value}" for r in requirements if not matrix.supported(r)]
    if missing:
        return False, "unsupported/unverified capabilities: " + ", ".join(missing)
    return True, ""
