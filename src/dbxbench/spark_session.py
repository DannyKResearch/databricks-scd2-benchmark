"""Spark session acquisition for Databricks and for local OSS Spark + Delta (tiny/emulation mode)."""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

from .config import BenchConfig
from .sqlutil import TableRef, quote_ident, sql_string

log = logging.getLogger(__name__)


def on_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _sanitize_spark_home() -> None:
    """A system SPARK_HOME for a different Spark version breaks pip-installed PySpark."""
    home = os.environ.get("SPARK_HOME")
    if not home:
        return
    try:
        import pyspark

        pkg = Path(pyspark.__file__).parent
        if not Path(home).resolve().is_relative_to(pkg.resolve()):
            log.warning("ignoring SPARK_HOME=%s (does not match pip pyspark %s)", home, pyspark.__version__)
            os.environ.pop("SPARK_HOME")
    except Exception:  # pragma: no cover
        os.environ.pop("SPARK_HOME", None)


def local_spark(warehouse: Path, app_name: str = "dbxbench", cores: int = 2, tz: str = "UTC") -> Any:
    _sanitize_spark_home()
    # Python workers must use this interpreter, not whatever `python3` is first on PATH.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse.mkdir(parents=True, exist_ok=True)
    builder = (
        SparkSession.builder.master(f"local[{cores}]")
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.warehouse.dir", str(warehouse.resolve()))
        .config("spark.sql.session.timeZone", tz)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.databricks.delta.snapshotPartitions", "2")  # default 50 => 50-task log replays
        .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "2")
        .config("spark.default.parallelism", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.databricks.delta.properties.defaults.enableDeletionVectors", "true")
        .config("spark.driver.memory", "1g")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def get_spark(cfg: BenchConfig, run_dir: Path | None = None) -> Any:
    if cfg.is_local:
        root = (run_dir / "local_warehouse") if run_dir else Path("./results/local_warehouse")
        return local_spark(root, tz=cfg.compute.timezone)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    try:
        spark.conf.set("spark.sql.session.timeZone", cfg.compute.timezone)
    except Exception as exc:  # pragma: no cover - some serverless confs are read-only
        log.warning("could not set session time zone: %s", exc)
    return spark


class LocalCatalog:
    """Local mode uses an in-memory catalog per process; tables live at fixed paths and every process
    re-registers them by identifier, so SQL text uses the same table identifiers as on Databricks."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> dict[str, str]:
        if not self.path.exists():
            return {}
        data: dict[str, str] = json.loads(self.path.read_text(encoding="utf-8"))
        return data

    def location_for(self, warehouse: Path, ref: TableRef) -> str:
        return str((warehouse / "tables" / ref.schema / ref.name).resolve())

    def record(self, ref: TableRef, location: str) -> None:
        data = self.load()
        data[ref.plain] = location
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=1, sort_keys=True), encoding="utf-8")
        tmp.replace(self.path)

    def register_all(self, spark: Any) -> int:
        n = 0
        schemas: set[str] = set()
        for plain, loc in sorted(self.load().items()):
            ref = TableRef.parse(plain)
            if ref.schema not in schemas:
                spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(ref.catalog)}.{quote_ident(ref.schema)}")
                schemas.add(ref.schema)
            spark.sql(f"CREATE TABLE IF NOT EXISTS {ref.sql} USING DELTA LOCATION {sql_string(loc)}")
            n += 1
        return n
