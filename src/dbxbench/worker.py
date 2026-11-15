"""Independent writer process: one Databricks job run (primary) or one local OS process (harness test).

Lifecycle: arrival -> own Spark session -> prevalidate every assigned publication (untimed) ->
barrier ready -> wait go (bounded, fail closed) -> sleep until go_at + start offset -> publish
assigned publications in order via the atomic engine -> done. Every step is a telemetry event.
No data generation happens here; all writers read the same immutable pre-generated staging tables.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import socket
import sys
import threading
from pathlib import Path
from typing import Any

from .barrier import BarrierAborted, BarrierTimeout, FileBarrier
from .config import BenchConfig, FaultScenario, OrderingMode, Strategy, TransactionMode
from .engine import EngineFaults, PublicationEngine
from .executor import SparkSqlExecutor, SqlLog
from .logging_setup import configure_logging
from .model import Publication, publication_ts
from .naming import CellTables
from .retry import RetryPolicy, stable_seed
from .spark_session import LocalCatalog, get_spark, local_spark
from .sqldialect import dialect_for
from .sqlutil import TableRef
from .telemetry import TelemetrySink

log = logging.getLogger("dbxbench.worker")

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_BARRIER = 3


def _cluster_id(spark: Any) -> str | None:
    try:
        return str(spark.conf.get("spark.databricks.clusterUsageTags.clusterId"))
    except Exception:
        return None


def run_worker(plan: dict[str, Any], job_run_id: str | None = None, task_run_id: str | None = None) -> int:
    cfg = BenchConfig.model_validate(plan["config"])
    writer_id = plan["writer_id"]
    tel = TelemetrySink(
        Path(plan["telemetry_path"]),
        run_id=plan["run_id"],
        cell_key=plan["cell_key"],
        writer_id=writer_id,
        job_run_id=job_run_id,
        task_run_id=task_run_id,
        pid=os.getpid(),
        host=socket.gethostname(),
    )
    tel.emit("arrival", launcher=plan.get("launcher"), submitted_at=plan.get("submitted_at"))
    try:
        if cfg.is_local:
            spark = local_spark(
                Path(plan["local_warehouse"]), app_name=f"dbxbench-{writer_id}", cores=1, tz=cfg.compute.timezone
            )
            LocalCatalog(Path(plan["local_catalog"])).register_all(spark)
        else:
            spark = get_spark(cfg)
        tel.emit(
            "session_ready",
            spark_version=getattr(spark, "version", None),
            cluster_id=_cluster_id(spark),
            spark_app_id=_app_id(spark),
        )
        executor = SparkSqlExecutor(spark, SqlLog(Path(plan["sql_log"]) if plan.get("sql_log") else None))
        tables = CellTables(
            plan["cell_key"],
            tuple(TableRef.parse(t) for t in plan["tables"]["targets"]),
            TableRef.parse(plan["tables"]["ledger"]),
            TableRef.parse(plan["tables"]["fence"]),
        )
        staging = tuple(TableRef.parse(s) for s in plan["staging"])
        w = cfg.workload
        cc = cfg.concurrency
        engine = PublicationEngine(
            executor=executor,
            dialect=dialect_for(cfg),
            run_id=plan["run_id"],
            cell_key=plan["cell_key"],
            strategy=Strategy(plan["strategy"]),
            ordering=OrderingMode(plan["ordering"]),
            writer_id=writer_id,
            tables=tables,
            staging=staging,
            policy=RetryPolicy(
                cc.max_attempts,
                cc.initial_backoff_ms,
                cc.max_backoff_ms,
                cc.overall_deadline_seconds,
                cc.per_attempt_timeout_seconds,
                seed=stable_seed(cfg.project.seed, plan["cell_key"], writer_id),
            ),
            telemetry=tel,
            expected_ts=lambda q: publication_ts(w.publication_timestamp_origin_utc, w.publication_interval_millis, q),
            staging_count_check=w.in_transaction_staging_count_check,
        )
        pubs = [Publication.from_json(p) for p in plan["publications"]]
        for p in pubs:
            rep = engine.prevalidate(p)
            if not rep.ok:
                tel.emit("prevalidation_failed", publication_id=p.publication_id, problems=rep.problems[:10])
        faults = set(plan.get("faults", []))
        b = plan["barrier"]
        if b["enabled"]:
            barrier = FileBarrier(Path(b["root"]), poll_seconds=b["poll_seconds"])
            barrier.mark_ready(writer_id, pid=os.getpid(), job_run_id=job_run_id)
            tel.emit("ready")
            try:
                go = barrier.wait_go(b["timeout_seconds"])
            except (BarrierTimeout, BarrierAborted) as exc:
                tel.emit("barrier_failed_closed", reason=str(exc)[:300])
                return EXIT_BARRIER
            target = FileBarrier.go_time(go, int(plan.get("start_offset_ms", 0)))
            tel.emit("go_received", go_at=go["go_at"], target_start=target.isoformat())
            slept = FileBarrier.sleep_until(target)
            tel.emit("start", slept_s=slept)
        else:
            tel.emit("ready")
            tel.emit("start", slept_s=0.0)
        for i, p in enumerate(pubs):
            ef = EngineFaults()
            if i == 0 and FaultScenario.WRITER_TERMINATION.value in faults:
                ef = EngineFaults(on_first_submit=_terminate_soon(tel, float(plan.get("terminate_after_s", 0.5))))
            engine.publish(p, ef)
        tel.emit("done")
        engine.close()
        return EXIT_OK
    except Exception as exc:  # recorded; the harness reports FAIL for any unhandled worker error
        tel.emit("worker_error", error_type=type(exc).__name__, error=str(exc)[:2000])
        log.exception("worker failed")
        return EXIT_ERROR


def _app_id(spark: Any) -> str | None:
    try:
        return str(spark.sparkContext.applicationId)
    except Exception:
        return None


def _terminate_soon(tel: TelemetrySink, after_s: float) -> Any:
    def arm() -> None:
        def kill() -> None:
            tel.emit("simulated_writer_termination", after_s=after_s)
            os._exit(137)

        threading.Timer(after_s, kill).start()

    return arm


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="dbxbench-worker")
    ap.add_argument("--plan", required=True, help="path to the writer plan JSON")
    ap.add_argument("--job-run-id", default=None)
    ap.add_argument("--task-run-id", default=None)
    args = ap.parse_args(argv)
    configure_logging()
    plan = json.loads(Path(args.plan).read_text(encoding="utf-8"))
    if plan["config"]["compute"]["transaction_mode"] == TransactionMode.LOCAL_NONATOMIC_EMULATION.value:
        log.warning("local_nonatomic_emulation: this writer's results are NOT evidence of atomicity")
    return run_worker(plan, args.job_run_id, args.task_run_id)


if __name__ == "__main__":
    sys.exit(main())
