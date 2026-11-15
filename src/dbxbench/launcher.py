"""Launchers for N independent writers.

* ``DatabricksJobsLauncher`` (primary evidence): N separate runs of the bundle's ``dbxbench_worker``
  job via the Jobs API ``run-now``; each run has its own driver/Spark session (job cluster or
  serverless). Submissions are issued concurrently from a thread pool so the launcher is not a hidden
  serialisation point; per-writer submit timestamps are recorded so submission spread is measurable.
* ``LocalProcessLauncher`` (harness self-test): N separate OS processes, each with its own JVM and
  SparkSession. Proves barrier/telemetry/analysis mechanics locally; NOT Databricks evidence.
"""

from __future__ import annotations

import concurrent.futures as cf
import hashlib
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from .sqlutil import iso, utc_now


@dataclass
class LaunchHandle:
    writer_id: str
    plan_path: str
    submit_start: str
    submit_end: str | None = None
    external_id: str | None = None  # pid or Databricks run_id
    final_state: str | None = None
    exit_code: int | None = None
    detail: dict[str, Any] = field(default_factory=dict)
    proc: Any = None

    def to_json(self) -> dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if k != "proc"}


class Launcher(Protocol):
    name: str

    def launch(self, plans: dict[str, Path]) -> list[LaunchHandle]: ...

    def wait(self, handles: list[LaunchHandle], timeout_s: float) -> list[LaunchHandle]: ...


class LocalProcessLauncher:
    name = "local_process"

    def __init__(self, log_dir: Path) -> None:
        self.log_dir = log_dir

    def launch(self, plans: dict[str, Path]) -> list[LaunchHandle]:
        self.log_dir.mkdir(parents=True, exist_ok=True)
        handles = []
        for writer_id, path in sorted(plans.items()):
            h = LaunchHandle(writer_id, str(path), iso(utc_now()))
            out = (self.log_dir / f"{writer_id}.log").open("w", encoding="utf-8")
            h.proc = subprocess.Popen(
                [sys.executable, "-m", "dbxbench.worker", "--plan", str(path), "--job-run-id", f"local-{writer_id}"],
                stdout=out,
                stderr=subprocess.STDOUT,
            )
            h.submit_end = iso(utc_now())
            h.external_id = str(h.proc.pid)
            handles.append(h)
        return handles

    def wait(self, handles: list[LaunchHandle], timeout_s: float) -> list[LaunchHandle]:
        deadline = time.monotonic() + timeout_s
        for h in handles:
            remaining = max(0.1, deadline - time.monotonic())
            try:
                h.exit_code = h.proc.wait(timeout=remaining)
                h.final_state = "EXITED"
            except subprocess.TimeoutExpired:
                h.proc.kill()
                h.exit_code = h.proc.wait()
                h.final_state = "KILLED_TIMEOUT"
        return handles


class DatabricksJobsLauncher:
    name = "databricks_jobs"

    def __init__(self, job_id: int, poll_seconds: float = 10.0, client: Any = None) -> None:
        self.job_id = job_id
        self.poll_seconds = poll_seconds
        if client is None:
            from databricks.sdk import WorkspaceClient  # auth: env vars / profile / job identity

            client = WorkspaceClient()
        self.w = client

    def _submit(self, writer_id: str, path: Path) -> LaunchHandle:
        h = LaunchHandle(writer_id, str(path), iso(utc_now()))
        # idempotency_token: a retried submission cannot start a second run for the same writer plan.
        token = hashlib.sha256(str(path).encode()).hexdigest()[:48]
        resp = self.w.jobs.run_now(
            job_id=self.job_id,
            job_parameters={"plan_path": str(path), "writer_id": writer_id},
            idempotency_token=token,
        )
        h.submit_end = iso(utc_now())
        h.external_id = str(resp.run_id)  # Wait[Run] exposes run_id from the run-now response
        h.detail["idempotency_token"] = token
        return h

    def launch(self, plans: dict[str, Path]) -> list[LaunchHandle]:
        with cf.ThreadPoolExecutor(max_workers=max(1, min(32, len(plans)))) as pool:
            futs = [pool.submit(self._submit, wid, p) for wid, p in sorted(plans.items())]
            return [f.result() for f in futs]

    def wait(self, handles: list[LaunchHandle], timeout_s: float) -> list[LaunchHandle]:
        deadline = time.monotonic() + timeout_s
        pending = {h.external_id: h for h in handles}
        while pending and time.monotonic() < deadline:
            for rid, h in list(pending.items()):
                run = self.w.jobs.get_run(int(rid))  # type: ignore[arg-type]
                st = run.state
                life = st.life_cycle_state.value if st and st.life_cycle_state else None
                if life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                    h.final_state = st.result_state.value if st and st.result_state else life
                    h.detail = {
                        "life_cycle_state": life,
                        "state_message": st.state_message if st else None,
                        "start_time_ms": run.start_time,
                        "end_time_ms": run.end_time,
                        "cluster_id": run.cluster_instance.cluster_id if run.cluster_instance else None,
                        "run_page_url": run.run_page_url,
                    }
                    del pending[rid]
            if pending:
                time.sleep(self.poll_seconds)
        for h in pending.values():
            h.final_state = "TIMEOUT_WAITING"
            try:
                self.w.jobs.cancel_run(int(h.external_id))  # type: ignore[arg-type]
                h.detail["cancel_requested"] = True
            except Exception as exc:
                h.detail["cancel_error"] = str(exc)[:200]
        return handles
