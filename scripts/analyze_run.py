"""Offline re-analysis of a results directory copied from a Volume (no Spark, no workspace access).

python scripts/analyze_run.py <results_root>/<run_id> [--no-charts]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> int:
    from dbxbench.analysis import analyse_run
    from dbxbench.report import build_report
    from dbxbench.results import RunPaths

    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir")
    ap.add_argument("--no-charts", action="store_true")
    a = ap.parse_args()
    paths = RunPaths(Path(a.run_dir))
    s = analyse_run(paths)
    print(build_report(paths, charts=not a.no_charts))
    print(s["correctness"])
    return 1 if s["correctness"]["verdict"] == "FAIL" else 0


if __name__ == "__main__":
    sys.exit(main())
