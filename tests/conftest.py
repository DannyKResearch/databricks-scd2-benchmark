"""Shared pytest fixtures and the guard that keeps live Databricks tests honest.

Tests marked ``databricks`` only run on a Databricks runtime with ``DBXBENCH_DATABRICKS_TESTS=1``.
Everywhere else they are SKIPPED with a ``NOT_RUN`` reason, so a laptop run can never report them as passing.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "config" / "example.yaml"
LOCAL = ROOT / "config" / "local_tiny.yaml"


@pytest.fixture
def example_cfg():
    from dbxbench.config import load_config

    return load_config(EXAMPLE)


@pytest.fixture
def local_cfg(tmp_path):
    from dbxbench.config import load_config

    return load_config(LOCAL, {"results_root": str(tmp_path / "results")})


def pytest_collection_modifyitems(config, items):
    """Databricks tests never silently pass off-platform: they are SKIPPED with an explicit NOT_RUN reason."""
    # Serverless sets DATABRICKS_RUNTIME_VERSION to a "client.x" value; /databricks is a fallback marker.
    platform = "DATABRICKS_RUNTIME_VERSION" in os.environ or os.path.isdir("/databricks/python")
    on_dbr = platform and os.environ.get("DBXBENCH_DATABRICKS_TESTS") == "1"
    for item in items:
        if "databricks" in item.keywords and not on_dbr:
            item.add_marker(
                pytest.mark.skip(reason="NOT_RUN: requires Databricks runtime + DBXBENCH_DATABRICKS_TESTS=1")
            )
