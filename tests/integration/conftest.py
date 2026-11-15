"""Session-wide local Spark + Delta session for the ``spark``-marked integration tests (skipped if PySpark
or delta-spark is not installed).
"""

from __future__ import annotations

import pytest


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark", reason="NOT_RUN: pip install -e '.[local]' for Spark integration tests")
    pytest.importorskip("delta", reason="NOT_RUN: delta-spark not installed")
    from dbxbench.spark_session import local_spark

    s = local_spark(tmp_path_factory.mktemp("wh"), app_name="dbxbench-tests", cores=2)
    yield s
    s.stop()
