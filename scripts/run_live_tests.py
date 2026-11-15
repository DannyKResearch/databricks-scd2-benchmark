"""Entry point for the dbxbench_live_tests job: runs `pytest -m databricks` from the bundle's files.

Exits non-zero on any failure. Skips are reported in the output (-ra) so a skipped live test can
never masquerade as a pass.
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile


def main() -> int:
    import pytest

    args = sys.argv[1:]
    # KEY=VALUE arguments become environment variables (serverless jobs have no spark_env_vars).
    for kv in [x for x in args if "=" in x and x.split("=", 1)[0].startswith("DBXBENCH_")]:
        k, v = kv.split("=", 1)
        os.environ[k] = v
    positional = [x for x in args if "=" not in x]
    root = positional[0] if positional else os.getcwd()
    # /Workspace files reject __pycache__ and pytest cache writes (Errno 95), so run from a local copy.
    sys.dont_write_bytecode = True
    os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
    work = tempfile.mkdtemp(prefix="dbxbench_live_")
    for name in ("tests", "config"):
        shutil.copytree(os.path.join(root, name), os.path.join(work, name))
    shutil.copy2(os.path.join(root, "pyproject.toml"), work)
    os.chdir(work)
    sys.path.insert(0, work)
    return int(pytest.main(["-m", "databricks", "-ra", "-p", "no:cacheprovider", "tests/databricks"]))


if __name__ == "__main__":
    sys.exit(main())
