# Databricks notebook source
# MAGIC %md
# MAGIC # dbxbench demo launcher
# MAGIC Simple demonstration only: runs doctor and a tiny single-writer smoke in THIS notebook's session.
# MAGIC It does **not** provide independent-writer evidence (use the `dbxbench_suite` job, which launches
# MAGIC separate `dbxbench_worker` job runs). Requires DBR 18.0+ and the dbxbench wheel installed.

# COMMAND ----------
dbutils.widgets.text("config", "/Workspace/Users/<you>/dbxbench/config/example.yaml")  # noqa: F821
dbutils.widgets.text("catalog", "")  # noqa: F821
dbutils.widgets.text("results_root", "/Volumes/<catalog>/<schema>/<volume>/runs")  # noqa: F821

# COMMAND ----------
from dbxbench.cli import main

args = ["--config", dbutils.widgets.get("config"), "--catalog", dbutils.widgets.get("catalog"),  # noqa: F821
        "--results-root", dbutils.widgets.get("results_root")]  # noqa: F821
main(["doctor", *args])

# COMMAND ----------
# Tiny single-writer smoke: performance + fault cells only (no contention in a notebook).
main(["smoke", *args, "--tables", "2", "--rows", "1000", "--publications", "3", "--writers", "1",
      "--kind", "performance", "--kind", "fault"])
