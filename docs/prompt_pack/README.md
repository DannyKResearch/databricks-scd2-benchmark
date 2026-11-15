# DBX Research — Claude Prompt Pack

**Purpose:** have Claude implement a rerunnable, configurable **PySpark + Databricks SQL** benchmark of (1) full-snapshot SCD2 versus append-only publication; (2) overlapping multi-table transactions with independent concurrent Spark jobs; and (3) managed Delta versus managed Iceberg performance.

## How to use

1. Read `00_MASTER_PROMPT.md`, then paste its contents into Claude in a new coding session with this whole pack in context.
2. Provide your Databricks environment details (workspace URL via normal deployment configuration, Unity Catalog catalog/schema, runtime, compute and permissions). **Never paste tokens or secrets into Claude prompts.**
3. Ask Claude to implement phases sequentially using prompts `01` through `07`. Require test results and actual changed files after each phase, not architectural essays.
4. Run a tiny smoke test first and only scale up after verifying rollback and historical correctness.
5. Apply `08_REVIEW_AND_ACCEPTANCE.md` as a separate final code-review pass.

The sample YAML is a **desired configuration schema** that Claude should implement; it is not an already-working application. The pack specifies empirical measurements, not fabricated results.

## Primary reference docs (verify live before coding)
- https://docs.databricks.com/aws/en/transactions
- https://docs.databricks.com/aws/en/transactions/transaction-modes
- https://docs.databricks.com/aws/en/transactions/tutorial
- https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-txn-begin-atomic
- https://docs.databricks.com/aws/en/optimizations/isolation/row-level-concurrency
- https://docs.databricks.com/aws/en/iceberg/
- https://docs.databricks.com/aws/en/tables/features/catalog-commits

**Validated design constraint:** transactional write targets must be Unity Catalog managed Delta or Iceberg tables with Catalog Commits enabled; `BEGIN ATOMIC` can be invoked through one `spark.sql()` SQL block on supported compute (DBR 18.0+ on classic clusters, or supported serverless/SQL). External S3 Delta tables may be immutable input/staging, not targets of these transactions. Check Iceberg availability in the actual workspace; mark unavailable cases SKIPPED, not PASS.
