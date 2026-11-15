# SQL emitted by dbxbench (generated, not hand-written)

Produced by `dbxbench init --config config/example.yaml --run-id r20261114t000000_example --dry-run --tables 2 --writers 2 --contention same_scope --ordering off,sequenced`. Nothing was executed.

DDL runs outside any transaction. Each `BEGIN ATOMIC … END;` block is submitted as **one** `spark.sql()` call. Only the first 6 of 135 DDL statements are shown; all publication blocks are shown (one per kind/strategy/format).

```sql
-- ===== DRY RUN: nothing is executed; run-owned namespace: dbx_research.scd2_bench_r20261114t000000_example 
-- 26 cells; estimated rows written ~168,000; ~0.048 GB uncompressed

-- DDL (excerpt)
CREATE SCHEMA `dbx_research`.`scd2_bench_r20261114t000000_example` COMMENT 'dbxbench run-owned benchmark schema' WITH DBPROPERTIES ('dbxbench.owner' = 'dbxbench', 'dbxbench.run_id' = 'r20261114t000000_example');

CREATE TABLE `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_d00_t01` (
  `dataset_scope` STRING,
  `business_key` STRING,
  `attr_int` BIGINT,
  `attr_decimal` DECIMAL(18,4),
  `attr_category` STRING,
  `attr_flag` BOOLEAN,
  `attr_text` STRING,
  `payload_hash` STRING,
  `publication_id` STRING,
  `publication_sequence` BIGINT
) USING DELTA
PARTITIONED BY (`publication_sequence`)
TBLPROPERTIES ('dbxbench.owner' = 'dbxbench', 'dbxbench.run_id' = 'r20261114t000000_example', 'delta.enableDeletionVectors' = 'true');

CREATE TABLE `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_d00_t02` (
  `dataset_scope` STRING,
  `business_key` STRING,
  `attr_int` BIGINT,
  `attr_decimal` DECIMAL(18,4),
  `attr_category` STRING,
  `attr_flag` BOOLEAN,
  `attr_text` STRING,
  `payload_hash` STRING,
  `publication_id` STRING,
  `publication_sequence` BIGINT
) USING DELTA
PARTITIONED BY (`publication_sequence`)
TBLPROPERTIES ('dbxbench.owner' = 'dbxbench', 'dbxbench.run_id' = 'r20261114t000000_example', 'delta.enableDeletionVectors' = 'true');

CREATE TABLE `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_dc_t01` (
  `dataset_scope` STRING,
  `business_key` STRING,
  `attr_int` BIGINT,
  `attr_decimal` DECIMAL(18,4),
  `attr_category` STRING,
  `attr_flag` BOOLEAN,
  `attr_text` STRING,
  `payload_hash` STRING,
  `publication_id` STRING,
  `publication_sequence` BIGINT
) USING DELTA
PARTITIONED BY (`publication_sequence`)
TBLPROPERTIES ('dbxbench.owner' = 'dbxbench', 'dbxbench.run_id' = 'r20261114t000000_example', 'delta.enableDeletionVectors' = 'true');

CREATE TABLE `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_dc_t02` (
  `dataset_scope` STRING,
  `business_key` STRING,
  `attr_int` BIGINT,
  `attr_decimal` DECIMAL(18,4),
  `attr_category` STRING,
  `attr_flag` BOOLEAN,
  `attr_text` STRING,
  `payload_hash` STRING,
  `publication_id` STRING,
  `publication_sequence` BIGINT
) USING DELTA
PARTITIONED BY (`publication_sequence`)
TBLPROPERTIES ('dbxbench.owner' = 'dbxbench', 'dbxbench.run_id' = 'r20261114t000000_example', 'delta.enableDeletionVectors' = 'true');

CREATE TABLE `dbx_research`.`scd2_bench_r20261114t000000_example`.`c010_scd2_ice_t01` (
  `dataset_scope` STRING,
  `business_key` STRING,
  `attr_int` BIGINT,
  `attr_decimal` DECIMAL(18,4),
  `attr_category` STRING,
  `attr_flag` BOOLEAN,
  `attr_text` STRING,
  `payload_hash` STRING,
  `effective_from_date` TIMESTAMP,
  `effective_to_date` TIMESTAMP,
  `publication_id` STRING,
  `publication_sequence` BIGINT,
  `publication_ts` TIMESTAMP
) USING ICEBERG
TBLPROPERTIES ('dbxbench.owner' = 'dbxbench', 'dbxbench.run_id' = 'r20261114t000000_example');

-- ===== publication transaction (performance:scd2_full:iceberg, cell c010_scd2_ice, scope_0001_p000001) — submitted as ONE spark.sql() call
BEGIN ATOMIC
  DECLARE v_existing BIGINT DEFAULT 0;
  DECLARE v_last_seq BIGINT DEFAULT NULL;
  DECLARE v_fence_ok BIGINT DEFAULT 0;
  DECLARE v_staged BIGINT DEFAULT 0;
  SET v_existing = (SELECT count(*) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`c010_scd2_ice_ledger` WHERE `dataset_scope` = 'scope_0001' AND `publication_id` = 'scope_0001_p000001');
  IF v_existing > 0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_DUPLICATE_PUBLICATION: scope=scope_0001 publication=scope_0001_p000001 seq=1'; END IF;
  SET v_last_seq = (SELECT max(`last_sequence`) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`c010_scd2_ice_fence` WHERE `dataset_scope` = 'scope_0001');
  IF v_last_seq IS NULL THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_FENCE_MISSING: scope=scope_0001 publication=scope_0001_p000001 seq=1'; END IF;
  IF v_last_seq >= 1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_STALE_SEQUENCE: scope=scope_0001 publication=scope_0001_p000001 seq=1'; END IF;
  IF v_last_seq < 1 - 1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_SEQUENCE_NOT_READY: scope=scope_0001 publication=scope_0001_p000001 seq=1'; END IF;
  UPDATE `dbx_research`.`scd2_bench_r20261114t000000_example`.`c010_scd2_ice_fence` SET `last_sequence` = 1, `last_publication_id` = 'scope_0001_p000001', `last_publication_ts` = TIMESTAMP'2026-01-01 00:00:01.000000+00:00', `commit_count` = `commit_count` + 1, `updated_by_attempt` = 'w000-dryrun' WHERE `dataset_scope` = 'scope_0001';
  SET v_fence_ok = (SELECT count(*) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`c010_scd2_ice_fence` WHERE `dataset_scope` = 'scope_0001' AND `updated_by_attempt` = 'w000-dryrun');
  IF v_fence_ok <> 1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_FENCE_UPDATE_NOT_VISIBLE: scope=scope_0001 publication=scope_0001_p000001 seq=1'; END IF;
  SET v_staged = (SELECT count(*) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_d00_t01` WHERE `dataset_scope` = 'scope_0001' AND `publication_sequence` = 1);
  IF v_staged <> 1000 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_STAGING_COUNT_MISMATCH: scope=scope_0001 publication=scope_0001_p000001 seq=1 table=c010_scd2_ice_t01'; END IF;
  UPDATE `dbx_research`.`scd2_bench_r20261114t000000_example`.`c010_scd2_ice_t01` SET `effective_to_date` = TIMESTAMP'2026-01-01 00:00:01.000000+00:00' WHERE `dataset_scope` = 'scope_0001' AND `effective_to_date` IS NULL;
  INSERT INTO `dbx_research`.`scd2_bench_r20261114t000000_example`.`c010_scd2_ice_t01` (`dataset_scope`, `business_key`, `attr_int`, `attr_decimal`, `attr_category`, `attr_flag`, `attr_text`, `payload_hash`, `effective_from_date`, `effective_to_date`, `publication_id`, `publication_sequence`, `publication_ts`) SELECT `dataset_scope`, `business_key`, `attr_int`, `attr_decimal`, `attr_category`, `attr_flag`, `attr_text`, `payload_hash`, TIMESTAMP'2026-01-01 00:00:01.000000+00:00', CAST(NULL AS TIMESTAMP), 'scope_0001_p000001', 1, TIMESTAMP'2026-01-01 00:00:01.000000+00:00' FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_d00_t01` WHERE `dataset_scope` = 'scope_0001' AND `publication_sequence` = 1;
  SET v_staged = (SELECT count(*) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_d00_t02` WHERE `dataset_scope` = 'scope_0001' AND `publication_sequence` = 1);
  IF v_staged <> 1000 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_STAGING_COUNT_MISMATCH: scope=scope_0001 publication=scope_0001_p000001 seq=1 table=c010_scd2_ice_t02'; END IF;
  UPDATE `dbx_research`.`scd2_bench_r20261114t000000_example`.`c010_scd2_ice_t02` SET `effective_to_date` = TIMESTAMP'2026-01-01 00:00:01.000000+00:00' WHERE `dataset_scope` = 'scope_0001' AND `effective_to_date` IS NULL;
  INSERT INTO `dbx_research`.`scd2_bench_r20261114t000000_example`.`c010_scd2_ice_t02` (`dataset_scope`, `business_key`, `attr_int`, `attr_decimal`, `attr_category`, `attr_flag`, `attr_text`, `payload_hash`, `effective_from_date`, `effective_to_date`, `publication_id`, `publication_sequence`, `publication_ts`) SELECT `dataset_scope`, `business_key`, `attr_int`, `attr_decimal`, `attr_category`, `attr_flag`, `attr_text`, `payload_hash`, TIMESTAMP'2026-01-01 00:00:01.000000+00:00', CAST(NULL AS TIMESTAMP), 'scope_0001_p000001', 1, TIMESTAMP'2026-01-01 00:00:01.000000+00:00' FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_d00_t02` WHERE `dataset_scope` = 'scope_0001' AND `publication_sequence` = 1;
  INSERT INTO `dbx_research`.`scd2_bench_r20261114t000000_example`.`c010_scd2_ice_ledger` (`run_id`, `cell_key`, `dataset_scope`, `publication_id`, `publication_sequence`, `publication_ts`, `strategy`, `ordering_mode`, `writer_id`, `attempt_token`, `attempt_number`, `table_names`, `rows_per_table`, `ledger_write_ts`) VALUES ('r20261114t000000_example', 'c010_scd2_ice', 'scope_0001', 'scope_0001_p000001', 1, TIMESTAMP'2026-01-01 00:00:01.000000+00:00', 'scd2_full', 'sequenced', 'w000', 'w000-dryrun', 1, array('c010_scd2_ice_t01', 'c010_scd2_ice_t02'), 1000, current_timestamp());
END;

-- ===== publication transaction (performance:scd2_full:delta, cell c005_scd2_dlt, scope_0001_p000001) — submitted as ONE spark.sql() call
BEGIN ATOMIC
  DECLARE v_existing BIGINT DEFAULT 0;
  DECLARE v_last_seq BIGINT DEFAULT NULL;
  DECLARE v_fence_ok BIGINT DEFAULT 0;
  DECLARE v_staged BIGINT DEFAULT 0;
  SET v_existing = (SELECT count(*) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`c005_scd2_dlt_ledger` WHERE `dataset_scope` = 'scope_0001' AND `publication_id` = 'scope_0001_p000001');
  IF v_existing > 0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_DUPLICATE_PUBLICATION: scope=scope_0001 publication=scope_0001_p000001 seq=1'; END IF;
  SET v_last_seq = (SELECT max(`last_sequence`) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`c005_scd2_dlt_fence` WHERE `dataset_scope` = 'scope_0001');
  IF v_last_seq IS NULL THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_FENCE_MISSING: scope=scope_0001 publication=scope_0001_p000001 seq=1'; END IF;
  IF v_last_seq >= 1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_STALE_SEQUENCE: scope=scope_0001 publication=scope_0001_p000001 seq=1'; END IF;
  IF v_last_seq < 1 - 1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_SEQUENCE_NOT_READY: scope=scope_0001 publication=scope_0001_p000001 seq=1'; END IF;
  UPDATE `dbx_research`.`scd2_bench_r20261114t000000_example`.`c005_scd2_dlt_fence` SET `last_sequence` = 1, `last_publication_id` = 'scope_0001_p000001', `last_publication_ts` = TIMESTAMP'2026-01-01 00:00:01.000000+00:00', `commit_count` = `commit_count` + 1, `updated_by_attempt` = 'w000-dryrun' WHERE `dataset_scope` = 'scope_0001';
  SET v_fence_ok = (SELECT count(*) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`c005_scd2_dlt_fence` WHERE `dataset_scope` = 'scope_0001' AND `updated_by_attempt` = 'w000-dryrun');
  IF v_fence_ok <> 1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_FENCE_UPDATE_NOT_VISIBLE: scope=scope_0001 publication=scope_0001_p000001 seq=1'; END IF;
  SET v_staged = (SELECT count(*) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_d00_t01` WHERE `dataset_scope` = 'scope_0001' AND `publication_sequence` = 1);
  IF v_staged <> 1000 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_STAGING_COUNT_MISMATCH: scope=scope_0001 publication=scope_0001_p000001 seq=1 table=c005_scd2_dlt_t01'; END IF;
  UPDATE `dbx_research`.`scd2_bench_r20261114t000000_example`.`c005_scd2_dlt_t01` SET `effective_to_date` = TIMESTAMP'2026-01-01 00:00:01.000000+00:00' WHERE `dataset_scope` = 'scope_0001' AND `effective_to_date` IS NULL;
  INSERT INTO `dbx_research`.`scd2_bench_r20261114t000000_example`.`c005_scd2_dlt_t01` (`dataset_scope`, `business_key`, `attr_int`, `attr_decimal`, `attr_category`, `attr_flag`, `attr_text`, `payload_hash`, `effective_from_date`, `effective_to_date`, `publication_id`, `publication_sequence`, `publication_ts`) SELECT `dataset_scope`, `business_key`, `attr_int`, `attr_decimal`, `attr_category`, `attr_flag`, `attr_text`, `payload_hash`, TIMESTAMP'2026-01-01 00:00:01.000000+00:00', CAST(NULL AS TIMESTAMP), 'scope_0001_p000001', 1, TIMESTAMP'2026-01-01 00:00:01.000000+00:00' FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_d00_t01` WHERE `dataset_scope` = 'scope_0001' AND `publication_sequence` = 1;
  SET v_staged = (SELECT count(*) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_d00_t02` WHERE `dataset_scope` = 'scope_0001' AND `publication_sequence` = 1);
  IF v_staged <> 1000 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_STAGING_COUNT_MISMATCH: scope=scope_0001 publication=scope_0001_p000001 seq=1 table=c005_scd2_dlt_t02'; END IF;
  UPDATE `dbx_research`.`scd2_bench_r20261114t000000_example`.`c005_scd2_dlt_t02` SET `effective_to_date` = TIMESTAMP'2026-01-01 00:00:01.000000+00:00' WHERE `dataset_scope` = 'scope_0001' AND `effective_to_date` IS NULL;
  INSERT INTO `dbx_research`.`scd2_bench_r20261114t000000_example`.`c005_scd2_dlt_t02` (`dataset_scope`, `business_key`, `attr_int`, `attr_decimal`, `attr_category`, `attr_flag`, `attr_text`, `payload_hash`, `effective_from_date`, `effective_to_date`, `publication_id`, `publication_sequence`, `publication_ts`) SELECT `dataset_scope`, `business_key`, `attr_int`, `attr_decimal`, `attr_category`, `attr_flag`, `attr_text`, `payload_hash`, TIMESTAMP'2026-01-01 00:00:01.000000+00:00', CAST(NULL AS TIMESTAMP), 'scope_0001_p000001', 1, TIMESTAMP'2026-01-01 00:00:01.000000+00:00' FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_d00_t02` WHERE `dataset_scope` = 'scope_0001' AND `publication_sequence` = 1;
  INSERT INTO `dbx_research`.`scd2_bench_r20261114t000000_example`.`c005_scd2_dlt_ledger` (`run_id`, `cell_key`, `dataset_scope`, `publication_id`, `publication_sequence`, `publication_ts`, `strategy`, `ordering_mode`, `writer_id`, `attempt_token`, `attempt_number`, `table_names`, `rows_per_table`, `ledger_write_ts`) VALUES ('r20261114t000000_example', 'c005_scd2_dlt', 'scope_0001', 'scope_0001_p000001', 1, TIMESTAMP'2026-01-01 00:00:01.000000+00:00', 'scd2_full', 'sequenced', 'w000', 'w000-dryrun', 1, array('c005_scd2_dlt_t01', 'c005_scd2_dlt_t02'), 1000, current_timestamp());
END;

-- ===== publication transaction (performance:append_only_full:iceberg, cell c012_app_ice, scope_0001_p000001) — submitted as ONE spark.sql() call
BEGIN ATOMIC
  DECLARE v_existing BIGINT DEFAULT 0;
  DECLARE v_last_seq BIGINT DEFAULT NULL;
  DECLARE v_fence_ok BIGINT DEFAULT 0;
  DECLARE v_staged BIGINT DEFAULT 0;
  SET v_existing = (SELECT count(*) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`c012_app_ice_ledger` WHERE `dataset_scope` = 'scope_0001' AND `publication_id` = 'scope_0001_p000001');
  IF v_existing > 0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_DUPLICATE_PUBLICATION: scope=scope_0001 publication=scope_0001_p000001 seq=1'; END IF;
  SET v_last_seq = (SELECT max(`last_sequence`) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`c012_app_ice_fence` WHERE `dataset_scope` = 'scope_0001');
  IF v_last_seq IS NULL THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_FENCE_MISSING: scope=scope_0001 publication=scope_0001_p000001 seq=1'; END IF;
  IF v_last_seq >= 1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_STALE_SEQUENCE: scope=scope_0001 publication=scope_0001_p000001 seq=1'; END IF;
  IF v_last_seq < 1 - 1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_SEQUENCE_NOT_READY: scope=scope_0001 publication=scope_0001_p000001 seq=1'; END IF;
  UPDATE `dbx_research`.`scd2_bench_r20261114t000000_example`.`c012_app_ice_fence` SET `last_sequence` = 1, `last_publication_id` = 'scope_0001_p000001', `last_publication_ts` = TIMESTAMP'2026-01-01 00:00:01.000000+00:00', `commit_count` = `commit_count` + 1, `updated_by_attempt` = 'w000-dryrun' WHERE `dataset_scope` = 'scope_0001';
  SET v_fence_ok = (SELECT count(*) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`c012_app_ice_fence` WHERE `dataset_scope` = 'scope_0001' AND `updated_by_attempt` = 'w000-dryrun');
  IF v_fence_ok <> 1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_FENCE_UPDATE_NOT_VISIBLE: scope=scope_0001 publication=scope_0001_p000001 seq=1'; END IF;
  SET v_staged = (SELECT count(*) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_d00_t01` WHERE `dataset_scope` = 'scope_0001' AND `publication_sequence` = 1);
  IF v_staged <> 1000 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_STAGING_COUNT_MISMATCH: scope=scope_0001 publication=scope_0001_p000001 seq=1 table=c012_app_ice_t01'; END IF;
  INSERT INTO `dbx_research`.`scd2_bench_r20261114t000000_example`.`c012_app_ice_t01` (`dataset_scope`, `business_key`, `attr_int`, `attr_decimal`, `attr_category`, `attr_flag`, `attr_text`, `payload_hash`, `effective_from_date`, `publication_id`, `publication_sequence`, `publication_ts`) SELECT `dataset_scope`, `business_key`, `attr_int`, `attr_decimal`, `attr_category`, `attr_flag`, `attr_text`, `payload_hash`, TIMESTAMP'2026-01-01 00:00:01.000000+00:00', 'scope_0001_p000001', 1, TIMESTAMP'2026-01-01 00:00:01.000000+00:00' FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_d00_t01` WHERE `dataset_scope` = 'scope_0001' AND `publication_sequence` = 1;
  SET v_staged = (SELECT count(*) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_d00_t02` WHERE `dataset_scope` = 'scope_0001' AND `publication_sequence` = 1);
  IF v_staged <> 1000 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_STAGING_COUNT_MISMATCH: scope=scope_0001 publication=scope_0001_p000001 seq=1 table=c012_app_ice_t02'; END IF;
  INSERT INTO `dbx_research`.`scd2_bench_r20261114t000000_example`.`c012_app_ice_t02` (`dataset_scope`, `business_key`, `attr_int`, `attr_decimal`, `attr_category`, `attr_flag`, `attr_text`, `payload_hash`, `effective_from_date`, `publication_id`, `publication_sequence`, `publication_ts`) SELECT `dataset_scope`, `business_key`, `attr_int`, `attr_decimal`, `attr_category`, `attr_flag`, `attr_text`, `payload_hash`, TIMESTAMP'2026-01-01 00:00:01.000000+00:00', 'scope_0001_p000001', 1, TIMESTAMP'2026-01-01 00:00:01.000000+00:00' FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_d00_t02` WHERE `dataset_scope` = 'scope_0001' AND `publication_sequence` = 1;
  INSERT INTO `dbx_research`.`scd2_bench_r20261114t000000_example`.`c012_app_ice_ledger` (`run_id`, `cell_key`, `dataset_scope`, `publication_id`, `publication_sequence`, `publication_ts`, `strategy`, `ordering_mode`, `writer_id`, `attempt_token`, `attempt_number`, `table_names`, `rows_per_table`, `ledger_write_ts`) VALUES ('r20261114t000000_example', 'c012_app_ice', 'scope_0001', 'scope_0001_p000001', 1, TIMESTAMP'2026-01-01 00:00:01.000000+00:00', 'append_only_full', 'sequenced', 'w000', 'w000-dryrun', 1, array('c012_app_ice_t01', 'c012_app_ice_t02'), 1000, current_timestamp());
END;

-- ===== publication transaction (performance:append_only_full:delta, cell c007_app_dlt, scope_0001_p000001) — submitted as ONE spark.sql() call
BEGIN ATOMIC
  DECLARE v_existing BIGINT DEFAULT 0;
  DECLARE v_last_seq BIGINT DEFAULT NULL;
  DECLARE v_fence_ok BIGINT DEFAULT 0;
  DECLARE v_staged BIGINT DEFAULT 0;
  SET v_existing = (SELECT count(*) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`c007_app_dlt_ledger` WHERE `dataset_scope` = 'scope_0001' AND `publication_id` = 'scope_0001_p000001');
  IF v_existing > 0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_DUPLICATE_PUBLICATION: scope=scope_0001 publication=scope_0001_p000001 seq=1'; END IF;
  SET v_last_seq = (SELECT max(`last_sequence`) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`c007_app_dlt_fence` WHERE `dataset_scope` = 'scope_0001');
  IF v_last_seq IS NULL THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_FENCE_MISSING: scope=scope_0001 publication=scope_0001_p000001 seq=1'; END IF;
  IF v_last_seq >= 1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_STALE_SEQUENCE: scope=scope_0001 publication=scope_0001_p000001 seq=1'; END IF;
  IF v_last_seq < 1 - 1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_SEQUENCE_NOT_READY: scope=scope_0001 publication=scope_0001_p000001 seq=1'; END IF;
  UPDATE `dbx_research`.`scd2_bench_r20261114t000000_example`.`c007_app_dlt_fence` SET `last_sequence` = 1, `last_publication_id` = 'scope_0001_p000001', `last_publication_ts` = TIMESTAMP'2026-01-01 00:00:01.000000+00:00', `commit_count` = `commit_count` + 1, `updated_by_attempt` = 'w000-dryrun' WHERE `dataset_scope` = 'scope_0001';
  SET v_fence_ok = (SELECT count(*) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`c007_app_dlt_fence` WHERE `dataset_scope` = 'scope_0001' AND `updated_by_attempt` = 'w000-dryrun');
  IF v_fence_ok <> 1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_FENCE_UPDATE_NOT_VISIBLE: scope=scope_0001 publication=scope_0001_p000001 seq=1'; END IF;
  SET v_staged = (SELECT count(*) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_d00_t01` WHERE `dataset_scope` = 'scope_0001' AND `publication_sequence` = 1);
  IF v_staged <> 1000 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_STAGING_COUNT_MISMATCH: scope=scope_0001 publication=scope_0001_p000001 seq=1 table=c007_app_dlt_t01'; END IF;
  INSERT INTO `dbx_research`.`scd2_bench_r20261114t000000_example`.`c007_app_dlt_t01` (`dataset_scope`, `business_key`, `attr_int`, `attr_decimal`, `attr_category`, `attr_flag`, `attr_text`, `payload_hash`, `effective_from_date`, `publication_id`, `publication_sequence`, `publication_ts`) SELECT `dataset_scope`, `business_key`, `attr_int`, `attr_decimal`, `attr_category`, `attr_flag`, `attr_text`, `payload_hash`, TIMESTAMP'2026-01-01 00:00:01.000000+00:00', 'scope_0001_p000001', 1, TIMESTAMP'2026-01-01 00:00:01.000000+00:00' FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_d00_t01` WHERE `dataset_scope` = 'scope_0001' AND `publication_sequence` = 1;
  SET v_staged = (SELECT count(*) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_d00_t02` WHERE `dataset_scope` = 'scope_0001' AND `publication_sequence` = 1);
  IF v_staged <> 1000 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_STAGING_COUNT_MISMATCH: scope=scope_0001 publication=scope_0001_p000001 seq=1 table=c007_app_dlt_t02'; END IF;
  INSERT INTO `dbx_research`.`scd2_bench_r20261114t000000_example`.`c007_app_dlt_t02` (`dataset_scope`, `business_key`, `attr_int`, `attr_decimal`, `attr_category`, `attr_flag`, `attr_text`, `payload_hash`, `effective_from_date`, `publication_id`, `publication_sequence`, `publication_ts`) SELECT `dataset_scope`, `business_key`, `attr_int`, `attr_decimal`, `attr_category`, `attr_flag`, `attr_text`, `payload_hash`, TIMESTAMP'2026-01-01 00:00:01.000000+00:00', 'scope_0001_p000001', 1, TIMESTAMP'2026-01-01 00:00:01.000000+00:00' FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_d00_t02` WHERE `dataset_scope` = 'scope_0001' AND `publication_sequence` = 1;
  INSERT INTO `dbx_research`.`scd2_bench_r20261114t000000_example`.`c007_app_dlt_ledger` (`run_id`, `cell_key`, `dataset_scope`, `publication_id`, `publication_sequence`, `publication_ts`, `strategy`, `ordering_mode`, `writer_id`, `attempt_token`, `attempt_number`, `table_names`, `rows_per_table`, `ledger_write_ts`) VALUES ('r20261114t000000_example', 'c007_app_dlt', 'scope_0001', 'scope_0001_p000001', 1, TIMESTAMP'2026-01-01 00:00:01.000000+00:00', 'append_only_full', 'sequenced', 'w000', 'w000-dryrun', 1, array('c007_app_dlt_t01', 'c007_app_dlt_t02'), 1000, current_timestamp());
END;

-- ===== publication transaction (fault:scd2_full:delta, cell c020_scd2_dlt, scope_0001_p000002) — submitted as ONE spark.sql() call
BEGIN ATOMIC
  DECLARE v_existing BIGINT DEFAULT 0;
  DECLARE v_last_seq BIGINT DEFAULT NULL;
  DECLARE v_fence_ok BIGINT DEFAULT 0;
  DECLARE v_staged BIGINT DEFAULT 0;
  SET v_existing = (SELECT count(*) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`c020_scd2_dlt_ledger` WHERE `dataset_scope` = 'scope_0001' AND `publication_id` = 'scope_0001_p000002');
  IF v_existing > 0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_DUPLICATE_PUBLICATION: scope=scope_0001 publication=scope_0001_p000002 seq=2'; END IF;
  SET v_last_seq = (SELECT max(`last_sequence`) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`c020_scd2_dlt_fence` WHERE `dataset_scope` = 'scope_0001');
  IF v_last_seq IS NULL THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_FENCE_MISSING: scope=scope_0001 publication=scope_0001_p000002 seq=2'; END IF;
  IF v_last_seq >= 2 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_STALE_SEQUENCE: scope=scope_0001 publication=scope_0001_p000002 seq=2'; END IF;
  IF v_last_seq < 2 - 1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_SEQUENCE_NOT_READY: scope=scope_0001 publication=scope_0001_p000002 seq=2'; END IF;
  UPDATE `dbx_research`.`scd2_bench_r20261114t000000_example`.`c020_scd2_dlt_fence` SET `last_sequence` = 2, `last_publication_id` = 'scope_0001_p000002', `last_publication_ts` = TIMESTAMP'2026-01-01 00:00:02.000000+00:00', `commit_count` = `commit_count` + 1, `updated_by_attempt` = 'w000-dryrun' WHERE `dataset_scope` = 'scope_0001';
  SET v_fence_ok = (SELECT count(*) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`c020_scd2_dlt_fence` WHERE `dataset_scope` = 'scope_0001' AND `updated_by_attempt` = 'w000-dryrun');
  IF v_fence_ok <> 1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_FENCE_UPDATE_NOT_VISIBLE: scope=scope_0001 publication=scope_0001_p000002 seq=2'; END IF;
  SET v_staged = (SELECT count(*) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_d00_t01` WHERE `dataset_scope` = 'scope_0001' AND `publication_sequence` = 2);
  IF v_staged <> 1000 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_STAGING_COUNT_MISMATCH: scope=scope_0001 publication=scope_0001_p000002 seq=2 table=c020_scd2_dlt_t01'; END IF;
  UPDATE `dbx_research`.`scd2_bench_r20261114t000000_example`.`c020_scd2_dlt_t01` SET `effective_to_date` = TIMESTAMP'2026-01-01 00:00:02.000000+00:00' WHERE `dataset_scope` = 'scope_0001' AND `effective_to_date` IS NULL;
  INSERT INTO `dbx_research`.`scd2_bench_r20261114t000000_example`.`c020_scd2_dlt_t01` (`dataset_scope`, `business_key`, `attr_int`, `attr_decimal`, `attr_category`, `attr_flag`, `attr_text`, `payload_hash`, `effective_from_date`, `effective_to_date`, `publication_id`, `publication_sequence`, `publication_ts`) SELECT `dataset_scope`, `business_key`, `attr_int`, `attr_decimal`, `attr_category`, `attr_flag`, `attr_text`, `payload_hash`, TIMESTAMP'2026-01-01 00:00:02.000000+00:00', CAST(NULL AS TIMESTAMP), 'scope_0001_p000002', 2, TIMESTAMP'2026-01-01 00:00:02.000000+00:00' FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_d00_t01` WHERE `dataset_scope` = 'scope_0001' AND `publication_sequence` = 2;
  SET v_staged = (SELECT count(*) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_d00_t02` WHERE `dataset_scope` = 'scope_0001' AND `publication_sequence` = 2);
  IF v_staged <> 1000 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_STAGING_COUNT_MISMATCH: scope=scope_0001 publication=scope_0001_p000002 seq=2 table=c020_scd2_dlt_t02'; END IF;
  UPDATE `dbx_research`.`scd2_bench_r20261114t000000_example`.`c020_scd2_dlt_t02` SET `effective_to_date` = TIMESTAMP'2026-01-01 00:00:02.000000+00:00' WHERE `dataset_scope` = 'scope_0001' AND `effective_to_date` IS NULL;
  INSERT INTO `dbx_research`.`scd2_bench_r20261114t000000_example`.`c020_scd2_dlt_t02` (`dataset_scope`, `business_key`, `attr_int`, `attr_decimal`, `attr_category`, `attr_flag`, `attr_text`, `payload_hash`, `effective_from_date`, `effective_to_date`, `publication_id`, `publication_sequence`, `publication_ts`) SELECT `dataset_scope`, `business_key`, `attr_int`, `attr_decimal`, `attr_category`, `attr_flag`, `attr_text`, `payload_hash`, TIMESTAMP'2026-01-01 00:00:02.000000+00:00', CAST(NULL AS TIMESTAMP), 'scope_0001_p000002', 2, TIMESTAMP'2026-01-01 00:00:02.000000+00:00' FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_d00_t02` WHERE `dataset_scope` = 'scope_0001' AND `publication_sequence` = 2;
  INSERT INTO `dbx_research`.`scd2_bench_r20261114t000000_example`.`c020_scd2_dlt_ledger` (`run_id`, `cell_key`, `dataset_scope`, `publication_id`, `publication_sequence`, `publication_ts`, `strategy`, `ordering_mode`, `writer_id`, `attempt_token`, `attempt_number`, `table_names`, `rows_per_table`, `ledger_write_ts`) VALUES ('r20261114t000000_example', 'c020_scd2_dlt', 'scope_0001', 'scope_0001_p000002', 2, TIMESTAMP'2026-01-01 00:00:02.000000+00:00', 'scd2_full', 'sequenced', 'w000', 'w000-dryrun', 1, array('c020_scd2_dlt_t01', 'c020_scd2_dlt_t02'), 1000, current_timestamp());
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_INJECTED_FAULT: after_ledger scope=scope_0001 publication=scope_0001_p000002 seq=2';
END;

-- ===== publication transaction (contention:scd2_full:delta, cell c016_scd2_dlt, scope_0001_p000002) — submitted as ONE spark.sql() call
BEGIN ATOMIC
  DECLARE v_existing BIGINT DEFAULT 0;
  DECLARE v_last_seq BIGINT DEFAULT NULL;
  DECLARE v_fence_ok BIGINT DEFAULT 0;
  DECLARE v_staged BIGINT DEFAULT 0;
  SET v_existing = (SELECT count(*) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`c016_scd2_dlt_ledger` WHERE `dataset_scope` = 'scope_0001' AND `publication_id` = 'scope_0001_p000002');
  IF v_existing > 0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_DUPLICATE_PUBLICATION: scope=scope_0001 publication=scope_0001_p000002 seq=2'; END IF;
  SET v_last_seq = (SELECT max(`last_sequence`) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`c016_scd2_dlt_fence` WHERE `dataset_scope` = 'scope_0001');
  IF v_last_seq IS NULL THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_FENCE_MISSING: scope=scope_0001 publication=scope_0001_p000002 seq=2'; END IF;
  IF v_last_seq >= 2 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_STALE_SEQUENCE: scope=scope_0001 publication=scope_0001_p000002 seq=2'; END IF;
  IF v_last_seq < 2 - 1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_SEQUENCE_NOT_READY: scope=scope_0001 publication=scope_0001_p000002 seq=2'; END IF;
  UPDATE `dbx_research`.`scd2_bench_r20261114t000000_example`.`c016_scd2_dlt_fence` SET `last_sequence` = 2, `last_publication_id` = 'scope_0001_p000002', `last_publication_ts` = TIMESTAMP'2026-01-01 00:00:02.000000+00:00', `commit_count` = `commit_count` + 1, `updated_by_attempt` = 'w000-dryrun' WHERE `dataset_scope` = 'scope_0001';
  SET v_fence_ok = (SELECT count(*) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`c016_scd2_dlt_fence` WHERE `dataset_scope` = 'scope_0001' AND `updated_by_attempt` = 'w000-dryrun');
  IF v_fence_ok <> 1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_FENCE_UPDATE_NOT_VISIBLE: scope=scope_0001 publication=scope_0001_p000002 seq=2'; END IF;
  SET v_staged = (SELECT count(*) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_dc_t01` WHERE `dataset_scope` = 'scope_0001' AND `publication_sequence` = 2);
  IF v_staged <> 1000 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_STAGING_COUNT_MISMATCH: scope=scope_0001 publication=scope_0001_p000002 seq=2 table=c016_scd2_dlt_t01'; END IF;
  UPDATE `dbx_research`.`scd2_bench_r20261114t000000_example`.`c016_scd2_dlt_t01` SET `effective_to_date` = TIMESTAMP'2026-01-01 00:00:02.000000+00:00' WHERE `dataset_scope` = 'scope_0001' AND `effective_to_date` IS NULL;
  INSERT INTO `dbx_research`.`scd2_bench_r20261114t000000_example`.`c016_scd2_dlt_t01` (`dataset_scope`, `business_key`, `attr_int`, `attr_decimal`, `attr_category`, `attr_flag`, `attr_text`, `payload_hash`, `effective_from_date`, `effective_to_date`, `publication_id`, `publication_sequence`, `publication_ts`) SELECT `dataset_scope`, `business_key`, `attr_int`, `attr_decimal`, `attr_category`, `attr_flag`, `attr_text`, `payload_hash`, TIMESTAMP'2026-01-01 00:00:02.000000+00:00', CAST(NULL AS TIMESTAMP), 'scope_0001_p000002', 2, TIMESTAMP'2026-01-01 00:00:02.000000+00:00' FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_dc_t01` WHERE `dataset_scope` = 'scope_0001' AND `publication_sequence` = 2;
  SET v_staged = (SELECT count(*) FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_dc_t02` WHERE `dataset_scope` = 'scope_0001' AND `publication_sequence` = 2);
  IF v_staged <> 1000 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'DBXBENCH_STAGING_COUNT_MISMATCH: scope=scope_0001 publication=scope_0001_p000002 seq=2 table=c016_scd2_dlt_t02'; END IF;
  UPDATE `dbx_research`.`scd2_bench_r20261114t000000_example`.`c016_scd2_dlt_t02` SET `effective_to_date` = TIMESTAMP'2026-01-01 00:00:02.000000+00:00' WHERE `dataset_scope` = 'scope_0001' AND `effective_to_date` IS NULL;
  INSERT INTO `dbx_research`.`scd2_bench_r20261114t000000_example`.`c016_scd2_dlt_t02` (`dataset_scope`, `business_key`, `attr_int`, `attr_decimal`, `attr_category`, `attr_flag`, `attr_text`, `payload_hash`, `effective_from_date`, `effective_to_date`, `publication_id`, `publication_sequence`, `publication_ts`) SELECT `dataset_scope`, `business_key`, `attr_int`, `attr_decimal`, `attr_category`, `attr_flag`, `attr_text`, `payload_hash`, TIMESTAMP'2026-01-01 00:00:02.000000+00:00', CAST(NULL AS TIMESTAMP), 'scope_0001_p000002', 2, TIMESTAMP'2026-01-01 00:00:02.000000+00:00' FROM `dbx_research`.`scd2_bench_r20261114t000000_example`.`stg_dc_t02` WHERE `dataset_scope` = 'scope_0001' AND `publication_sequence` = 2;
  INSERT INTO `dbx_research`.`scd2_bench_r20261114t000000_example`.`c016_scd2_dlt_ledger` (`run_id`, `cell_key`, `dataset_scope`, `publication_id`, `publication_sequence`, `publication_ts`, `strategy`, `ordering_mode`, `writer_id`, `attempt_token`, `attempt_number`, `table_names`, `rows_per_table`, `ledger_write_ts`) VALUES ('r20261114t000000_example', 'c016_scd2_dlt', 'scope_0001', 'scope_0001_p000002', 2, TIMESTAMP'2026-01-01 00:00:02.000000+00:00', 'scd2_full', 'sequenced', 'w000', 'w000-dryrun', 1, array('c016_scd2_dlt_t01', 'c016_scd2_dlt_t02'), 1000, current_timestamp());
END;
```
