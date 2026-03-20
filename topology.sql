-- snowflake/warehouses/topology.sql
-- Three-warehouse topology — one per workload class.
-- Prevents resource contention between pipeline operations and analytics queries.
--
-- Design rationale:
--   PIPELINE_WH  → Airflow tasks: bursty, latency-tolerant, small
--   ANALYTICS_WH → Tableau/analysts: concurrent, latency-sensitive, multi-cluster
--   TRANSFORM_WH → Heavy aggregations: isolated, never competes with BI
--
-- Resource monitors enforce hard credit ceilings per warehouse.
-- "Warehouses are workload contracts, not just compute."

USE ROLE SYSADMIN;
USE DATABASE WEATHER_LAKEHOUSE_DB;

-- ── PIPELINE_WH ───────────────────────────────────────────────────────────
CREATE OR REPLACE WAREHOUSE PIPELINE_WH
  WAREHOUSE_SIZE      = 'XSMALL'
  AUTO_SUSPEND        = 60          -- 60s idle → suspend immediately (bursty workload)
  AUTO_RESUME         = TRUE
  INITIALLY_SUSPENDED = TRUE
  MAX_CONCURRENCY_LEVEL = 4
  COMMENT = 'Pipeline warehouse: Airflow tasks, Iceberg REFRESH, dim_locations MERGE. Not for analysts.';

GRANT USAGE ON WAREHOUSE PIPELINE_WH TO ROLE PIPELINE_ROLE;

-- ── ANALYTICS_WH ─────────────────────────────────────────────────────────
CREATE OR REPLACE WAREHOUSE ANALYTICS_WH
  WAREHOUSE_SIZE      = 'SMALL'
  MIN_CLUSTER_COUNT   = 1
  MAX_CLUSTER_COUNT   = 3           -- Horizontal scale for concurrent dashboard refreshes
  SCALING_POLICY      = 'ECONOMY'   -- Fill current cluster before spinning new one
  AUTO_SUSPEND        = 120
  AUTO_RESUME         = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Analytics warehouse: Tableau, ad-hoc SQL. Multi-cluster for concurrency.';

GRANT USAGE ON WAREHOUSE ANALYTICS_WH TO ROLE ANALYST_ROLE;

-- ── TRANSFORM_WH ─────────────────────────────────────────────────────────
CREATE OR REPLACE WAREHOUSE TRANSFORM_WH
  WAREHOUSE_SIZE      = 'MEDIUM'
  AUTO_SUSPEND        = 60
  AUTO_RESUME         = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Transform warehouse: complex window functions, backfills. Isolated from BI.';

GRANT USAGE ON WAREHOUSE TRANSFORM_WH TO ROLE TRANSFORM_ROLE;

-- ── Resource Monitors ─────────────────────────────────────────────────────
-- Hard credit ceilings per warehouse per month.
-- Prevents cost surprises — pipeline credit spend is predictable and bounded.

CREATE OR REPLACE RESOURCE MONITOR pipeline_rm
  WITH CREDIT_QUOTA = 50
  FREQUENCY        = MONTHLY
  START_TIMESTAMP  = IMMEDIATELY
  TRIGGERS
    ON 75  PERCENT DO NOTIFY           -- Email alert at 75%
    ON 90  PERCENT DO NOTIFY           -- Email alert at 90%
    ON 100 PERCENT DO SUSPEND;         -- Hard suspend at 100%

CREATE OR REPLACE RESOURCE MONITOR analytics_rm
  WITH CREDIT_QUOTA = 200
  FREQUENCY        = MONTHLY
  START_TIMESTAMP  = IMMEDIATELY
  TRIGGERS
    ON 75  PERCENT DO NOTIFY
    ON 95  PERCENT DO NOTIFY
    ON 100 PERCENT DO SUSPEND_IMMEDIATE; -- Kill running queries at ceiling

CREATE OR REPLACE RESOURCE MONITOR transform_rm
  WITH CREDIT_QUOTA = 100
  FREQUENCY        = MONTHLY
  START_TIMESTAMP  = IMMEDIATELY
  TRIGGERS
    ON 80  PERCENT DO NOTIFY
    ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE PIPELINE_WH  SET RESOURCE_MONITOR = pipeline_rm;
ALTER WAREHOUSE ANALYTICS_WH SET RESOURCE_MONITOR = analytics_rm;
ALTER WAREHOUSE TRANSFORM_WH SET RESOURCE_MONITOR = transform_rm;

-- ── Verify topology ───────────────────────────────────────────────────────
SHOW WAREHOUSES;
SHOW RESOURCE MONITORS;
