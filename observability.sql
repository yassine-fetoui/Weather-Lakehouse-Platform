-- snowflake/monitoring/observability.sql
-- Operational queries for monitoring cost, performance, and data freshness.
-- Run in ANALYTICS_WH or PIPELINE_WH depending on query weight.

USE DATABASE WEATHER_LAKEHOUSE_DB;
USE WAREHOUSE ANALYTICS_WH;

-- ── 1. Credit consumption per warehouse (current month) ───────────────────
SELECT
  warehouse_name,
  SUM(credits_used)                                    AS total_credits,
  SUM(credits_used_cloud_services)                     AS cloud_services_credits,
  DATE_TRUNC('day', start_time)                        AS day,
  COUNT(*)                                             AS query_count
FROM snowflake.account_usage.warehouse_metering_history
WHERE start_time >= DATE_TRUNC('month', CURRENT_TIMESTAMP())
GROUP BY 1, 4
ORDER BY 1, 4;

-- ── 2. Most expensive queries (last 7 days) ───────────────────────────────
SELECT
  query_id,
  query_text,
  warehouse_name,
  execution_time / 1000                                AS execution_seconds,
  credits_used_cloud_services,
  bytes_scanned / 1e9                                  AS gb_scanned,
  percentage_scanned_from_cache                        AS cache_hit_pct,
  start_time
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND execution_status = 'SUCCESS'
  AND warehouse_name IS NOT NULL
ORDER BY execution_time DESC
LIMIT 20;

-- ── 3. Micro-partition pruning efficiency per table ───────────────────────
-- Low pruned_ratio indicates clustering key is working well.
-- High partitions_scanned/total ratio → consider adding or adjusting clustering key.
SELECT
  query_id,
  query_text,
  partitions_total,
  partitions_scanned,
  ROUND(partitions_scanned / NULLIF(partitions_total, 0) * 100, 1) AS pct_scanned,
  start_time
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND query_text ILIKE '%weather_actual_data_timeseries%'
  AND partitions_total > 0
ORDER BY pct_scanned DESC
LIMIT 20;

-- ── 4. Automatic Clustering credit spend vs. benefit ─────────────────────
-- If credits_used >> bytes_reclustered / query_savings, reconsider key choice.
SELECT
  table_name,
  SUM(credits_used)                                    AS total_credits,
  SUM(num_bytes_reclustered) / 1e9                     AS gb_reclustered,
  COUNT(*)                                             AS recluster_jobs,
  DATE_TRUNC('week', start_time)                       AS week
FROM snowflake.account_usage.automatic_clustering_history
WHERE start_time >= DATEADD('month', -1, CURRENT_TIMESTAMP())
GROUP BY 1, 5
ORDER BY 1, 5;

-- ── 5. Iceberg table freshness (last refresh per table) ───────────────────
-- Confirms the Airflow DAG's ALTER ICEBERG TABLE ... REFRESH step is firing.
-- Tables showing last_refresh > 25h indicate the DAG may have failed silently.
SELECT
  table_catalog,
  table_schema,
  table_name,
  last_altered                                         AS last_refresh,
  DATEDIFF('hour', last_altered, CURRENT_TIMESTAMP()) AS hours_since_refresh
FROM information_schema.tables
WHERE table_schema = 'PUBLIC'
  AND table_type = 'EXTERNAL'
ORDER BY hours_since_refresh DESC;

-- ── 6. Resource monitor usage vs quota ───────────────────────────────────
-- Alerts before credit ceilings are hit.
SELECT
  name,
  credit_quota,
  credits_used,
  ROUND(credits_used / credit_quota * 100, 1)          AS pct_used,
  frequency,
  suspend_at
FROM snowflake.account_usage.resource_monitors
ORDER BY pct_used DESC;

-- ── 7. Concurrent query load per warehouse ────────────────────────────────
-- Helps validate whether ANALYTICS_WH multi-cluster scaling is triggering correctly.
SELECT
  warehouse_name,
  DATE_TRUNC('hour', start_time)                       AS hour,
  COUNT(*)                                             AS queries,
  AVG(queued_overload_time / 1000)                     AS avg_queue_seconds,
  MAX(queued_overload_time / 1000)                     AS max_queue_seconds
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND warehouse_name = 'ANALYTICS_WH'
GROUP BY 1, 2
ORDER BY 1, 2;
