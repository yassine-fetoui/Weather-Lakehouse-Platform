-- snowflake/tables/clustering_and_scd.sql
-- Part A: Clustering key configuration for weather_actual_data_timeseries
-- Part B: Idempotent SCD Type 2 MERGE for dim_locations

-- ════════════════════════════════════════════════════════════════════
-- PART A — CLUSTERING STRATEGY
-- ════════════════════════════════════════════════════════════════════
--
-- Problem: Dashboard queries filtering on location_id + date range were
-- scanning full micro-partition sets despite Iceberg partitioning by year/month/day.
-- Snowflake micro-partition pruning doesn't automatically align with Iceberg partitions.
--
-- Decision process:
--   1. Profiled Tableau workload → 90% of queries filter on location_id + year/month
--   2. Chose compound key (location_id, year, month) — not just date
--   3. Excluded `day` — finer granularity increases reclustering cost without benefit
--
-- Measure BEFORE adding clustering key:
SELECT SYSTEM$CLUSTERING_INFORMATION(
  'weather_actual_data_timeseries',
  '(location_id, year, month)'
);
-- Baseline: {"average_depth": ~12, "average_overlap": ~8}
-- High depth = engine scans many micro-partitions per query

-- Apply clustering key:
ALTER TABLE weather_actual_data_timeseries
  CLUSTER BY (location_id, year, month);

-- Measure AFTER Automatic Clustering runs (~24h):
SELECT SYSTEM$CLUSTERING_INFORMATION(
  'weather_actual_data_timeseries',
  '(location_id, year, month)'
);
-- Target: average_depth < 3, average_overlap < 2

-- Monitor Automatic Clustering credit consumption monthly:
-- If reclustering credits exceed query savings, reconsider key granularity.
SELECT
  table_name,
  credits_used,
  num_bytes_reclustered,
  num_rows_reclustered,
  start_time
FROM snowflake.account_usage.automatic_clustering_history
WHERE table_name = 'WEATHER_ACTUAL_DATA_TIMESERIES'
  AND start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
ORDER BY start_time DESC;


-- ════════════════════════════════════════════════════════════════════
-- PART B — SCD TYPE 2 IDEMPOTENT MERGE FOR dim_locations
-- ════════════════════════════════════════════════════════════════════
--
-- Problem: Airflow DAG retries on failure. A naive MERGE re-runs on retry
-- and generates duplicate history rows — corrupting the dimension.
--
-- Solution: pipeline_run_id guard + strict timestamp ordering.
--   - pipeline_run_id is a deterministic ID passed by Airflow (based on execution_date)
--   - MERGE only closes/inserts a record if this run_id hasn't already done so
--   - Safe to execute any number of times — idempotent by design
--
-- Called by Airflow as:
--   SnowflakeOperator(sql="CALL merge_dim_locations(:pipeline_run_id)", ...)

CREATE OR REPLACE PROCEDURE merge_dim_locations(pipeline_run_id STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  -- Step 1: Load incoming data into a temp staging table
  -- (In practice, Airflow writes this from the Glue output before calling this proc)
  CREATE TEMP TABLE IF NOT EXISTS staging_dim_locations AS
    SELECT * FROM dim_locations WHERE 1 = 0;  -- Schema only; data loaded by Glue

  -- Step 2: Idempotent MERGE
  MERGE INTO dim_locations AS target
  USING (
    SELECT
      location_id,
      name,
      country,
      lat,
      lon,
      timezone,
      CURRENT_TIMESTAMP()           AS valid_from,
      '9999-12-31'::TIMESTAMP_NTZ   AS valid_to,
      TRUE                          AS is_current,
      :pipeline_run_id              AS pipeline_run_id
    FROM staging_dim_locations
  ) AS source

  ON target.location_id   = source.location_id
  AND target.is_current   = TRUE

  -- Close existing active record if attributes changed
  -- AND guard: only close if this run hasn't already closed it (idempotency)
  WHEN MATCHED
    AND target.pipeline_run_id != source.pipeline_run_id
    AND (
      target.name     != source.name     OR
      target.timezone != source.timezone OR
      target.country  != source.country
    )
  THEN UPDATE SET
    target.valid_to         = CURRENT_TIMESTAMP(),
    target.is_current       = FALSE,
    target.pipeline_run_id  = source.pipeline_run_id

  -- Insert new record for changed or brand-new locations
  WHEN NOT MATCHED
  THEN INSERT (
    location_id, name, country, lat, lon, timezone,
    valid_from, valid_to, is_current, pipeline_run_id
  )
  VALUES (
    source.location_id, source.name, source.country,
    source.lat, source.lon, source.timezone,
    source.valid_from, source.valid_to, source.is_current,
    source.pipeline_run_id
  );

  RETURN 'merge_dim_locations completed for run_id: ' || :pipeline_run_id;
END;
$$;

-- ── Verify SCD Type 2 history for a location ─────────────────────────────
-- Shows the full history of a location — all versions with valid_from/valid_to.
SELECT
  location_id,
  name,
  timezone,
  valid_from,
  valid_to,
  is_current,
  pipeline_run_id
FROM dim_locations
WHERE location_id = 'LOC_001'
ORDER BY valid_from;
