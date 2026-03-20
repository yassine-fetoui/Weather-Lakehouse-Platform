-- snowflake/tables/iceberg_tables.sql
-- Creates external Iceberg tables backed by S3 + Glue Catalog.
-- Snowflake reads table metadata from Glue and data files from S3.
-- NO data is copied into Snowflake storage.
--
-- Run AFTER:
--   ✅ 02_catalog_integration.sql
--   ✅ 03_external_volume.sql
--   ✅ Terraform Phase 2 (scoped IAM trust policy applied)
--   ✅ SYSTEM$VALIDATE_STORAGE_INTEGRATION returns success

USE ROLE SYSADMIN;
USE WAREHOUSE PIPELINE_WH;
USE DATABASE WEATHER_LAKEHOUSE_DB;
USE SCHEMA PUBLIC;

-- ── dim_locations ─────────────────────────────────────────────────────────
-- SCD Type 2 dimension. Tracks location metadata history.
-- Partitioned by is_current for efficient current-record lookups.
CREATE OR REPLACE ICEBERG TABLE dim_locations
  EXTERNAL_VOLUME = 'weather_lakehouse_vol'
  CATALOG         = 'glue_catalog_weather_lakehouse'
  CATALOG_TABLE_NAME = 'dim_locations'
  AUTO_REFRESH    = FALSE;  -- Refresh is controlled by Airflow (ALTER ... REFRESH)
                             -- AUTO_REFRESH = TRUE would fire on Glue catalog changes,
                             -- but is less predictable than explicit DAG-driven refresh.

-- ── weather_actual_data_timeseries ────────────────────────────────────────
-- Hourly granularity. High volume. Clustered for dashboard query patterns.
-- Partitioned in Iceberg by year/month/day.
CREATE OR REPLACE ICEBERG TABLE weather_actual_data_timeseries
  EXTERNAL_VOLUME = 'weather_lakehouse_vol'
  CATALOG         = 'glue_catalog_weather_lakehouse'
  CATALOG_TABLE_NAME = 'weather_actual_data_timeseries'
  AUTO_REFRESH    = FALSE;

-- ── weather_actual_data_timeseries_agg ───────────────────────────────────
-- Pre-aggregated by year/month/hour. Serves Tableau trend dashboards.
-- Much smaller than timeseries — no clustering needed at this granularity.
CREATE OR REPLACE ICEBERG TABLE weather_actual_data_timeseries_agg
  EXTERNAL_VOLUME = 'weather_lakehouse_vol'
  CATALOG         = 'glue_catalog_weather_lakehouse'
  CATALOG_TABLE_NAME = 'weather_actual_data_timeseries_agg'
  AUTO_REFRESH    = FALSE;

-- ── weather_forecast_data_timeseries ─────────────────────────────────────
-- Forecasted weather. Partitioned by ref_year/ref_month (when forecast was made).
-- Enables point-in-time accuracy analysis: how good were forecasts made N days ago?
CREATE OR REPLACE ICEBERG TABLE weather_forecast_data_timeseries
  EXTERNAL_VOLUME = 'weather_lakehouse_vol'
  CATALOG         = 'glue_catalog_weather_lakehouse'
  CATALOG_TABLE_NAME = 'weather_forecast_data_timeseries'
  AUTO_REFRESH    = FALSE;

-- ── Grant read access to analyst role ────────────────────────────────────
GRANT SELECT ON ALL TABLES IN SCHEMA PUBLIC TO ROLE ANALYST_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA PUBLIC TO ROLE ANALYST_ROLE;

-- ── Verify tables are visible ─────────────────────────────────────────────
SHOW ICEBERG TABLES;
SELECT * FROM dim_locations LIMIT 5;
