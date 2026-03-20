-- snowflake/setup/03_external_volume.sql
-- Defines the S3 external volume Snowflake uses to read Iceberg data files.
-- ALLOW_WRITES = FALSE enforces the design contract:
--   Snowflake is a READ-ONLY compute layer. Glue owns all writes.

-- IMPORTANT: Replace placeholder values before running.
-- STORAGE_BASE_URL     → s3://your-bucket/warehouse/weather_lakehouse.db/
-- STORAGE_AWS_ROLE_ARN → same snowflake-service-role ARN as catalog integration

CREATE OR REPLACE EXTERNAL VOLUME weather_lakehouse_vol
  STORAGE_LOCATIONS = (
    (
      NAME                = 's3_weather_lakehouse'
      STORAGE_PROVIDER    = 'S3'
      STORAGE_BASE_URL    = 's3://weather-datalake-prod/warehouse/weather_lakehouse.db/'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::ACCOUNT_ID:role/snowflake-service-role-prod'
    )
  )
  ALLOW_WRITES = FALSE;  -- Snowflake is read-only. Glue owns the write path.

-- ── Extract IAM values → required for Terraform Phase 2 ──────────────────
-- From the output JSON, copy:
--   STORAGE_AWS_IAM_USER_ARN → snowflake_storage_iam_user_arn in terraform.tfvars
--   STORAGE_AWS_EXTERNAL_ID  → storage_aws_external_id in terraform.tfvars
DESC EXTERNAL VOLUME weather_lakehouse_vol;

-- ── Validate S3 connectivity (run after Terraform Phase 2 apply) ──────────
-- Should return "success" — if it fails, the IAM trust policy is misconfigured.
SELECT SYSTEM$VALIDATE_STORAGE_INTEGRATION('weather_lakehouse_vol');
