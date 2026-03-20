-- snowflake/setup/02_catalog_integration.sql
-- Creates the Glue Catalog integration.
-- After running this, execute 04_iam_values.sql to extract the IAM values
-- needed for the Terraform Phase 2 trust policy.

-- IMPORTANT: Replace placeholder values before running.
-- GLUE_AWS_ROLE_ARN    → from terraform output: snowflake_service_role_arn
-- GLUE_CATALOG_ID      → your 12-digit AWS account ID
-- GLUE_REGION          → must match your S3 bucket region

CREATE OR REPLACE CATALOG INTEGRATION glue_catalog_weather_lakehouse
  CATALOG_SOURCE              = GLUE
  CATALOG_NAMESPACE           = 'weather_lakehouse'   -- Glue database name
  TABLE_FORMAT                = ICEBERG
  GLUE_AWS_ROLE_ARN           = 'arn:aws:iam::ACCOUNT_ID:role/snowflake-service-role-prod'
  GLUE_CATALOG_ID             = 'ACCOUNT_ID'
  GLUE_REGION                 = 'us-east-1'
  ENABLED                     = TRUE
  REFRESH_INTERVAL_SECONDS    = 600;  -- 10-min cache. Airflow forces refresh after each Glue job.

-- ── Validate integration was created ─────────────────────────────────────
SHOW INTEGRATIONS LIKE 'glue_catalog_weather_lakehouse';

-- ── Extract IAM values → required for Terraform Phase 2 ──────────────────
-- Copy GLUE_AWS_IAM_USER_ARN and GLUE_AWS_EXTERNAL_ID from the output.
-- These go into terraform.tfvars as:
--   snowflake_glue_iam_user_arn = "..."
--   glue_aws_external_id        = "..."
DESCRIBE CATALOG INTEGRATION glue_catalog_weather_lakehouse;
