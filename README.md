# Weather Lakehouse Platform
### AWS · Apache Iceberg · Snowflake · Airflow · Terraform

<div align="center">

![AWS](https://img.shields.io/badge/AWS-Cloud-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)
![Apache Iceberg](https://img.shields.io/badge/Apache_Iceberg-Table_Format-3C77B4?style=for-the-badge&logo=apache&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-Data_Warehouse-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache_Airflow-Orchestration-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-IaC-7B42BC?style=for-the-badge&logo=terraform&logoColor=white)

**Open Lakehouse architecture — Iceberg on S3 as the single source of truth, Snowflake as a pure compute layer, with zero data duplication.**

</div>

---

## Table of Contents

- [Architecture & Design Philosophy](#architecture--design-philosophy)
- [Why Iceberg + Snowflake (not COPY INTO)](#why-iceberg--snowflake-not-copy-into)
- [Data Model](#data-model)
- [Key Engineering Challenges](#key-engineering-challenges)
  - [Snowflake–AWS IAM Bootstrap (Chicken-and-Egg)](#challenge-1--snowflakeaws-iam-bootstrap-chicken-and-egg)
  - [Iceberg Metadata Refresh in Airflow DAG](#challenge-2--iceberg-metadata-refresh-in-airflow-dag)
  - [Warehouse Topology & Cost Governance](#challenge-3--warehouse-topology--cost-governance)
  - [Clustering Strategy on Time-Series Tables](#challenge-4--clustering-strategy-on-time-series-tables)
  - [SCD Type 2 with Idempotent MERGE on dim_locations](#challenge-5--scd-type-2-with-idempotent-merge-on-dim_locations)
- [Pipeline Orchestration (Airflow)](#pipeline-orchestration-airflow)
- [Infrastructure (Terraform)](#infrastructure-terraform)
- [Snowflake Setup](#snowflake-setup)
- [Getting Started](#getting-started)
- [Project Structure](#project-structure)

---

## Architecture & Design Philosophy

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        Weather Lakehouse Platform                             │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  INGESTION                 LAKEHOUSE LAYER                  COMPUTE           │
│                                                                               │
│  ┌──────────┐   Lambda    ┌──────────────────────┐         ┌──────────────┐  │
│  │WeatherAPI│────────────▶│  S3 Data Lake        │         │  Snowflake   │  │
│  │(REST API)│  async/     │                      │◀────────│  (external   │  │
│  └──────────┘  batched    │  raw/    → JSON       │  reads  │   Iceberg    │  │
│                           │  curated/ → Iceberg   │  only   │   tables)    │  │
│                           │  staging/ → Iceberg   │         └──────┬───────┘  │
│                           └──────────┬───────────┘                │           │
│                                      │ Glue PySpark               │           │
│                                      │ (ACID MERGE)               ▼           │
│  ORCHESTRATION             CATALOG   │                     BI / Analytics      │
│  ┌──────────┐   ┌──────────────────┐ │                     (Tableau, etc.)    │
│  │ Airflow  │──▶│ AWS Glue Catalog │─┘                                        │
│  │   DAG    │   │ (Iceberg tables) │                                           │
│  └──────────┘   └──────────────────┘                                           │
│                                                                               │
│  INFRASTRUCTURE: Terraform · GitHub Actions CI/CD · Docker                   │
└──────────────────────────────────────────────────────────────────────────────┘
```

**Core design principle:** S3 + Iceberg is the single source of truth. Snowflake is a compute layer that reads Iceberg metadata from the Glue Catalog — **no data duplication, no COPY INTO, no sync pipelines**.

| Metric | Value |
|---|---|
| Locations tracked | 300+ global |
| Data points / batch | up to 8,000 (days × locations) |
| Async concurrency | 30 req/s (Lambda) |
| Iceberg MERGE upsert | ACID, idempotent |
| Snowflake data copies | **0** — pure external Iceberg |
| Security audit findings | **0** |

---

## Why Iceberg + Snowflake (not COPY INTO)

The standard approach is to COPY data into Snowflake's internal storage. This project deliberately avoids it.

| Dimension | COPY INTO Snowflake | This project (External Iceberg) |
|---|---|---|
| Storage cost | Paid twice (S3 + Snowflake) | **S3 only** |
| Engine lock-in | Snowflake-only | Athena, Spark, Snowflake all read the same files |
| Schema evolution | Manual DDL migrations | **Iceberg handles it natively** |
| Data freshness | Requires a sync pipeline | Snowflake reads Glue catalog directly |
| ACID guarantees | Snowflake-side only | **Iceberg MERGE at write time** |

Snowflake's `CATALOG INTEGRATION` with AWS Glue means Snowflake can read Iceberg table metadata directly — no ETL job moves data between systems. If Snowflake is ever replaced, the data layer is untouched.

---

## Data Model

```
┌─────────────────────┐
│    dim_locations    │ ← SCD Type 2: tracks location metadata history
│─────────────────────│
│ location_id   (PK)  │
│ name                │
│ country             │
│ lat / lon           │
│ timezone            │
│ valid_from          │
│ valid_to            │
│ is_current          │
│ pipeline_run_id     │ ← idempotency guard
└──────────┬──────────┘
           │ 1:N
┌──────────▼──────────────────────┐   ┌──────────────────────────────────┐
│  weather_actual_data_timeseries │   │ weather_forecast_data_timeseries │
│─────────────────────────────────│   │──────────────────────────────────│
│ location_id                     │   │ location_id                      │
│ event_ts (hourly)               │   │ forecast_ts                      │
│ temp_c / temp_f                 │   │ ref_date (when forecast was made)│
│ wind_kph                        │   │ temp_c / temp_f                  │
│ humidity                        │   │ wind_kph / humidity              │
│ precip_mm                       │   │ precip_mm / chance_of_rain       │
│ uv_index                        │   │ uv_index                         │
│ year / month / day (partition)  │   │ ref_year / ref_month (partition) │
└─────────────────────────────────┘   └──────────────────────────────────┘
           │ aggregated into
┌──────────▼────────────────────────────┐
│  weather_actual_data_timeseries_agg   │ ← year/month/hour rollups
│───────────────────────────────────────│   for trend & seasonal analysis
│ location_id, year, month, hour        │
│ avg_temp_c, max_temp_c, min_temp_c    │
│ avg_humidity, avg_precip_mm           │
└───────────────────────────────────────┘
```

---

## Key Engineering Challenges

### Challenge 1 — Snowflake/AWS IAM Bootstrap (Chicken-and-Egg)

**Problem:** The Snowflake `CATALOG INTEGRATION` and `EXTERNAL VOLUME` need an IAM role ARN to be created first — but the IAM role's trust policy needs the `GLUE_AWS_EXTERNAL_ID` and `STORAGE_AWS_EXTERNAL_ID` that Snowflake only generates *after* those objects exist. A circular dependency Terraform cannot resolve in one pass.

**Solution:** Two-phase Terraform design with explicit phase boundary.

```hcl
# terraform/modules/iam/snowflake_role.tf

# ── PHASE 1: Bootstrap role with placeholder trust ─────────────────────────
# Apply this first. Snowflake can assume it (via account principal),
# but external IDs are not yet scoped — intentionally permissive for bootstrap.

data "aws_iam_policy_document" "snowflake_assume_role_phase1" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [var.snowflake_aws_account_arn]  # Snowflake's AWS account
    }
    # No external ID condition yet — added in Phase 2
  }
}

resource "aws_iam_role" "snowflake_service_role" {
  name               = "snowflake-service-role-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.snowflake_assume_role_phase1.json
}
```

```hcl
# ── PHASE 2: Tighten trust policy with Snowflake-generated external IDs ────
# Run AFTER: DESCRIBE CATALOG INTEGRATION and DESC EXTERNAL VOLUME in Snowflake
# to extract GLUE_AWS_EXTERNAL_ID and STORAGE_AWS_EXTERNAL_ID.

data "aws_iam_policy_document" "snowflake_assume_role_phase2" {
  # Glue catalog integration trust
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [var.snowflake_glue_iam_user_arn]  # From DESCRIBE CATALOG INTEGRATION
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.glue_aws_external_id]             # From DESCRIBE CATALOG INTEGRATION
    }
  }

  # S3 external volume trust
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [var.snowflake_storage_iam_user_arn] # From DESC EXTERNAL VOLUME
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.storage_aws_external_id]            # From DESC EXTERNAL VOLUME
    }
  }
}
```

> **Why this matters:** Anyone can create the IAM role. Scoping the trust policy to specific external IDs prevents any other Snowflake account from assuming your role — a real security exposure if skipped.

---

### Challenge 2 — Iceberg Metadata Refresh in Airflow DAG

**Problem:** Snowflake reads Iceberg metadata from the Glue Catalog. When Glue writes new Iceberg data, Snowflake's cached metadata does not update automatically — queries return stale or missing partitions until the next refresh cycle (up to 10 minutes by default).

**Solution:** Explicit `ALTER ICEBERG TABLE ... REFRESH` step injected into the Airflow DAG after every Glue job, using the Snowflake Airflow provider.

```python
# airflow/dags/weather_pipeline.py

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import json

SNOWFLAKE_CONN_ID = "snowflake_default"
GLUE_CONN_ID     = "aws_default"

ICEBERG_TABLES = [
    "dim_locations",
    "weather_actual_data_timeseries",
    "weather_actual_data_timeseries_agg",
    "weather_forecast_data_timeseries",
]

default_args = {
    "owner":            "data-engineering",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="weather_lakehouse_pipeline",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=True,           # Enables backfill — important for historical loads
    max_active_runs=1,      # Prevent concurrent DAG runs corrupting Iceberg state
    default_args=default_args,
    tags=["weather", "iceberg", "snowflake"],
) as dag:

    # ── Task 1: Update dim_locations ──────────────────────────────────────
    update_dim_locations = GlueJobOperator(
        task_id="update_dim_locations",
        job_name="weather-dim-locations-job",
        script_location="s3://weather-scripts/glue/dim_locations.py",
        aws_conn_id=GLUE_CONN_ID,
        wait_for_completion=True,
    )

    # ── Task 2: Generate batch metadata files in S3 ───────────────────────
    generate_batches = LambdaInvokeFunctionOperator(
        task_id="generate_location_date_batches",
        function_name="weather-batch-generator",
        payload=json.dumps({
            "execution_date": "{{ ds }}",
            "max_points_per_batch": 8000,
        }),
        aws_conn_id=GLUE_CONN_ID,
    )

    # ── Task 3: Invoke Lambda per batch (dynamic task mapping) ────────────
    # XCom pulls batch_ids from previous task
    fetch_weather_data = LambdaInvokeFunctionOperator.partial(
        task_id="fetch_weather_batch",
        function_name="weather-fetcher",
        aws_conn_id=GLUE_CONN_ID,
    ).expand(
        payload=generate_batches.output,  # Dynamic fan-out per batch
    )

    # ── Task 4: Glue transformation jobs ─────────────────────────────────
    with TaskGroup("glue_transformations") as glue_group:
        transform_actual = GlueJobOperator(
            task_id="transform_actual_data",
            job_name="weather-actual-transform",
            script_location="s3://weather-scripts/glue/transform_actual.py",
            script_args={"--execution_date": "{{ ds }}"},
            aws_conn_id=GLUE_CONN_ID,
            wait_for_completion=True,
        )
        transform_forecast = GlueJobOperator(
            task_id="transform_forecast_data",
            job_name="weather-forecast-transform",
            script_location="s3://weather-scripts/glue/transform_forecast.py",
            script_args={"--execution_date": "{{ ds }}"},
            aws_conn_id=GLUE_CONN_ID,
            wait_for_completion=True,
        )

    # ── Task 5: Refresh Snowflake Iceberg metadata ─────────────────────────
    # CRITICAL: Snowflake caches Glue catalog metadata.
    # Without this step, Snowflake queries return stale/missing partitions
    # for up to REFRESH_INTERVAL_SECONDS (set to 600s in CATALOG INTEGRATION).
    refresh_snowflake_tables = SnowflakeOperator(
        task_id="refresh_snowflake_iceberg_tables",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=[
            f"ALTER ICEBERG TABLE {table} REFRESH;"
            for table in ICEBERG_TABLES
        ],
    )

    # ── DAG dependency chain ───────────────────────────────────────────────
    update_dim_locations >> generate_batches >> fetch_weather_data
    fetch_weather_data   >> glue_group >> refresh_snowflake_tables
```

> **Why this matters:** Without the refresh step, a Snowflake dashboard query run right after a Glue job can miss entire day-partitions of data — silently, with no error. This is one of the most common production bugs in Glue → Snowflake Iceberg integrations.

---

### Challenge 3 — Warehouse Topology & Cost Governance

**Problem:** A single Snowflake warehouse serving Airflow-triggered Glue metadata queries, dim_locations MERGE operations, and Tableau dashboard queries creates resource contention. Heavy analytical queries compete with operational pipeline queries, causing both to slow down and triggering unnecessary auto-scaling credits.

**Solution:** Dedicated warehouse per workload class with resource monitors.

```sql
-- snowflake/warehouses/topology.sql

-- ── PIPELINE warehouse: operational, small, aggressive auto-suspend ────────
-- Used by: Airflow DAG tasks, Iceberg REFRESH statements, dim_locations MERGE
CREATE OR REPLACE WAREHOUSE PIPELINE_WH
  WAREHOUSE_SIZE   = 'XSMALL'
  AUTO_SUSPEND     = 60        -- Suspend after 60s idle (pipeline tasks are bursty)
  AUTO_RESUME      = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Operational warehouse for pipeline tasks. Not for ad-hoc queries.';

-- ── ANALYTICS warehouse: BI tools, multi-cluster for concurrency ──────────
-- Used by: Tableau, ad-hoc SQL, data analysts
CREATE OR REPLACE WAREHOUSE ANALYTICS_WH
  WAREHOUSE_SIZE   = 'SMALL'
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 3        -- Auto-scale horizontally for concurrent dashboard refreshes
  SCALING_POLICY   = 'ECONOMY' -- Prefer full clusters before adding new ones
  AUTO_SUSPEND     = 120
  AUTO_RESUME      = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Analytics warehouse for BI and ad-hoc queries. Multi-cluster enabled.';

-- ── TRANSFORM warehouse: heavy aggregation jobs, isolated ────────────────
-- Used by: Complex window function queries, backfill operations
CREATE OR REPLACE WAREHOUSE TRANSFORM_WH
  WAREHOUSE_SIZE   = 'MEDIUM'
  AUTO_SUSPEND     = 60
  AUTO_RESUME      = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Isolated warehouse for heavy transformations. Never shared with BI.';

-- ── Resource Monitors: credit quotas per warehouse ─────────────────────────
-- Prevents runaway queries from exceeding monthly budget.

CREATE OR REPLACE RESOURCE MONITOR pipeline_monitor
  WITH CREDIT_QUOTA = 50        -- 50 credits/month ceiling for pipeline work
  FREQUENCY        = MONTHLY
  START_TIMESTAMP  = IMMEDIATELY
  TRIGGERS
    ON 75 PERCENT DO NOTIFY     -- Alert at 75%
    ON 90 PERCENT DO NOTIFY
    ON 100 PERCENT DO SUSPEND;  -- Hard suspend at 100% — no overrun

ALTER WAREHOUSE PIPELINE_WH SET RESOURCE_MONITOR = pipeline_monitor;

CREATE OR REPLACE RESOURCE MONITOR analytics_monitor
  WITH CREDIT_QUOTA = 200
  FREQUENCY        = MONTHLY
  START_TIMESTAMP  = IMMEDIATELY
  TRIGGERS
    ON 75 PERCENT DO NOTIFY
    ON 100 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE ANALYTICS_WH SET RESOURCE_MONITOR = analytics_monitor;
```

> **Architecture principle:** Warehouses are workload contracts — each encodes a cost budget, a performance SLA, and an isolation boundary. Sharing one warehouse across pipeline and analytics workloads means you've made an implicit architecture decision that is almost always wrong.

---

### Challenge 4 — Clustering Strategy on Time-Series Tables

**Problem:** `weather_actual_data_timeseries` is a high-volume time-series table partitioned by `year/month/day` in Iceberg — but Snowflake's micro-partition pruning doesn't automatically align with Iceberg partitions. Dashboard queries filtering on `location_id + date range` were scanning full micro-partition sets.

**Solution:** Compound clustering key derived from query pattern analysis, not intuition.

```sql
-- snowflake/tables/clustering.sql

-- Step 1: Measure clustering depth BEFORE adding key
SELECT SYSTEM$CLUSTERING_INFORMATION(
  'weather_actual_data_timeseries',
  '(location_id, year, month)'
);
-- Returns: {"average_depth": 12.4, "average_overlap": 8.1}
-- → High depth = engine scans many micro-partitions per query

-- Step 2: Add compound clustering key
-- Chosen based on actual WHERE clause patterns from Tableau workload:
--   90% of queries filter on location_id + date range (year/month)
--   Clustering on date alone would miss the dominant access pattern.
--   We exclude `day` — finer granularity increases reclustering cost
--   without proportional benefit at current data volume.

ALTER TABLE weather_actual_data_timeseries
  CLUSTER BY (location_id, year, month);

-- Step 3: Validate after Automatic Clustering runs (~24h)
SELECT SYSTEM$CLUSTERING_INFORMATION(
  'weather_actual_data_timeseries',
  '(location_id, year, month)'
);
-- Target: average_depth < 3, average_overlap < 2

-- Step 4: Monitor Automatic Clustering credit consumption
-- Ensures reclustering cost is proportional to query savings.
SELECT
  table_name,
  credits_used,
  num_bytes_reclustered,
  num_rows_reclustered,
  start_time
FROM snowflake.account_usage.automatic_clustering_history
WHERE table_name = 'WEATHER_ACTUAL_DATA_TIMESERIES'
  AND start_time > DATEADD('day', -30, CURRENT_TIMESTAMP())
ORDER BY start_time DESC;
```

> **Senior insight:** Clustering the wrong column costs money without helping anyone. Always profile query patterns first. The decision of what NOT to cluster on is as important as what you do cluster on.

---

### Challenge 5 — SCD Type 2 with Idempotent MERGE on dim_locations

**Problem:** Location metadata (timezone, country classification) changes over time — we need historical tracking. But the Airflow DAG retries on failure. A naive SCD Type 2 MERGE re-runs on retry and generates ghost history rows, corrupting the dimension.

**Solution:** Idempotent MERGE with `pipeline_run_id` guard and strict timestamp ordering.

```sql
-- snowflake/tables/scd_type2_merge.sql

-- Called by Airflow with a deterministic run_id based on execution_date.
-- Safe to re-run any number of times — produces identical results.

MERGE INTO dim_locations AS target
USING (
    -- Source: incoming location data from S3/Iceberg staging
    SELECT
        location_id,
        name,
        country,
        lat,
        lon,
        timezone,
        CURRENT_TIMESTAMP()    AS valid_from,
        '9999-12-31'::DATE     AS valid_to,
        TRUE                   AS is_current,
        :pipeline_run_id       AS pipeline_run_id   -- Airflow passes this as a parameter
    FROM staging_dim_locations
) AS source

ON target.location_id = source.location_id
   AND target.is_current = TRUE

-- ── UPDATE: close existing record if attributes changed ───────────────────
WHEN MATCHED
  AND (
    target.name     != source.name     OR
    target.timezone != source.timezone OR
    target.country  != source.country
  )
  -- Idempotency guard: only close if this pipeline_run_id hasn't already done it
  AND target.pipeline_run_id != source.pipeline_run_id
THEN UPDATE SET
    target.valid_to         = CURRENT_TIMESTAMP(),
    target.is_current       = FALSE

-- ── INSERT: new record for changed location OR brand-new location ─────────
WHEN NOT MATCHED
  OR (
    MATCHED
    AND target.pipeline_run_id != source.pipeline_run_id
    AND (
      target.name     != source.name     OR
      target.timezone != source.timezone OR
      target.country  != source.country
    )
  )
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
```

> **Architecture principle:** ETL operations in a shared warehouse must be idempotent by design, not by hope. If a DAG can retry, the SQL must be safe to re-execute. The `pipeline_run_id` column is the contract that makes this deterministic.

---

## Pipeline Orchestration (Airflow)

```
DAG: weather_lakehouse_pipeline (daily)
│
├── update_dim_locations          [Glue] SCD Type 2 MERGE on dim_locations
│
└── generate_location_date_batches [Lambda] Queries Athena for latest date per location
    │                                       Generates batch JSON files in S3
    │                                       Max 8,000 points/batch (days × locations)
    │
    └── fetch_weather_batch.*      [Lambda] Dynamic fan-out — one task per batch
        │                                   30 async requests/s via aiohttp
        │
        └── glue_transformations/
            ├── transform_actual_data    [Glue] Flatten JSON → Iceberg MERGE (ACID)
            └── transform_forecast_data [Glue] Flatten JSON → Iceberg MERGE (ACID)
                │
                └── refresh_snowflake_iceberg_tables  [Snowflake]
                                                       ALTER ICEBERG TABLE ... REFRESH
                                                       for all 4 tables
```

**Key decisions:**
- `max_active_runs=1` — prevents concurrent DAG runs from creating Iceberg write conflicts
- `catchup=True` — enables historical backfill with full execution_date tracking
- `retry_delay=5m` — gives Glue time to release Iceberg locks before retry

---

## Infrastructure (Terraform)

```
terraform/
├── environments/prod/
│   ├── backend.tf            # S3 remote state + DynamoDB lock
│   ├── main.tf               # Module composition
│   └── variables.tf
└── modules/
    ├── s3/                   # Data lake buckets + lifecycle policies
    ├── iam/
    │   ├── glue_role.tf      # Least-privilege, VPC endpoint conditioned
    │   └── snowflake_role.tf # Two-phase bootstrap (see Challenge 1)
    ├── glue/                 # Glue jobs, triggers, Data Catalog
    └── snowflake/            # Outputs used in Snowflake setup scripts
```

**Remote state:**
```hcl
backend "s3" {
  bucket         = "your-terraform-state"
  key            = "prod/weather-lakehouse/terraform.tfstate"
  region         = "us-east-1"
  encrypt        = true
  kms_key_id     = "alias/terraform-state-key"
  dynamodb_table = "terraform-state-lock"
}
```

---

## Snowflake Setup

Run in order after `terraform apply`:

```
snowflake/
├── setup/
│   ├── 01_database_warehouse.sql      # Database + warehouse creation
│   ├── 02_catalog_integration.sql     # Glue catalog integration
│   ├── 03_external_volume.sql         # S3 external volume
│   └── 04_iam_values.sql              # DESCRIBE commands to extract IAM values → feed back to Terraform Phase 2
├── warehouses/
│   └── topology.sql                   # 3-warehouse topology + resource monitors
├── tables/
│   ├── iceberg_tables.sql             # CREATE ICEBERG TABLE statements
│   ├── clustering.sql                 # Clustering keys + validation queries
│   └── scd_type2_merge.sql            # Idempotent SCD Type 2 MERGE
└── monitoring/
    └── observability.sql              # Credit usage, clustering history, query profiling
```

---

## Getting Started

### Prerequisites

- AWS account with admin access
- Snowflake account (AWS region matching your S3 bucket)
- Terraform >= 1.5
- Python 3.11+
- Docker (for Airflow)

### 1. Bootstrap infrastructure (Phase 1)

```bash
git clone https://github.com/YOUR_USERNAME/weather-lakehouse-platform.git
cd weather-lakehouse-platform

cp terraform/environments/prod/terraform.tfvars.example \
   terraform/environments/prod/terraform.tfvars
# Fill in: account_id, region, snowflake_aws_account_arn

cd terraform/environments/prod
terraform init
terraform apply -target=module.s3 -target=module.iam -target=module.glue
```

### 2. Create Snowflake objects & extract IAM values

```bash
# Run in Snowflake worksheet (in order):
snowflake/setup/01_database_warehouse.sql
snowflake/setup/02_catalog_integration.sql
snowflake/setup/03_external_volume.sql
snowflake/setup/04_iam_values.sql     # Copy the output values

# Add extracted values to terraform.tfvars:
# glue_aws_external_id        = "..."
# storage_aws_external_id     = "..."
# snowflake_glue_iam_user_arn = "..."
```

### 3. Tighten IAM trust (Phase 2)

```bash
terraform apply   # Now applies the scoped trust policy with external IDs
```

### 4. Configure warehouse topology & tables

```bash
snowflake/warehouses/topology.sql
snowflake/tables/iceberg_tables.sql
snowflake/tables/clustering.sql
```

### 5. Start Airflow

```bash
cd airflow
echo "AIRFLOW_UID=$(id -u)" > .env
docker-compose up airflow-init
docker-compose up --build -d
# UI: http://localhost:8080 (airflow / airflow)
```

---

## Project Structure

```
weather-lakehouse-platform/
├── .github/workflows/
│   └── cicd.yml                     # Terraform plan + security scan on PR
├── airflow/
│   ├── dags/weather_pipeline.py     # Full Airflow DAG
│   └── docker-compose.yml
├── glue/
│   ├── jobs/
│   │   ├── transform_actual.py      # PySpark: flatten + Iceberg MERGE (actual data)
│   │   └── transform_forecast.py    # PySpark: flatten + Iceberg MERGE (forecast)
│   └── schemas/                     # JSON schemas driving Iceberg table creation
├── lambda/
│   ├── ingestion/handler.py         # Async WeatherAPI fetch → S3 raw
│   └── catalog_refresh/handler.py   # EventBridge-triggered Athena refresh
├── snowflake/
│   ├── setup/                       # Sequential SQL setup scripts
│   ├── warehouses/topology.sql      # 3-warehouse topology + resource monitors
│   ├── tables/                      # Iceberg DDL + clustering + SCD MERGE
│   └── monitoring/observability.sql # Credit + clustering + query observability
├── terraform/
│   ├── environments/prod/
│   └── modules/{s3,iam,glue,snowflake}/
└── docs/
    └── architecture.md
```

---

<div align="center">

Built by [Yassine](https://github.com/YOUR_USERNAME) ·  Data Engineer



</div>
