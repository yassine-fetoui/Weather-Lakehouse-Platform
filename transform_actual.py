"""
AWS Glue PySpark Job — Raw JSON → Apache Iceberg (ACID MERGE)
Transforms weather actual data and merges into the Iceberg table.

Design decisions:
- MERGE (not INSERT OVERWRITE) — enables incremental upserts without full partition rewrites
- Staging table pattern — write to temp Iceberg table, then MERGE into main
  This makes the operation restartable without double-inserts
- Partition pruning — reads only the relevant date partition from raw zone
"""

import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "SOURCE_BUCKET",
    "DATABASE_NAME",
    "EXECUTION_DATE",    # YYYY-MM-DD — passed by Airflow
])

sc         = SparkContext()
glueContext = GlueContext(sc)
spark      = glueContext.spark_session
job        = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_BUCKET  = args["SOURCE_BUCKET"]
DATABASE_NAME  = args["DATABASE_NAME"]
EXECUTION_DATE = args["EXECUTION_DATE"]

year, month, day = EXECUTION_DATE.split("-")

# ── Configure Spark for Iceberg ────────────────────────────────────────────
spark.conf.set("spark.sql.extensions",
               "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark.conf.set("spark.sql.catalog.glue_catalog",
               "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse",
               f"s3://{SOURCE_BUCKET}/warehouse/")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl",
               "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl",
               "org.apache.iceberg.aws.s3.S3FileIO")


# ── Extract ────────────────────────────────────────────────────────────────

def read_raw_partition(year: str, month: str, day: str) -> DataFrame:
    """Read only the raw JSON for the execution date — avoids full S3 scan."""
    path = f"s3://{SOURCE_BUCKET}/raw/year={year}/month={month}/day={day}/"
    print(f"[EXTRACT] Reading from: {path}")
    return spark.read.json(path)


# ── Transform ──────────────────────────────────────────────────────────────

def flatten_actual_data(df: DataFrame) -> DataFrame:
    """
    Flatten nested WeatherAPI JSON response.
    Each top-level record has a `location` object and `forecast.forecastday` array.
    We explode hourly data into one row per location per hour.
    """
    return (
        df.select(
            F.col("location.name").alias("location_name"),
            F.col("location_id").cast(StringType()),
            F.explode("forecast.forecastday").alias("forecastday"),
        )
        .select(
            "location_id",
            "location_name",
            F.col("forecastday.date").alias("date"),
            F.explode("forecastday.hour").alias("hour_data"),
        )
        .select(
            "location_id",
            "location_name",
            F.to_timestamp("date").alias("event_date"),
            F.to_timestamp("hour_data.time").alias("event_ts"),
            F.col("hour_data.temp_c").cast(DoubleType()).alias("temp_c"),
            F.col("hour_data.temp_f").cast(DoubleType()).alias("temp_f"),
            F.col("hour_data.wind_kph").cast(DoubleType()).alias("wind_kph"),
            F.col("hour_data.humidity").cast(IntegerType()).alias("humidity"),
            F.col("hour_data.precip_mm").cast(DoubleType()).alias("precip_mm"),
            F.col("hour_data.uv").cast(DoubleType()).alias("uv_index"),
            F.col("hour_data.is_day").cast(IntegerType()).alias("is_day"),
        )
        .withColumn("year",  F.lit(year))
        .withColumn("month", F.lit(month))
        .withColumn("day",   F.lit(day))
        .withColumn("processed_at", F.current_timestamp())
        .dropDuplicates(["location_id", "event_ts"])  # Idempotency — safe on retry
    )


def build_hourly_agg(df: DataFrame) -> DataFrame:
    """Aggregate hourly data → year/month/hour rollup for trend analysis."""
    return (
        df.groupBy("location_id", "year", "month",
                   F.hour("event_ts").alias("hour"))
          .agg(
              F.avg("temp_c").alias("avg_temp_c"),
              F.max("temp_c").alias("max_temp_c"),
              F.min("temp_c").alias("min_temp_c"),
              F.avg("humidity").alias("avg_humidity"),
              F.avg("precip_mm").alias("avg_precip_mm"),
              F.avg("wind_kph").alias("avg_wind_kph"),
              F.avg("uv_index").alias("avg_uv_index"),
              F.count("*").alias("record_count"),
          )
          .withColumn("processed_at", F.current_timestamp())
    )


# ── Load: Iceberg MERGE (staging → main) ──────────────────────────────────

def upsert_to_iceberg(df: DataFrame, table: str, merge_keys: list[str]) -> None:
    """
    Write via staging table + MERGE pattern.
    Staging write is idempotent (overwrite). MERGE is the atomic operation.
    This means a job restart after the MERGE fails won't double-insert.
    """
    staging_table = f"glue_catalog.{DATABASE_NAME}.{table}_staging"
    main_table    = f"glue_catalog.{DATABASE_NAME}.{table}"

    # Write staging (overwrite — safe to replay)
    df.writeTo(staging_table) \
      .tableProperty("write.format.default", "parquet") \
      .tableProperty("write.parquet.compression-codec", "snappy") \
      .overwritePartitions()

    print(f"[STAGING] Written {df.count()} rows to {staging_table}")

    # Build MERGE condition from composite key
    on_clause     = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
    update_cols   = [c for c in df.columns if c not in merge_keys]
    update_clause = ", ".join([f"target.{c} = source.{c}" for c in update_cols])
    insert_cols   = ", ".join(df.columns)
    insert_vals   = ", ".join([f"source.{c}" for c in df.columns])

    merge_sql = f"""
        MERGE INTO {main_table} AS target
        USING {staging_table} AS source
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """

    spark.sql(merge_sql)
    print(f"[MERGE] ACID MERGE completed into {main_table}")


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    print(f"[START] transform_actual | execution_date={EXECUTION_DATE}")

    raw_df      = read_raw_partition(year, month, day)
    actual_df   = flatten_actual_data(raw_df)
    agg_df      = build_hourly_agg(actual_df)

    print(f"[TRANSFORM] Rows (actual): {actual_df.count()} | Rows (agg): {agg_df.count()}")

    upsert_to_iceberg(
        actual_df,
        table="weather_actual_data_timeseries",
        merge_keys=["location_id", "event_ts"],
    )
    upsert_to_iceberg(
        agg_df,
        table="weather_actual_data_timeseries_agg",
        merge_keys=["location_id", "year", "month", "hour"],
    )

    print("[DONE] transform_actual completed successfully.")


main()
job.commit()
