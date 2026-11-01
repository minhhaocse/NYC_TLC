"""PySpark Data Cleaning & Harmonization Module (Silver Layer).

This module is responsible for:
1. Initializing an optimized local SparkSession tuned for local hardware.
2. Dynamic schema harmonization converting heterogeneous fleet columns into a unified Silver schema.
3. Feature engineering: computing trip duration, dual-unit distances (miles/km), speeds (mph/kmh), and wait times.
4. Enforcing rigorous Quality Gates: boundary verification, anomaly filtering, and unknown zone fallback.
5. Partitioned Lakehouse storage partitioned by year, month, and fleet.
6. Generating comprehensive data quality metrics and lineage audit logs.
"""

import json
import logging
import os
from pathlib import Path
import sys
from typing import Dict, List, Optional
import urllib.request

def ensure_windows_hadoop_env() -> None:
    """Configures Python path and ensures winutils.exe exists for Spark Parquet write on Windows."""
    os.environ["PYSPARK_PYTHON"] = "python"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

    if os.name == "nt":
        project_root = Path(__file__).resolve().parent.parent
        hadoop_home = project_root / "tools" / "hadoop"
        bin_dir = hadoop_home / "bin"
        bin_dir.mkdir(parents=True, exist_ok=True)

        winutils_exe = bin_dir / "winutils.exe"
        hadoop_dll = bin_dir / "hadoop.dll"
        cdn_base = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin"
        headers = {"User-Agent": "Mozilla/5.0"}
        if not winutils_exe.exists():
            print("[INFO] Setting up Windows Hadoop tools: Downloading winutils.exe...")
            req = urllib.request.Request(f"{cdn_base}/winutils.exe", headers=headers)
            with urllib.request.urlopen(req, timeout=30) as resp, open(winutils_exe, "wb") as f:
                f.write(resp.read())
        if not hadoop_dll.exists():
            print("[INFO] Setting up Windows Hadoop tools: Downloading hadoop.dll...")
            req = urllib.request.Request(f"{cdn_base}/hadoop.dll", headers=headers)
            with urllib.request.urlopen(req, timeout=30) as resp, open(hadoop_dll, "wb") as f:
                f.write(resp.read())

        os.environ["HADOOP_HOME"] = str(hadoop_home)
        os.environ["PATH"] = str(bin_dir) + os.pathsep + os.environ.get("PATH", "")

ensure_windows_hadoop_env()

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType

from src.config import (
    BRONZE_DIR,
    FLEET_CONFIG,
    LOGS_DIR,
    MAX_DURATION_HOURS,
    MAX_OUTSIDE_MONTH_FRACTION,
    MAX_SPEED_MPH,
    MILES_TO_KM,
    MIN_DISTANCE_MILES,
    MIN_DURATION_MINUTES,
    SILVER_DIR,
    SPARK_APP_NAME,
    SPARK_DRIVER_MEMORY,
    SPARK_SQL_SHUFFLE_PARTITIONS,
    UNKNOWN_ZONE_ID,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOGS_DIR / "spark_cleaning.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("SparkCleaner")

def get_spark_session() -> SparkSession:
    """Builds an optimized SparkSession based on the setting in src.config."""
    return (
        SparkSession.builder.appName(SPARK_APP_NAME)
        .master("local[*]")
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", str(SPARK_SQL_SHUFFLE_PARTITIONS))
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

def harmonize_fleet_schema(raw_df: DataFrame, fleet: str) -> DataFrame:
    """Maps fleet-specific raw attributes into the unified Silver relational schema."""
    cfg = FLEET_CONFIG[fleet]
    cols = raw_df.columns
    def get_col(col_name: Optional[str], default_expr=F.lit(None)):
        """Safely extracts a column if present in source, otherwise returns default null expression."""
        if col_name and col_name in cols:
            return F.col(col_name)
        return default_expr
    # Unified Master Projection Expression
    harmonized = raw_df.select(
        F.lit(fleet).alias("fleet_type"),
        get_col(cfg["vendor_id_col"]).cast(StringType()).alias("vendor_id"),
        get_col(cfg["license_num_col"]).cast(StringType()).alias("license_num"),
        get_col(cfg["dispatch_base_col"]).cast(StringType()).alias("dispatching_base_num"),
        get_col(cfg["rate_code_col"]).cast(IntegerType()).alias("rate_code_id"),
        get_col(cfg["trip_type_col"]).cast(IntegerType()).alias("trip_type"),
        get_col(cfg["payment_type_col"]).cast(IntegerType()).alias("payment_type"),
        # Temporal Dimensions
        get_col(cfg["request_datetime_col"]).cast(TimestampType()).alias("request_at"),
        get_col(cfg["on_scene_datetime_col"]).cast(TimestampType()).alias("on_scene_at"),
        get_col(cfg["pickup_datetime_col"]).cast(TimestampType()).alias("pickup_at"),
        get_col(cfg["dropoff_datetime_col"]).cast(TimestampType()).alias("dropoff_at"),
        # Spatio-Temporal Location IDs (Fallback to UNKNOWN_ZONE_ID if missing/null)
        F.coalesce(get_col(cfg["pickup_zone_col"]).cast(IntegerType()), F.lit(UNKNOWN_ZONE_ID)).alias("pickup_zone_id"),
        F.coalesce(get_col(cfg["dropoff_zone_col"]).cast(IntegerType()), F.lit(UNKNOWN_ZONE_ID)).alias("dropoff_zone_id"),
        # Operational Metrics
        get_col(cfg["passenger_count_col"]).cast(IntegerType()).alias("passenger_count"),
        get_col(cfg["distance_col"]).cast(DoubleType()).alias("trip_distance_miles"),
        get_col(cfg["shared_match_col"]).cast(StringType()).alias("shared_match_flag"),
        get_col(cfg["wav_match_col"]).cast(StringType()).alias("wav_match_flag"),
        # Financial Breakdown & Surcharges
        get_col(cfg["fare_amount_col"]).cast(DoubleType()).alias("fare_amount"),
        get_col(cfg["extra_col"]).cast(DoubleType()).alias("extra_amount"),
        get_col(cfg["mta_tax_col"]).cast(DoubleType()).alias("mta_tax"),
        get_col(cfg["tip_amount_col"]).cast(DoubleType()).alias("tip_amount"),
        get_col(cfg["tolls_amount_col"]).cast(DoubleType()).alias("tolls_amount"),
        get_col(cfg["improvement_surcharge_col"]).cast(DoubleType()).alias("improvement_surcharge"),
        get_col(cfg["congestion_fee_col"]).cast(DoubleType()).alias("congestion_fee"),
        get_col(cfg["airport_fee_col"]).cast(DoubleType()).alias("airport_fee"),
        get_col(cfg["cbd_congestion_fee_col"]).cast(DoubleType()).alias("cbd_congestion_fee"),
        get_col(cfg["driver_pay_col"]).cast(DoubleType()).alias("driver_pay"),
        get_col(cfg["total_amount_col"]).cast(DoubleType()).alias("total_amount"),
    )
    return harmonized

def enrich_and_filter_trips(df: DataFrame, year_month: str, fleet: str) -> tuple[DataFrame, Dict]:
    """COmputes operational features (speed, duration, wait times)" and applies Quality Gates"""
    enriched = df.withColumn(
        "trip_duration_minutes",
        F.round((F.unix_timestamp("dropoff_at") - F.unix_timestamp("pickup_at")) / 60.0, 2),
    )

    enriched = enriched.withColumn(
        "trip_distance_km",
        F.when(F.col("trip_distance_miles").isNotNull(), F.round(F.col("trip_distance_miles") * MILES_TO_KM, 2)),
    )

    duration_hours_expr = F.col("trip_duration_minutes") / 60.0
    enriched = enriched.withColumn(
        "speed_mph",
        F.when(
            (F.col("trip_distance_miles").isNotNull() & (duration_hours_expr > 0)),
            F.round(F.col("trip_distance_miles") / duration_hours_expr, 2),
        ),
    ).withColumn (
        "speed_kmh",
        F.when(F.col("speed_mph").isNotNull(), F.round(F.col("speed_mph") * MILES_TO_KM, 2)),
    )

    enriched = enriched.withColumn(
        "wait_time_minutes",
        F.when(
            F.col("request_at").isNotNull(),
            F.round((F.unix_timestamp("pickup_at") - F.unix_timestamp("request_at")) / 60.0, 2),
        ),
    )

    enriched = enriched.withColumn("year", F.year("pickup_at")).withColumn("month", F.lpad(F.month("pickup_at"), 2, "0"))

    # --------------------------------------------------------------------------
    # QUALITY GATE AUDITING
    # --------------------------------------------------------------------------
    total_raw_rows = enriched.count()

    target_year, target_month_str = year_month.split("-")
    in_month_filter = (F.col("year") == int(target_year)) & (F.col("month") == target_month_str)
    outside_month_count = enriched.filter(~in_month_filter).count()

    outside_fraction = outside_month_count / total_raw_rows if total_raw_rows > 0 else 0.0
    if outside_fraction > MAX_OUTSIDE_MONTH_FRACTION:
        logger.warning(f"[{fleet.upper()}] Outside-month records ({outside_fraction*100:.2f}%) exceed threshold ({MAX_OUTSIDE_MONTH_FRACTION*100}%).")

    valid_records = enriched.filter(in_month_filter)
    valid_duration = (F.col("trip_duration_minutes") >= MIN_DURATION_MINUTES) & (F.col("trip_duration_minutes") <= (MAX_DURATION_HOURS * 60))
    valid_distance = (F.col("trip_distance_miles").isNull() | (F.col("trip_distance_miles") >= MIN_DISTANCE_MILES))
    valid_speed =  (F.col("speed_mph").isNull() | (F.col("speed_mph") <= MAX_SPEED_MPH))
    valid_fare = (
        (F.col("total_amount").isNull() | (F.col("total_amount") >= 0))
        & (F.col("fare_amount").isNull() | (F.col("fare_amount") >= 0))
        & (F.col("tip_amount").isNull() | (F.col("tip_amount") >= 0))
    )
    valid_wait = F.col("wait_time_minutes").isNull() | (F.col("wait_time_minutes") >= 0)
    valid_passengers = F.col("passenger_count").isNull() | (F.col("passenger_count") >= 0)

    clean_df = valid_records.filter(valid_duration & valid_distance & valid_speed & valid_fare & valid_wait & valid_passengers)
    clean_count = clean_df.count()

    metrics = {
        "fleet": fleet,
        "year_month": year_month,
        "raw_rows": total_raw_rows,
        "retained_clean_rows": clean_count,
        "outside_month_rows": outside_month_count,
        "outside_month_fraction": round(outside_fraction, 6),
        "filtered_anomaly_rows": total_raw_rows - outside_month_count - clean_count,
        "data_retention_rate_pct": round((clean_count / total_raw_rows) * 100, 2) if total_raw_rows > 0 else 0.0,
    }
    return clean_df, metrics

def process_fleet_to_silver(
        spark: SparkSession,
        fleet:str,
        year_month: str
) -> Dict:
    """Reads Bronze Parquet, harmonizes, cleans, and save to Silver Lakehouse"""
    prefix = FLEET_CONFIG[fleet]["file_prefix"]
    bronze_path = BRONZE_DIR / fleet / year_month/ f"{prefix}_{year_month}.parquet"

    if not bronze_path.exists():
        raise FileNotFoundError(f"Bronze file not found: {bronze_path}. Run ingestion first.")

    logger.info(f"[{fleet.upper()}] Reading Bronze Parquet: {bronze_path}")
    raw_df = spark.read.parquet(str(bronze_path))

    logger.info(f"[{fleet.upper()}] Harmonizing schema to Silver Lakehouse standard...")
    harmonize_df = harmonize_fleet_schema(raw_df, fleet)
    
    logger.info(f"[{fleet.upper()}] Applying Quality Gates and feature engineering...")
    clean_df, metrics = enrich_and_filter_trips(harmonize_df, year_month, fleet)

    silver_output_dir = SILVER_DIR / "trips"
    logger.info(f"[{fleet.upper()}] Writing Silver Parquet partitioned by (year, month, fleet)...")

    (
        clean_df.write.mode("overwrite")
        .partitionBy("year", "month", "fleet_type")
        .parquet(str(silver_output_dir))
    )
    logger.info(f"[{fleet.upper()}] Silver write complete! Retained {metrics['retained_clean_rows']:,} clean trips ({metrics['data_retention_rate_pct']}%).")
    return metrics

# ==============================================================================
# CLI EXECUTION & VERIFICATION
# ==============================================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NYC TLC Silver Lakehouse Cleaning Engine")
    parser.add_argument("--month", type=str, default="2025-01", help="Target month (YYYY-MM)")
    parser.add_argument(
        "--fleets",
        nargs="+",
        default=["yellow", "green", "fhv", "fhvhv"],
        help="List of fleets to process (e.g. yellow green fhv fhvhv)",
    )
    args = parser.parse_args()

    print("\n" + "=" * 75)
    print(f"\tSTARTING PYSPARK SILVER TRANSFORMATION ENGINE | TARGET: {args.month}      ")
    print("=" * 75)

    spark = get_spark_session()
    audit_results = []

    try:
        for fleet_name in args.fleets:
            print(f"\n>>> TRANSFORMING FLEET: {fleet_name.upper()} <<<")
            metrics = process_fleet_to_silver(spark, fleet_name, args.month)
            audit_results.append(metrics)

        # Print Executive Quality Audit Table
        print("\n" + "=" * 75)
        print("\t\tSILVER DATA QUALITY AUDIT REPORT")
        print("=" * 75)
        print(f"{'Fleet':<9} | {'Raw Rows':<11} | {'Clean Rows':<11} | {'Outside Mo':<10} | {'Retention %'}")
        print("-" * 75)
        for m in audit_results:
            print(
                f"{m['fleet'].upper():<9} | {m['raw_rows']:<11,} | {m['retained_clean_rows']:<11,} | {m['outside_month_rows']:<10,} | {m['data_retention_rate_pct']}%"
            )
        print("=" * 75)

        # Save audit report to JSON
        audit_file = SILVER_DIR / f"quality_audit_{args.month}.json"
        with open(audit_file, "w", encoding="utf-8") as f:
            json.dump(audit_results, f, indent=4)
        print(f"Quality audit report saved to: {audit_file}")
        print(">>> SUCCESS: SILVER LAKEHOUSE TRANSFORMATION COMPLETED! <<<")
        print("=" * 75 + "\n")

    finally:
        spark.stop()