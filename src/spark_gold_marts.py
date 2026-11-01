"""PySpark Gold Analytical Marts Aggregation Module.

This module provides a production-grade template to aggregate clean Silver Lakehouse
trips into high-performance Gold Data Marts for BI Dashboards and GIS Maps.
Team members can follow the sample pattern below to build specialized marts.
"""
import logging
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.spark_cleaner import ensure_windows_hadoop_env, get_spark_session

ensure_windows_hadoop_env()

from src.config import (
    GOLD_DIR,
    LOGS_DIR,
    METADATA_DIR,
    PROJECT_ROOT,
    SILVER_DIR,
    SPARK_APP_NAME,
    SPARK_DRIVER_MEMORY,
    SPARK_SQL_SHUFFLE_PARTITIONS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOGS_DIR / "spark_gold.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("GoldMartsBuilder")

def build_sample_zone_daily_metrics(
        spark: SparkSession,
        silver_df: DataFrame,
        zone_lookup_df: DataFrame,
) -> Path:
    """Aggregates daily trip volumes, speeds, and revenues by Pickup Borough & Zone.
    
    This serves as a template for team members to build custom marts for their dashboards.
    """ 
    logger.info("Building Sample Mart: Zone Daily Metrics joined with GIS Metadata...")

    joined_df = silver_df.join(
        zone_lookup_df.select(
            F.col("LocationID").alias("lookup_zone_id"),
            F.col("Borough").alias("pickup_borough"),
            F.col("Zone").alias("pickup_zone_name"),
            F.col("service_zone").alias("pickup_service_zone"),
        ),
        silver_df.pickup_zone_id == F.col("lookup_zone_id"),
        how="left",
    )

    sample_mart_df = (
        joined_df.withColumn("trip_date", F.to_date("pickup_at"))
        .groupBy("trip_date", "pickup_borough", "pickup_zone_name", "fleet_type")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.avg("trip_distance_km"), 2).alias("avg_distance_km"),
            F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
            F.round(F.avg("speed_kmh"), 2).alias("avg_speed_kmh"),
            F.round(F.sum("fare_amount"), 2).alias("total_fare_usd"),
            F.round(F.sum("tip_amount"), 2).alias("total_tip_usd"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue_usd"),
        )
        .orderBy("trip_date", "pickup_borough", "pickup_zone_name")
    )

    output_path = GOLD_DIR / "sample_zone_daily_metrics.parquet"
    logger.info(f"Saving Gold Mart Parquet to: {output_path}")

    sample_mart_df.coalesce(1).write.mode("overwrite").parquet(str(output_path))
    temp_dir = output_path / "_temporary"
    if temp_dir.exists():
        shutil.rmtree(temp_dir, ignore_errors=True)
    logger.info("Sample Gold Mart successfully built and verified!")
    return output_path

def build_sample_zone_daily_metrics_sql(
    spark: SparkSession,
    silver_df: DataFrame,
    zone_lookup_df: DataFrame,
) -> Path:
    """Alternative implementation using pure Spark SQL."""
    # 1. Đăng ký bảng tạm trong bộ nhớ Spark
    silver_df.createOrReplaceTempView("silver_trips")
    zone_lookup_df.createOrReplaceTempView("zone_lookup")

    # 2. Viết câu truy vấn SQL chuẩn mực
    sample_mart_df = spark.sql("""
        SELECT 
            DATE(s.pickup_at) AS trip_date,
            z.Borough AS pickup_borough,
            z.Zone AS pickup_zone_name,
            s.fleet_type,
            COUNT(*) AS total_trips,
            ROUND(AVG(s.trip_distance_km), 2) AS avg_distance_km,
            ROUND(AVG(s.trip_duration_minutes), 2) AS avg_duration_minutes,
            ROUND(AVG(s.speed_kmh), 2) AS avg_speed_kmh,
            ROUND(SUM(s.fare_amount), 2) AS total_fare_usd,
            ROUND(SUM(s.tip_amount), 2) AS total_tip_usd,
            ROUND(SUM(s.total_amount), 2) AS total_revenue_usd
        FROM silver_trips s
        LEFT JOIN zone_lookup z ON s.pickup_zone_id = z.LocationID
        GROUP BY 1, 2, 3, 4
        ORDER BY trip_date, pickup_borough, pickup_zone_name
    """)

    output_path = GOLD_DIR / "sample_zone_daily_metrics.parquet"
    sample_mart_df.coalesce(1).write.mode("overwrite").parquet(str(output_path))
    temp_dir = output_path / "_temporary"
    if temp_dir.exists():
        shutil.rmtree(temp_dir, ignore_errors=True)
    return output_path

# ==============================================================================
# CLI EXECUTION & VERIFICATION
# ==============================================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NYC TLC Gold Analytical Marts Aggregation Engine")
    parser.add_argument("--month", type=str, default="all", help="Target month (YYYY-MM) or 'all' to aggregate entire Silver horizon")
    args = parser.parse_args()

    print("\n" + "=" * 75)
    print(f"\tSTARTING PYSPARK GOLD MARTS ENGINE | TARGET MONTH: {args.month}")
    print("=" * 75)

    spark = get_spark_session()

    try:
        # Step 1: Read clean partitioned Silver Lakehouse data
        silver_trips_path = SILVER_DIR / "trips"
        if not silver_trips_path.exists():
            raise FileNotFoundError(f"Silver trips not found at {silver_trips_path}. Run spark_cleaner.py first!")

        logger.info(f"Loading partitioned Silver data from: {silver_trips_path}")
        silver_df = spark.read.parquet(str(silver_trips_path))

        # If specific month requested, filter it; otherwise aggregate all months in Silver
        if args.month != "all":
            target_year, target_month_str = args.month.split("-")
            silver_input_df = silver_df.filter(
                (F.col("year") == int(target_year)) & (F.col("month") == target_month_str)
            )
        else:
            silver_input_df = silver_df

        # Step 2: Read Taxi Zone Lookup Metadata
        lookup_path = METADATA_DIR / "taxi_zone_lookup.csv"
        if not lookup_path.exists():
            raise FileNotFoundError(f"Taxi zone lookup CSV missing at {lookup_path}.")

        logger.info(f"Loading Taxi Zone metadata from: {lookup_path}")
        zone_lookup_df = spark.read.option("header", "true").option("inferSchema", "true").csv(str(lookup_path))

        # Step 3: Build the Sample Mart
        output_file = build_sample_zone_daily_metrics(spark, silver_input_df, zone_lookup_df)

        # Step 4: Quick Preview of the Sample Mart
        print("\n" + "=" * 75)
        print("\t\tSAMPLE GOLD MART PREVIEW (TOP 10 ROWS)")
        print("=" * 75)
        preview_df = spark.read.parquet(str(output_file))
        preview_df.show(10, truncate=False)

        total_mart_rows = preview_df.count()
        print(f"Total aggregated rows in Gold Mart: {total_mart_rows:,}")
        print(f"File stored at: {output_file}")
        print("=" * 75)
        print(">>> SUCCESS: GOLD MARTS PIPELINE EXECUTED SUCCESSFULLY! <<<")
        print("=" * 75 + "\n")

    finally:
        spark.stop()