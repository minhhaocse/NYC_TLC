"""Master End-to-End Big Data Pipeline Orchestrator.
This CLI script coordinates the end-to-end Medallion Lakehouse lifecycle:

1. Ingestion Engine: Raw Parquet data & GIS shapefile downloads (Bronze).
2. PySpark Cleaning Engine: Schema harmonization & Quality Gates (Silver).
3. PySpark Gold Engine: Analytical Marts aggregation (Gold).
"""

import argparse
import os
from pathlib import Path
import subprocess
import sys
import time
import shutil
from datetime import datetime
from typing import List

from src.spark_cleaner import ensure_windows_hadoop_env
ensure_windows_hadoop_env()

from src.config import BRONZE_DIR, GOLD_DIR, PROJECT_ROOT, SILVER_DIR

def generate_month_range(start_ym: str, end_ym: str) -> List[str]:
    """Generates an inclusive list of YYYY-MM strings between start and end."""
    start_dt = datetime.strptime(start_ym, "%Y-%m")
    end_dt = datetime.strptime(end_ym, "%Y-%m")
    if start_dt > end_dt:
        raise ValueError(f"Start month ({start_ym}) must be <= end month ({end_ym})")
    months = []
    curr = start_dt
    while curr <= end_dt:
        months.append(curr.strftime("%Y-%m"))
        # Move to next month safely
        year = curr.year + (1 if curr.month == 12 else 0)
        month = 1 if curr.month == 12 else curr.month + 1
        curr = datetime(year, month, 1)
    return months

def run_command(command:list[str], step_name: str) -> None:
    """Executes a pipeline step as a subprocess, ensuring fail-fast behavior."""
    print("\n" + "=" * 75)
    print(f"  EXECUTING STEP: {step_name.upper()}")
    print("=" * 75)
    start_t = time.time()

    result = subprocess.run(command, cwd=str(PROJECT_ROOT))
    if result.returncode != 0:
        print(f"\n[ERROR] Step '{step_name}' failed with exit code {result.returncode}!")
        sys.exit(result.returncode)

    elapsed = time.time() - start_t
    print(f"\n>>> [COMPLETED] {step_name} in {elapsed:.2f} seconds. <<<")

def clean_storage_layers(clean_silver: bool, clean_gold: bool) -> None:
    """Safely cleans specified Lakehouse layers before a fresh execution."""
    if clean_silver and (SILVER_DIR / "trips").exists():
        print(f"[CLEANUP] Removing existing Silver data at: {SILVER_DIR / 'trips'}...")
        shutil.rmtree(SILVER_DIR / "trips", ignore_errors=True)
    if clean_gold and GOLD_DIR.exists():
        print(f"[CLEANUP] Removing existing Gold data at: {GOLD_DIR}...")
        for item in GOLD_DIR.iterdir():
            if item.is_dir():
                shutil.rmtree(item, ignore_errors=True)
            elif item.is_file():
                item.unlink(missing_ok=True)

def main():
    parser = argparse.ArgumentParser(description="NYC TLC Master End-toEnd Lakehouse Pipline")

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--month", type=str, default="2025-01", help="Target month in YYYY-MM format")
    group.add_argument("--range", nargs=2, metavar=("START", "END"), help="Month range (e.g. 2025-01 2025-03)")
    group.add_argument("--year", type=str, help="Process entire year (e.g. 2024 or 2025)")
    
    parser.add_argument("--fleets", nargs="+", default=["yellow", "green", "fhv", "fhvhv"], help="Fleets to process (e.g yellow green fhv fhvhv)")
    
    parser.add_argument("--skip-ingest", action="store_true", help="Skip Bronze ingestion step")
    parser.add_argument("--skip-silver", action="store_true", help="Skip Silver PySpark cleaning step")
    parser.add_argument("--skip-gold", action="store_true", help="Skip Gold Mart aggregation step")
    
    parser.add_argument("--clean-all", action="store_true", help="Clean Silver and Gold before re-running")
    parser.add_argument("--clean-silver", action="store_true", help="Clean Silver layer before re-running")
    parser.add_argument("--clean-gold", action="store_true", help="Clean Gold layer before re-running")
    
    args = parser.parse_args()

    if args.range:
        target_months = generate_month_range(args.range[0], args.range[1])
    elif args.year:
        target_months = generate_month_range(f"{args.year}-01", f"{args.year}-12")
    else:
        target_months = [args.month]

    pipeline_start = time.time()
    print("\n" + "#" * 75)
    print("\tSTARTING NYC TLC END-TO-END BIG DATA LAKEHOUSE PIPELINE")
    print(f"\tTarget Months ({len(target_months)}): {', '.join(target_months)}")
    print(f"\tTarget Fleets ({len(args.fleets)}): {', '.join(args.fleets)}")
    print("#" * 75)

    if args.clean_all:
        clean_storage_layers(clean_silver=True, clean_gold=True)
    else:
        clean_storage_layers(clean_silver=args.clean_silver, clean_gold=args.clean_gold)

    python_bin = sys.executable

    # --------------------------------------------------------------------------
    # Process Each Month Through Bronze, Silver, and Gold
    # --------------------------------------------------------------------------
    for idx, ym in enumerate(target_months, 1):
        print("\n" + "*" * 75)
        print(f"\tPROCESSING BATCH {idx}/{len(target_months)}: TARGET MONTH {ym}")
        print("*" * 75)
        # Step 1: Bronze Ingestion
        if not args.skip_ingest:
            ingest_cmd = [python_bin, "-m", "src.ingestion", "--month", ym, "--fleets"] + args.fleets
            run_command(ingest_cmd, "Step 1: Bronze Ingestion Engine")
        else:
            print("\n[INFO] Skipping Step 1: Bronze Ingestion (--skip-ingest enabled).")

        # Step 2: Silver Cleaning
        if not args.skip_silver:
            silver_cmd = [python_bin, "-m", "src.spark_cleaner", "--month", ym, "--fleets"] + args.fleets
            run_command(silver_cmd, "Step 2: Silver PySpark Transformation & Quality Gates")
        else:
            print("\n[INFO] Skipping Step 2: Silver Cleaning (--skip-silver enabled).")

        # Step 3: Gold Aggregation
        if not args.skip_gold:
            gold_cmd = [python_bin, "-m", "src.spark_gold_marts", "--month", ym]
            run_command(gold_cmd, "Step 3: Gold Marts PySpark Aggregation")
        else:
            print("\n[INFO] Skipping Step 3: Gold Marts (--skip-gold enabled).")
    
    # --------------------------------------------------------------------------
    # Pipeline Summary & Storage Verification
    # --------------------------------------------------------------------------
    total_elapsed = time.time() - pipeline_start
    print("\n" + "#" * 75)
    print("\tALL PIPELINE PHASES SUCCESSFULLY EXECUTED & VALIDATED!")
    print(f"\tTotal Pipeline Runtime: {total_elapsed / 60:.2f} minutes ({total_elapsed:.1f}s)     ")
    print("#" * 75)
    print("\n[Lakehouse Storage Status]")
    print(f"  - Bronze Raw Layer : {BRONZE_DIR}")
    print(f"  - Silver Clean Layer: {SILVER_DIR / 'trips'}")
    print(f"  - Gold Marts Layer : {GOLD_DIR}")
    print("\n>>> DATA STORAGE & PROCESSING PHASE IS OFFICIALLY COMPLETE! <<<\n")

if __name__ == "__main__":
    main()