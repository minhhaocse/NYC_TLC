"""Centralized Configuration & Lakehouse Infrastructure Module.

This module acts as the Single Source of Truth for:
1. Dynamic, OS-independent project root and directory hierarchy resolution using pathlib.
2. Automated provisioning of Medallion Lakehouse storage directories (Bronze, Silver, Gold, Metadata, Logs).
3. Official NYC TLC CloudFront CDN endpoints and GIS reference data source URLs.
4. Comprehensive multi-fleet schema mapping covering all 4 fleets (Yellow, Green, FHV, FHVHV / Uber & Lyft).
5. Establishing rigorous Data Quality Gate thresholds (boundary limits, duration bounds, speed caps, unknown zone fallbacks).
6. Local SparkSession compute optimization parameters tuned for 12-16 GB RAM laptops.
7. International unit conversion constants (US Imperial to International Metric: miles/km, mph/kmh).
"""

from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent

DATA_DIR = PROJECT_ROOT / "data"

METADATA_DIR = DATA_DIR / "metadata" / "taxi_zones"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
LOGS_DIR = PROJECT_ROOT / "logs"

for directory in [METADATA_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR, LOGS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TLC_PAGE_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
ZONE_SHAPEFILE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"

FLEET_CONFIG = {
    # --------------------------------------------------------------------------
    # 1. YELLOW TAXI (20 Columns)
    # --------------------------------------------------------------------------
    "yellow": {
        "display_name": "Yellow Medallion Taxi",
        "file_prefix": "yellow_tripdata",
        
        # Operational Identifiers
        "vendor_id_col": "VendorID",                 # 1=Creative Mobile, 2=VeriFone
        "license_num_col": None,
        "dispatch_base_col": None,
        "rate_code_col": "RatecodeID",              # 1=Standard, 2=JFK, 3=Newark,...
        "trip_type_col": None,
        "payment_type_col": "payment_type",          # 1=Credit Card, 2=Cash, 3=No Charge,...
        
        # Temporal & Spatio-Temporal Dimensions
        "request_datetime_col": None,
        "on_scene_datetime_col": None,
        "pickup_datetime_col": "tpep_pickup_datetime",
        "dropoff_datetime_col": "tpep_dropoff_datetime",
        "pickup_zone_col": "PULocationID",
        "dropoff_zone_col": "DOLocationID",
        
        # Operational Metrics
        "passenger_count_col": "passenger_count",
        "distance_col": "trip_distance",
        "shared_match_col": None,
        "wav_match_col": None,
        
        # Financial Breakdown & Surcharges
        "fare_amount_col": "fare_amount",
        "extra_col": "extra",
        "mta_tax_col": "mta_tax",
        "tip_amount_col": "tip_amount",
        "tolls_amount_col": "tolls_amount",
        "improvement_surcharge_col": "improvement_surcharge",
        "congestion_fee_col": "congestion_surcharge",
        "airport_fee_col": "Airport_fee",
        "cbd_congestion_fee_col": "cbd_congestion_fee",
        "driver_pay_col": None,
        "total_amount_col": "total_amount",
    },
    # --------------------------------------------------------------------------
    # 2. GREEN TAXI (21 Columns)
    # --------------------------------------------------------------------------
    "green": {
        "display_name": "Green Boro Taxi (SHL)",
        "file_prefix": "green_tripdata",
        
        # Operational Identifiers
        "vendor_id_col": "VendorID",
        "license_num_col": None,
        "dispatch_base_col": None,
        "rate_code_col": "RatecodeID",
        "trip_type_col": "trip_type",                # 1=Street-hail, 2=Dispatch
        "payment_type_col": "payment_type",
        
        # Temporal & Spatio-Temporal Dimensions
        "request_datetime_col": None,
        "on_scene_datetime_col": None,
        "pickup_datetime_col": "lpep_pickup_datetime",
        "dropoff_datetime_col": "lpep_dropoff_datetime",
        "pickup_zone_col": "PULocationID",
        "dropoff_zone_col": "DOLocationID",
        
        # Operational Metrics
        "passenger_count_col": "passenger_count",
        "distance_col": "trip_distance",
        "shared_match_col": None,
        "wav_match_col": None,
        
        # Financial Breakdown & Surcharges
        "fare_amount_col": "fare_amount",
        "extra_col": "extra",
        "mta_tax_col": "mta_tax",
        "tip_amount_col": "tip_amount",
        "tolls_amount_col": "tolls_amount",
        "improvement_surcharge_col": "improvement_surcharge",
        "congestion_fee_col": "congestion_surcharge",
        "airport_fee_col": None,
        "cbd_congestion_fee_col": "cbd_congestion_fee",
        "driver_pay_col": None,
        "total_amount_col": "total_amount",
    },
    # --------------------------------------------------------------------------
    # 3. FHV - LIVERY / BLACK CAR (7 Columns)
    # --------------------------------------------------------------------------
    "fhv": {
        "display_name": "For-Hire Vehicle (Livery / Black Car)",
        "file_prefix": "fhv_tripdata",
        
        # Operational Identifiers
        "vendor_id_col": None,
        "license_num_col": None,
        "dispatch_base_col": "dispatching_base_num",
        "rate_code_col": None,
        "trip_type_col": None,
        "payment_type_col": None,
        
        # Temporal & Spatio-Temporal Dimensions (Note TLC Case-Sensitivity)
        "request_datetime_col": None,
        "on_scene_datetime_col": None,
        "pickup_datetime_col": "pickup_datetime",
        "dropoff_datetime_col": "dropOff_datetime",  # Capital 'Off'
        "pickup_zone_col": "PUlocationID",          # Lowercase 'location'
        "dropoff_zone_col": "DOlocationID",
        
        # Operational Metrics
        "passenger_count_col": None,
        "distance_col": None,
        "shared_match_col": "SR_Flag",              # 1=Shared Ride
        "wav_match_col": None,
        
        # Financial Breakdown (Not publicly reported by TLC for FHV)
        "fare_amount_col": None,
        "extra_col": None,
        "mta_tax_col": None,
        "tip_amount_col": None,
        "tolls_amount_col": None,
        "improvement_surcharge_col": None,
        "congestion_fee_col": None,
        "airport_fee_col": None,
        "cbd_congestion_fee_col": None,
        "driver_pay_col": None,
        "total_amount_col": None,
    },
    # --------------------------------------------------------------------------
    # 4. FHVHV - HIGH VOLUME (UBER / LYFT) (25 Columns)
    # --------------------------------------------------------------------------
    "fhvhv": {
        "display_name": "High Volume FHV (Uber / Lyft)",
        "file_prefix": "fhvhv_tripdata",
        
        # Operational Identifiers
        "vendor_id_col": None,
        "license_num_col": "hvfhs_license_num",      # HV0003=Uber, HV0005=Lyft
        "dispatch_base_col": "dispatching_base_num",
        "rate_code_col": None,
        "trip_type_col": None,
        "payment_type_col": None,                    # In-app digital payments
        
        # Temporal & Spatio-Temporal Dimensions
        "request_datetime_col": "request_datetime",
        "on_scene_datetime_col": "on_scene_datetime",
        "pickup_datetime_col": "pickup_datetime",
        "dropoff_datetime_col": "dropoff_datetime",
        "pickup_zone_col": "PULocationID",
        "dropoff_zone_col": "DOLocationID",
        
        # Operational Metrics
        "passenger_count_col": None,
        "distance_col": "trip_miles",               # Named 'trip_miles'
        "shared_match_col": "shared_match_flag",     # Y/N Carpooling
        "wav_match_col": "wav_match_flag",           # Y/N Wheelchair Accessible
        
        # Financial Breakdown & Surcharges
        "fare_amount_col": "base_passenger_fare",
        "extra_col": None,
        "mta_tax_col": "sales_tax",                  # NY State Sales Tax
        "tip_amount_col": "tips",                   # Named 'tips'
        "tolls_amount_col": "tolls",
        "improvement_surcharge_col": "bcf",          # Black Car Fund
        "congestion_fee_col": "congestion_surcharge",
        "airport_fee_col": "airport_fee",
        "cbd_congestion_fee_col": "cbd_congestion_fee",
        "driver_pay_col": "driver_pay",             # Take-home driver earnings
        "total_amount_col": "base_passenger_fare",
    },
}

# Ingestion Boundary Thresholds
MAX_OUTSIDE_MONTH_FRACTION = 0.01

# Unit Conversion Constants
MILES_TO_KM = 1.60934

# Trip Logic Validation Thresholds
MIN_DURATION_MINUTES = 0.5
MAX_DURATION_HOURS = 12
MIN_DISTANCE_MILES = 0.01
MAX_SPEED_MPH = 100.0
MAX_SPEED_KPH = MAX_SPEED_MPH * MILES_TO_KM
# Zone Fallback ID for Missing / Unmatched Locations
UNKNOWN_ZONE_ID = 26

# Spark runtime optimization settings
SPARK_APP_NAME = "NYC_TLC_Lakehouse_Pipeline"
SPARK_DRIVER_MEMORY = "4g"
SPARK_SQL_SHUFFLE_PARTITIONS = 8	# Tuned to match local CPU core count