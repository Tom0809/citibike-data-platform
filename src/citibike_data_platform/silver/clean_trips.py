import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    regexp_replace,
    current_timestamp,
    round,
)


# ============================================================
# Arguments
# ============================================================

parser = argparse.ArgumentParser()

parser.add_argument(
    "--catalog",
    required=True,
    help="Target Unity Catalog, e.g. citibike_dev or citibike_prod",
)

args = parser.parse_args()

CATALOG = args.catalog


# ============================================================
# Spark
# ============================================================

spark = SparkSession.builder.getOrCreate()


# ============================================================
# Environment
# ============================================================
#
# citibike_dev  -> dev
# citibike_prod -> prod
#

ENVIRONMENT = CATALOG.removeprefix("citibike_")


# ============================================================
# Config
# ============================================================

SOURCE_TABLE = f"{CATALOG}.bronze.trips"

TARGET_TABLE = f"{CATALOG}.silver.trips"

CHECKPOINT_PATH = (
    f"s3://tombucket2026/_checkpoints/"
    f"{ENVIRONMENT}/silver/trips_v3"
)


# ============================================================
# Helper: normalize station IDs
# ============================================================

def normalize_station_id(column_name):

    station_id = trim(col(column_name))

    # Remove trailing underscores
    #
    # 5308.04_   -> 5308.04
    # 5303.06__  -> 5303.06
    station_id = regexp_replace(
        station_id,
        r"_+$",
        "",
    )

    # Remove unnecessary trailing zeros after decimal
    #
    # 5997.10   -> 5997.1
    # 5997.100  -> 5997.1
    # 5308.040  -> 5308.04
    station_id = regexp_replace(
        station_id,
        r"(\.\d*?[1-9])0+$",
        "$1",
    )

    # Decimal part contains only zeros
    #
    # 5997.00 -> 5997
    station_id = regexp_replace(
        station_id,
        r"\.0+$",
        "",
    )

    return station_id


# ============================================================
# 1. Read Bronze incrementally
# ============================================================

df_bronze = (
    spark.readStream
    .table(SOURCE_TABLE)
)


# ============================================================
# 2. Clean and standardize trips
# ============================================================

df_silver = (
    df_bronze

    # --------------------------------------------------------
    # Ride ID
    # --------------------------------------------------------
    .withColumn(
        "ride_id",
        trim(col("ride_id")),
    )

    # --------------------------------------------------------
    # Station IDs
    # --------------------------------------------------------
    .withColumn(
        "start_station_id",
        normalize_station_id("start_station_id"),
    )

    .withColumn(
        "end_station_id",
        normalize_station_id("end_station_id"),
    )

    # --------------------------------------------------------
    # Categories
    # --------------------------------------------------------
    .withColumn(
        "rideable_type",
        lower(trim(col("rideable_type"))),
    )

    .withColumn(
        "member_casual",
        lower(trim(col("member_casual"))),
    )

    # --------------------------------------------------------
    # Timestamp types
    # --------------------------------------------------------
    .withColumn(
        "started_at",
        col("started_at").cast("timestamp"),
    )

    .withColumn(
        "ended_at",
        col("ended_at").cast("timestamp"),
    )

    # --------------------------------------------------------
    # Ride duration
    # --------------------------------------------------------
    .withColumn(
        "ride_duration_minutes",
        round(
            (
                col("ended_at").cast("long")
                - col("started_at").cast("long")
            ) / 60.0,
            2,
        ),
    )

    # --------------------------------------------------------
    # Start station name
    # --------------------------------------------------------
    .withColumn(
        "start_station_name",
        regexp_replace(
            regexp_replace(
                trim(col("start_station_name")),
                r"\s+",
                " ",
            ),
            r"\b([NSEW])\s+(\d+)\b",
            "$1$2",
        ),
    )

    # --------------------------------------------------------
    # End station name
    # --------------------------------------------------------
    .withColumn(
        "end_station_name",
        regexp_replace(
            regexp_replace(
                trim(col("end_station_name")),
                r"\s+",
                " ",
            ),
            r"\b([NSEW])\s+(\d+)\b",
            "$1$2",
        ),
    )

    # --------------------------------------------------------
    # Coordinates
    # --------------------------------------------------------
    .withColumn(
        "start_lat",
        col("start_lat").cast("double"),
    )

    .withColumn(
        "start_lng",
        col("start_lng").cast("double"),
    )

    .withColumn(
        "end_lat",
        col("end_lat").cast("double"),
    )

    .withColumn(
        "end_lng",
        col("end_lng").cast("double"),
    )

    # --------------------------------------------------------
    # Partition columns
    # --------------------------------------------------------
    .withColumn(
        "year",
        col("year").cast("int"),
    )

    .withColumn(
        "month",
        col("month").cast("int"),
    )

    # --------------------------------------------------------
    # Silver metadata
    # --------------------------------------------------------
    .withColumn(
        "_silver_processed_at",
        current_timestamp(),
    )
)


# ============================================================
# 3. Incremental write to Silver
# ============================================================

silver_stream = (
    df_silver.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH,
    )
    .trigger(
        availableNow=True
    )
    .toTable(
        TARGET_TABLE
    )
)


silver_stream.awaitTermination()