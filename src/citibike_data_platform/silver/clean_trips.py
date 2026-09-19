from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    regexp_replace,
    current_timestamp,
    round,
)


spark = SparkSession.builder.getOrCreate()

SOURCE_TABLE = "citibike_dev.bronze.trips"
TARGET_TABLE = "citibike_dev.silver.trips"

CHECKPOINT_PATH = "s3://tombucket2026/_checkpoints/silver/trips"


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

    # IDs
    .withColumn("ride_id", trim(col("ride_id")))
    .withColumn("start_station_id", trim(col("start_station_id")))
    .withColumn("end_station_id", trim(col("end_station_id")))

    # Categories
    .withColumn("rideable_type", lower(trim(col("rideable_type"))))
    .withColumn("member_casual", lower(trim(col("member_casual"))))

    # Timestamp types
    .withColumn("started_at", col("started_at").cast("timestamp"))
    .withColumn("ended_at", col("ended_at").cast("timestamp"))

    # Ride duration in minutes
    .withColumn(
        "ride_duration_minutes",
        round(
            (
                col("ended_at").cast("long")
                - col("started_at").cast("long")
            ) / 60.0,
            2
        )
    )

    # Clean start station name
    .withColumn(
        "start_station_name",
        regexp_replace(
            regexp_replace(
                trim(col("start_station_name")),
                r"\s+",
                " "
            ),
            r"\b([NSEW])\s+(\d+)\b",
            "$1$2"
        )
    )

    # Clean end station name
    .withColumn(
        "end_station_name",
        regexp_replace(
            regexp_replace(
                trim(col("end_station_name")),
                r"\s+",
                " "
            ),
            r"\b([NSEW])\s+(\d+)\b",
            "$1$2"
        )
    )

    # Coordinates
    .withColumn("start_lat", col("start_lat").cast("double"))
    .withColumn("start_lng", col("start_lng").cast("double"))
    .withColumn("end_lat", col("end_lat").cast("double"))
    .withColumn("end_lng", col("end_lng").cast("double"))

    # Partition columns
    .withColumn("year", col("year").cast("int"))
    .withColumn("month", col("month").cast("int"))

    # Silver metadata
    .withColumn("_silver_processed_at", current_timestamp())
)


# ============================================================
# 3. Incremental write to Silver
# ============================================================

silver_stream = (
    df_silver.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)

silver_stream.awaitTermination()