from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    trim,
    from_unixtime,
    current_timestamp,
    regexp_replace,
    row_number,
    round,
)

from pyspark.sql.window import Window
from delta.tables import DeltaTable


spark = SparkSession.builder.getOrCreate()


INFO_TABLE = "citibike_dev.bronze.station_information"
STATUS_TABLE = "citibike_dev.bronze.station_status"
TARGET_TABLE = "citibike_dev.silver.stations"


# ============================================================
# 1. Read Bronze tables
# ============================================================

df_info = spark.table(INFO_TABLE)

df_status = spark.table(STATUS_TABLE)


# ============================================================
# 2. Clean Station Information
#    Keep the latest information record for each station
# ============================================================

info_window = (
    Window
    .partitionBy("station_id")
    .orderBy(
        col("last_updated").desc(),
        col("_source_file").desc()
    )
)


df_info_clean = (
    df_info

    .select(
        "station_id",
        "name",
        "lat",
        "lon",
        "capacity",
        "region_id",
        "is_charging",
        "last_updated",
        "_source_file",
    )

    # Remove leading/trailing whitespace
    .withColumn(
        "name",
        trim(col("name"))
    )

    # Example:
    # "W 42" -> "W42"
    .withColumn(
        "name",
        regexp_replace(
            col("name"),
            r"\b([NSEW])\s+(\d+)\b",
            "$1$2"
        )
    )

    # Clearer business name
    .withColumnRenamed(
        "name",
        "address"
    )

    # Coordinate precision
    .withColumn(
        "lat",
        round(col("lat"), 6)
    )

    .withColumn(
        "lon",
        round(col("lon"), 6)
    )

    # Make sure is_charging is boolean
    .withColumn(
        "is_charging",
        col("is_charging").cast("boolean")
    )

    # Rank records inside each station_id
    .withColumn(
        "_row_num",
        row_number().over(info_window)
    )

    # Keep only the newest record
    .filter(
        col("_row_num") == 1
    )

    .drop(
        "_row_num"
    )

    # Convert Unix timestamp -> Spark timestamp
    .withColumn(
        "information_last_updated_at",
        from_unixtime(
            col("last_updated")
        ).cast("timestamp")
    )

    .drop(
        "last_updated"
    )

    # Keep lineage back to Bronze / S3
    .withColumnRenamed(
        "_source_file",
        "information_source_file"
    )
)


# ============================================================
# 3. Clean Station Status
#    Keep the latest status record for each station
# ============================================================

status_window = (
    Window
    .partitionBy("station_id")
    .orderBy(
        col("last_reported").desc(),
        col("last_updated").desc(),
        col("_source_file").desc()
    )
)


df_status_clean = (
    df_status

    .select(
        "station_id",
        "num_bikes_available",
        "num_bikes_disabled",
        "num_docks_available",
        "num_docks_disabled",
        "num_ebikes_available",
        "num_scooters_available",
        "num_scooters_unavailable",
        "is_installed",
        "is_renting",
        "is_returning",
        "last_reported",
        "last_updated",
        "_source_file",
    )

    # Rank status rows inside each station_id
    .withColumn(
        "_row_num",
        row_number().over(status_window)
    )

    # Keep newest status
    .filter(
        col("_row_num") == 1
    )

    .drop(
        "_row_num"
    )

    # Convert last_reported Unix timestamp -> timestamp
    .withColumn(
        "last_reported_at",
        from_unixtime(
            col("last_reported")
        ).cast("timestamp")
    )

    .drop(
        "last_reported"
    )

    # Convert snapshot last_updated -> timestamp
    .withColumn(
        "status_last_updated_at",
        from_unixtime(
            col("last_updated")
        ).cast("timestamp")
    )

    .drop(
        "last_updated"
    )

    # Keep lineage back to Bronze / S3
    .withColumnRenamed(
        "_source_file",
        "status_source_file"
    )
)


# ============================================================
# 4. Join latest Information + latest Status
# ============================================================

df_silver = (
    df_info_clean

    .join(
        df_status_clean,
        on="station_id",
        how="left"
    )

    .withColumn(
        "_silver_updated_at",
        current_timestamp()
    )
)


# ============================================================
# 5. Initial Load OR Merge into Silver
# ============================================================

if not spark.catalog.tableExists(TARGET_TABLE):

    # First run:
    # Silver table does not exist yet.
    (
        df_silver.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TARGET_TABLE)
    )

else:

    # Silver already exists.
    # Load it as a DeltaTable so we can MERGE into it.
    silver_table = DeltaTable.forName(
        spark,
        TARGET_TABLE
    )

    (
        silver_table
        .alias("target")

        .merge(
            df_silver.alias("source"),
            "target.station_id = source.station_id"
        )

        # Same station_id -> update existing Silver row
        .whenMatchedUpdateAll()

        # New station_id -> insert new Silver row
        .whenNotMatchedInsertAll()

        # Execute the MERGE
        .execute()
    )