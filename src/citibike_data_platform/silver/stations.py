from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    from_unixtime,
    current_timestamp,
    row_number,
)
from pyspark.sql.window import Window


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
# 2. Station information
# ============================================================

# If Bronze later contains multiple versions of the same station,
# keep the most recently updated one.
info_window = (
    Window
    .partitionBy("station_id")
    .orderBy(col("last_updated").desc())
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
        "last_updated",
    )

    # Light text cleaning
    .withColumn(
        "name",
        trim(col("name"))
    )

    # Keep latest information record for each station
    .withColumn(
        "_row_num",
        row_number().over(info_window)
    )
    .filter(col("_row_num") == 1)
    .drop("_row_num")

    .withColumnRenamed(
        "last_updated",
        "information_last_updated"
    )
)


# ============================================================
# 3. Station status
# ============================================================

# Keep the most recent status record for each station.
status_window = (
    Window
    .partitionBy("station_id")
    .orderBy(col("last_reported").desc())
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
    )

    # Keep latest status record for each station
    .withColumn(
        "_row_num",
        row_number().over(status_window)
    )
    .filter(col("_row_num") == 1)
    .drop("_row_num")

    # Convert Unix timestamp to Spark timestamp
    .withColumn(
        "last_reported_at",
        from_unixtime(col("last_reported")).cast("timestamp")
    )

    .drop("last_reported")

    .withColumnRenamed(
        "last_updated",
        "status_last_updated"
    )
)


# ============================================================
# 4. Join station information + status
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
# 5. Write Silver Delta table
# ============================================================

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)