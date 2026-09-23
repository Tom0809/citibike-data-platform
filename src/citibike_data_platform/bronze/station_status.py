import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, current_timestamp, col


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
# Paths
# ============================================================

SOURCE_PATH = "s3://tombucket2026/raw/station_status/"

SCHEMA_PATH = (
    f"s3://tombucket2026/_schemas/"
    f"{ENVIRONMENT}/bronze/station_status"
)

CHECKPOINT_PATH = (
    f"s3://tombucket2026/_checkpoints/"
    f"{ENVIRONMENT}/bronze/station_status"
)

TARGET_TABLE = f"{CATALOG}.bronze.station_status"


# ============================================================
# Read raw station status using Auto Loader
# ============================================================

df_raw = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)
    .option("cloundFiles.schemaEvolutionMode", "rescue")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("multiLine", "true")
    .load(SOURCE_PATH)
    .withColumn(
        "_source_file",
        col("_metadata.file_path"),
    )
)


# ============================================================
# Bronze transformation
# ============================================================

df_bronze = (
    df_raw
    .select(
        explode("data.stations").alias("station"),
        "last_updated",
        "ttl",
        "_source_file",
    )
    .select(
        "station.*",
        "last_updated",
        "ttl",
        "_source_file",
    )
    .withColumn(
        "_ingested_at",
        current_timestamp(),
    )
)


# ============================================================
# Write Bronze Delta table
# ============================================================

bronze_stream = (
    df_bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH,
    )
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)


bronze_stream.awaitTermination()