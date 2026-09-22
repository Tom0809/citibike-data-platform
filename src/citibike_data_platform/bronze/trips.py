import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp


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

SOURCE_PATH = "s3://tombucket2026/raw/trips/"

SCHEMA_PATH = (
    f"s3://tombucket2026/_schemas/"
    f"{ENVIRONMENT}/bronze/trips"
)

CHECKPOINT_PATH = (
    f"s3://tombucket2026/_checkpoints/"
    f"{ENVIRONMENT}/bronze/trips"
)

TARGET_TABLE = f"{CATALOG}.bronze.trips"


# ============================================================
# 1. Read Citi Bike historical trip CSV files
#    using Databricks Auto Loader
# ============================================================

df_raw = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)
    .option("cloudFiles.partitionColumns", "year,month")
    .option("header", "true")
    .load(SOURCE_PATH)

    # Keep source-file lineage
    .withColumn(
        "_source_file",
        col("_metadata.file_path"),
    )
)


# ============================================================
# 2. Bronze metadata
#
# Do NOT clean / aggregate trip data here.
# Keep the source data as close to raw as possible.
# ============================================================

df_bronze = (
    df_raw
    .withColumn(
        "_ingested_at",
        current_timestamp(),
    )
)


# ============================================================
# 3. Incremental write to Bronze Delta table
# ============================================================

bronze_stream = (
    df_bronze.writeStream
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


bronze_stream.awaitTermination()