from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp


spark = SparkSession.builder.getOrCreate()


# ============================================================
# Config
# ============================================================

SOURCE_PATH = "s3://tombucket2026/raw/trips/"

SCHEMA_PATH = "s3://tombucket2026/_schemas/bronze/trips"

CHECKPOINT_PATH = "s3://tombucket2026/_checkpoints/bronze/trips"

TARGET_TABLE = "citibike_dev.bronze.trips"


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
        col("_metadata.file_path")
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
        current_timestamp()
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
        CHECKPOINT_PATH
    )
    .trigger(
        availableNow=True
    )
    .toTable(
        TARGET_TABLE
    )
)


bronze_stream.awaitTermination()