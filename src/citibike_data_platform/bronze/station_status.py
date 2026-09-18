from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, current_timestamp, col

spark = SparkSession.builder.getOrCreate()

SOURCE_PATH = "s3://tombucket2026/raw/station_status/"

TARGET_TABLE = "citibike_dev.bronze.station_status"

df_raw = (
    spark.read
    .option("multiline", "true")
    .json(SOURCE_PATH)
    .withColumn("_source_file", col("_metadata.file_path"))
)

df_bronze = (
    df_raw
    .select(
        explode("data.stations").alias("station"),
        "last_updated",
        "ttl"
    )
    .select(
        "station.*",
        "last_updated",
        "ttl"
    )
    .withColumn("_ingested_at", current_timestamp())
)

df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(TARGET_TABLE)
