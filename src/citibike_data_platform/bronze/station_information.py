from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, current_timestamp, col


spark = SparkSession.builder.getOrCreate()


SOURCE_PATH = "s3://tombucket2026/raw/station_information/"

SCHEMA_PATH = "s3://tombucket2026/_schemas/bronze/station_information"

CHECKPOINT_PATH = "s3://tombucket2026/_checkpoints/bronze/station_information"

TARGET_TABLE = "citibike_dev.bronze.station_information"


df_raw = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("multiLine", "true")
    .load(SOURCE_PATH)
    .withColumn("_source_file", col("_metadata.file_path"))
)


df_bronze = (
    df_raw
    .select(
        explode("data.stations").alias("station"),
        "last_updated",
        "ttl",
        "_source_file"
    )
    .select(
        "station.*",
        "last_updated",
        "ttl",
        "_source_file"
    )
    .withColumn("_ingested_at", current_timestamp())
)


bronze_stream = (
    df_bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)

bronze_stream.awaitTermination()