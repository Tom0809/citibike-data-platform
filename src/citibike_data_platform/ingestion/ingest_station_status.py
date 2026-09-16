from datetime import datetime, timezone

from citibike_data_platform.ingestion.gbfs_client import (
    STATION_STATUS_URL,
    fetch_gbfs_feed,
)

from citibike_data_platform.ingestion.s3_writer import (
    write_json_to_s3,
)


BUCKET_NAME = "tombucket2026"


def ingest_station_status() -> None:
    # 1. Fetch current snapshot from Citi Bike API
    data = fetch_gbfs_feed(STATION_STATUS_URL)

    # 2. Record ingestion time in UTC
    now = datetime.now(timezone.utc)

    # 3. Create an immutable S3 object key
    key = (
        f"raw/station_status/"
        f"year={now:%Y}/"
        f"month={now:%m}/"
        f"day={now:%d}/"
        f"hour={now:%H}/"
        f"station_status_{now:%Y%m%d_%H%M%S}.json"
    )

    # 4. Write raw JSON to S3
    write_json_to_s3(
        data=data,
        bucket=BUCKET_NAME,
        key=key,
    )

    print(f"Uploaded to s3://{BUCKET_NAME}/{key}")


if __name__ == "__main__":
    ingest_station_status()