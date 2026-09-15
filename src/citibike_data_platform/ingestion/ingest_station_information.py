from datetime import datetime, timezone

from citibike_data_platform.ingestion.gbfs_client import (
    STATION_INFORMATION_URL,
    fetch_gbfs_feed,
)

from citibike_data_platform.ingestion.s3_writer import (
    write_json_to_s3,
)


BUCKET_NAME = "bike-s3-bucket-963910217446-ca-central-1-an"


def ingest_station_information() -> None:
    data = fetch_gbfs_feed(STATION_INFORMATION_URL)

    now = datetime.now(timezone.utc)

    key = (
        f"raw/station_information/"
        f"year={now:%Y}/"
        f"month={now:%m}/"
        f"day={now:%d}/"
        f"station_information_{now:%Y%m%d_%H%M%S}.json"
    )

    write_json_to_s3(
        data=data,
        bucket=BUCKET_NAME,
        key=key,
    )

    print(f"Uploaded to s3://{BUCKET_NAME}/{key}")


if __name__ == "__main__":
    ingest_station_information()