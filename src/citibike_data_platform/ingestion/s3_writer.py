import json

import boto3


def write_json_to_s3(
    data: dict,
    bucket: str,
    key: str,
    profile_name: str = "citibike",
) -> None:
    session = boto3.Session(profile_name=profile_name)

    s3 = session.client("s3")

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json",
    )