import json
import boto3
import csv
import io
import urllib.parse
from datetime import datetime

s3 = boto3.client("s3")

CONFIG_BUCKET = "filepipeline-config"
CONFIG_KEY = "config.json"


def lambda_handler(event, context):

    record = event["Records"][0]

    bucket = record["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

    config = load_config()

    return process_file(bucket, key, config)


# -------------------------
# LOAD CONFIG DYNAMICALLY
# -------------------------
def load_config():

    obj = s3.get_object(
        Bucket=CONFIG_BUCKET,
        Key=CONFIG_KEY
    )

    return json.loads(
        obj["Body"].read().decode("utf-8")
    )


# -------------------------
# DYNAMIC PROCESSING ENGINE
# -------------------------
def process_file(bucket, key, config):

    obj = s3.get_object(Bucket=bucket, Key=key)

    content = obj["Body"].read().decode("utf-8")

    reader = csv.DictReader(io.StringIO(content))

    rows = []

    for row in reader:

        transformed = apply_transformations(
            row,
            config["csv"]["transformations"]
        )

        transformed["processed_time"] = str(datetime.utcnow())

        rows.append(transformed)

    output = io.StringIO()

    writer = csv.DictWriter(
        output,
        fieldnames=rows[0].keys()
    )

    writer.writeheader()
    writer.writerows(rows)

    output_key = f"processed/{key}"

    s3.put_object(
        Bucket="filepipeline-processed",
        Key=output_key,
        Body=output.getvalue()
    )

    return {
        "rows": len(rows),
        "output": output_key
    }


# -------------------------
# GENERIC TRANSFORMATION ENGINE
# -------------------------
def apply_transformations(row, rules):

    result = {}

    for key, value in row.items():

        if key in rules:

            if rules[key] == "upper":
                result[key] = value.upper()

            elif rules[key] == "lower":
                result[key] = value.lower()

            else:
                result[key] = value

        else:
            result[key] = value

    return result
