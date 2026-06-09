import json
import boto3
import csv
import io
import urllib.parse
import logging

s3 = boto3.client("s3")

CONFIG_BUCKET = "filepipeline-config"
CONFIG_KEY = "config.json"

PROCESSED_BUCKET = "filepipeline-processed"
FAILED_BUCKET = "filepipeline-failed"

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# =========================
# MAIN HANDLER
# =========================
def lambda_handler(event, context):

    try:
        record = event["Records"][0]

        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(
            record["s3"]["object"]["key"]
        )

        logger.info(f"File received: {key}")

        config = load_config()

        file_type = detect_file_type(key)

        return process_file(bucket, key, file_type, config)

    except Exception as e:
        logger.exception("Pipeline failed")
        return {"statusCode": 500, "error": str(e)}


# =========================
# LOAD CONFIG (DYNAMIC)
# =========================
def load_config():

    obj = s3.get_object(
        Bucket=CONFIG_BUCKET,
        Key=CONFIG_KEY
    )

    return json.loads(
        obj["Body"].read().decode("utf-8")
    )


# =========================
# DETECT FILE TYPE
# =========================
def detect_file_type(key):

    if key.endswith(".csv"):
        return "csv"

    elif key.endswith(".json"):
        return "json"

    else:
        raise Exception("Unsupported file type")


# =========================
# CORE PROCESSING ENGINE
# =========================
def process_file(bucket, key, file_type, config):

    obj = s3.get_object(Bucket=bucket, Key=key)

    content = obj["Body"].read().decode("utf-8")

    rules = config.get(file_type, {})

    if file_type == "csv":
        return process_csv(content, key, rules)

    elif file_type == "json":
        return process_json(content, key, rules)


# =========================
# CSV PROCESSOR (DYNAMIC)
# =========================
def process_csv(content, key, rules):

    reader = csv.DictReader(io.StringIO(content))

    required = rules.get("required_columns", [])
    transformations = rules.get("transformations", {})

    # Validate schema dynamically
    for col in required:
        if col not in reader.fieldnames:
            raise Exception(f"Missing column: {col}")

    processed = []

    for row in reader:

        processed.append(
            apply_rules(row, transformations)
        )

    return store_result(
        processed,
        key,
        "csv"
    )


# =========================
# JSON PROCESSOR (DYNAMIC)
# =========================
def process_json(content, key, rules):

    data = json.loads(content)

    if isinstance(data, dict):
        data = [data]

    required = rules.get("required_fields", [])
    transformations = rules.get("transformations", {})

    processed = []

    for item in data:

        for field in required:
            if field not in item:
                raise Exception(f"Missing field: {field}")

        processed.append(
            apply_rules(item, transformations)
        )

    return store_result(
        processed,
        key,
        "json"
    )


# =========================
# RULE ENGINE (CORE LOGIC)
# =========================
def apply_rules(row, rules):

    result = {}

    for k, v in row.items():

        if k in rules:

            if rules[k] == "upper":
                result[k] = str(v).upper()

            elif rules[k] == "lower":
                result[k] = str(v).lower()

            else:
                result[k] = v
        else:
            result[k] = v

    return result


# =========================
# STORE OUTPUT
# =========================
def store_result(data, key, file_type):

    output_key = f"processed/{key}"

    if file_type == "csv":

        output = io.StringIO()

        writer = csv.DictWriter(
            output,
            fieldnames=data[0].keys()
        )

        writer.writeheader()
        writer.writerows(data)

        body = output.getvalue()

    else:

        body = json.dumps(data)

    s3.put_object(
        Bucket=PROCESSED_BUCKET,
        Key=output_key,
        Body=body
    )

    logger.info(f"Stored: {output_key}")

    return {
        "status": "SUCCESS",
        "records": len(data),
        "output": output_key
    }
