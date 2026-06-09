import json
import boto3
import urllib.parse
import logging

s3 = boto3.client("s3")
sns = boto3.client("sns")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SNS_TOPIC_ARN = "YOUR_SNS_TOPIC_ARN"


def lambda_handler(event, context):

    logger.info("Lambda triggered")
    logger.info(json.dumps(event))

    try:

        if "Records" in event and "s3" in event["Records"][0]:

            record = event["Records"][0]

            bucket = record["s3"]["bucket"]["name"]
            key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

            process_file(bucket, key)


        elif "Records" in event and "body" in event["Records"][0]:

            for record in event["Records"]:
                body = json.loads(record["body"])

                bucket = body.get("bucket")
                key = body.get("file") or body.get("key")

                if bucket and key:
                    process_file(bucket, key)


        else:

            bucket = event.get("bucket")
            key = event.get("file") or event.get("key")

            if bucket and key:
                process_file(bucket, key)
            else:
                raise ValueError("Invalid event format")

        return {"statusCode": 200, "body": "Success"}

    except Exception as e:
        logger.exception("FAILED")
        send_failure_alert(str(e))

        return {"statusCode": 500, "body": str(e)}


def process_file(bucket, key):

    logger.info(f"Processing s3://{bucket}/{key}")

    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj["Body"].read().decode("utf-8")

    logger.info(f"File size: {len(content)}")

    return True


def send_failure_alert(message):

    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message,
            Subject="Lambda Failure"
        )
    except Exception as e:
        logger.error(e)
