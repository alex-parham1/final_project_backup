import sys
sys.path.append("../")
sys.path.append("../src/scripts")
from src.scripts import transactions_and_baskets as tb
from src.scripts import extraction as ex
import json
import boto3
import pandas as pd
import os
import urllib.parse
from io import StringIO

s3 = boto3.client("s3")
region = os.environ.get("eu-west-1")

# yml test


def lambda_handler(event, context,s3=s3,clean_the_data=ex.clean_the_data ,etl=ex.etl, t_and_b=tb.insert_transactions):

    bucket = event["Records"][0]["s3"]["bucket"]["name"]

    key = urllib.parse.unquote_plus(
        event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
    )

    try:
        response = s3.get_object(Bucket=bucket, Key=key)

        file_content = response["Body"]

        file_data = file_content.read().decode("utf-8")

        df = pd.read_csv(StringIO(file_data))

        #  This is where the csv is sat in the file_data and needs reading into the main app

        df = clean_the_data(df)
        etl(df)
        t_and_b(df)

        return {
            "headers": {"Content-Type": "application/json"},
            "statusCode": 200,
            "body": json.dumps({"message": "successful upload", "event": event}),
        }

    except Exception as e:
        print(e)
        print(
            "Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.".format(
                key, bucket
            )
        )
        raise e
