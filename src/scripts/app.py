# import sys
# sys.path.append("../")
# from src.scripts import transactions_and_baskets as tb
# from src.scripts import extraction as ex
import extraction as ex
import transactions_and_baskets as tb
import json
import boto3
import pandas as pd
import os
import urllib.parse
from io import StringIO

s3 = boto3.client("s3")
region = os.environ.get("eu-west-1")

# yml test
import botocore.exceptions
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context,s3=s3,clean_the_data=ex.clean_the_data ,etl=ex.etl, t_and_b=tb.insert_transactions):

    bucket = event["Records"][0]["s3"]["bucket"]["name"]

    key = urllib.parse.unquote_plus(
        event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
    )
    print(key)

    try:
        response = s3.get_object(Bucket=bucket, Key=key)

    except Exception as e:
        print(e)
        print(
            "Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.".format(
                key, bucket
            )
        )
        raise e

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

    # except botocore.exceptions.ConnectionError as error:
    #     raise ValueError(f"A connection was unable to be made: {error}")

    # except botocore.exceptions.UnknownCredentialError as error:
    #     raise ValueError(f"The credentials you provided are incorrect: {error}")

    # # except s3.exceptions.RuntimeError as error:
    # #     logger.warn(f"Time out Error. See Cloudwatch for more details: {error}")

    # except botocore.exceptions.ClientError as error:
    #     logger.exception(f"Error with Lambda function. See Cloudwatch for details : {error}")
    #     raise error
