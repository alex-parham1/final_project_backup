import json
import urllib.parse
import boto3
import pandas as pd
from io import StringIO
import pymysql
import os
import sys

sys.path.append("../")
import cleaning
from dotenv import load_dotenv

debug = os.environ.get("debug")
if debug == "True":
    region = os.environ.get("region_name")
    session = boto3.Session(
        profile_name=os.environ.get("profile_name"), region_name=region
    )
    s3 = session.client("s3")
else:
    s3 = boto3.client("s3")


def lambda_handler(event, context):

    #  Gets the file from the bucket

    bucket = event["Records"][0]["s3"]["bucket"]["name"]

    key = urllib.parse.unquote_plus(
        event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
    )
    print(key)

    try:
        response = s3.get_object(Bucket=bucket, Key=key)

        file_content = response["Body"]

        file_data = file_content.read().decode("utf-8")

        # Data cleaning steps
        df = pd.read_csv(
            StringIO(file_data),
            names=[
                "date",
                "location",
                "customer_name",
                "products",
                "total",
                "payment_type",
                "card_number",
            ],
        )

        df["products"] = df["products"].apply(cleaning.clean_prods_csv)
        df["card_number"] = df["card_number"].apply(cleaning.clean_card_numbers)
        df["date"] = df["date"].apply(cleaning.clean_date_time)

        # Adds clean to the file name

        key = key[0:-23] + "cleaned_" + key[-23:]

        # saves new clean csv to clean bucket

        df.to_csv("/tmp/cleaned_data.csv", index=False)
        if debug == "False":
            response = s3.upload_file(
                Filename="/tmp/cleaned_data.csv",
                Bucket="team-yogurt-cleaned-data",
                Key=key,
            )
        else:
            return True

    except Exception as e:
        print(e)
        print(
            "Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.".format(
                key, bucket
            )
        )
        raise e
