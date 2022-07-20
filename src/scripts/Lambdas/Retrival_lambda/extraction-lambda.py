import json
import urllib.parse
import boto3
import pandas as pd
from io import StringIO
import pymysql
import os
import sys
import logging
import scripts.database as db
import cleaning

s3 = boto3.client("s3")
region = os.environ.get("eu-west-1")


def lambda_handler(event, context):

    #  Gets the file from the bucket

    bucket = event["Records"][0]["s3"]["bucket"]["name"]

    key = urllib.parse.unquote_plus(
        event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
    )

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
                "payment_type",
                "total",
                "card_number",
            ],
        )

        df["products"] = df["products"].apply(cleaning.clean_prods_csv)

        df["card_number"] = df["card_number"].apply(cleaning.clean_card_numbers)

        # Adds clean to the file name

        key = key[0:-23] + "cleaned_" + key[-23:]

        # saves new clean csv to clean bucket

        df.to_csv("/tmp/cleaned_data.csv")

        response = s3.upload_file(
            Filename="/tmp/cleaned_data.csv", Bucket="team-yogurt-cleaned-data", Key=key
        )

    except Exception as e:
        print(e)
        print(
            "Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.".format(
                key, bucket
            )
        )
        raise e


# database stuff

# print("get connection")
# connection=db.get_connection()
# print("connection")
# db.close_connection(connection, cursor=cursor)

#     cursor=connection.cursor()
# sql="""
# INSERT INTO customers (name)
#     VALUES ("benham")
# """
# cursor.execute(sql)
# #data=cursor.fetchall()
# connection.commit()

# #print(data)
