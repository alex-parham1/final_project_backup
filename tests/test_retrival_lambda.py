import unittest
from unittest import mock
import os
from dotenv import load_dotenv
# create sample event to pass through

# def lambda_handler(event, context):

#     bucket = event["Records"][0]["s3"]["bucket"]["name"]
#     print(event)

#     key = urllib.parse.unquote_plus(
#         event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
#     )

#     try:
#         response = s3.get_object(Bucket=bucket, Key=key)

#         file_content = response["Body"]

#         file_data = file_content.read().decode("utf-8")

#         # Data cleaning steps
#         df = pd.read_csv(
#             StringIO(file_data),
#             names=[
#                 "date",
#                 "location",
#                 "customer_name",
#                 "products",
#                 "total",
#                 "payment_type",
#                 "card_number",
#             ],
#         )

#         df["products"] = df["products"].apply(cleaning.clean_prods_csv)
#         df["card_number"] = df["card_number"].apply(cleaning.clean_card_numbers)
#         df["date"] = df["date"].apply(cleaning.clean_date_time)

#         # Adds clean to the file name

#         key = key[0:-23] + "cleaned_" + key[-23:]

#         # saves new clean csv to clean bucket

#         df.to_csv("/tmp/cleaned_data.csv", index=False)
#         if os.environ.get("debug") == "False":
#             response = s3.upload_file(
#                 Filename="/tmp/cleaned_data.csv", Bucket="team-yogurt-cleaned-data", Key=key
#             )

#     except Exception as e:
#         print(e)
#         print(
#             "Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.".format(
#                 key, bucket
#             )
#         )
#         raise e




# # setting the default AWS region enviroment required by the Python SDK boto3
# with mock.patch.dict('os.environ', {'AWS_REGION': 'eu-west-1'}):
#     from extraction_lambda import lambda_handler

# # mock call to S3 to read csv
# def mocked_read_s3_bucket_file(bucket_name, key_name)
#     return "something"



# class ExtractFilesTest(unittest.TestCase)


