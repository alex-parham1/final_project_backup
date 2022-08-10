import sys

sys.path.append("../")
sys.path.append("../src/scripts")
from src.scripts import firehose_lambda

test_event = {
    "invocationId": "77b8f50b-2317-4860-9a63-838ef6c751d1",
    "sourceKinesisStreamArn": "arn:aws:kinesis:eu-west-1:156058766667:stream/team-3-data",
    "deliveryStreamArn": "arn:aws:firehose:eu-west-1:156058766667:deliverystream/team-yogurt-tf-stream",
    "region": "eu-west-1",
    "records": [
        {
            "recordId": "49632147152541681040660808910683433819429361226392535042000000",
            "approximateArrivalTimestamp": 1660075311470,
            "data": "eyJkYXRlIjogIjA5LzA4LzIwMjIgMDk6MDAiLCAic3RvcmUiOiAiQ2hlc3RlciIsICJjdXN0b21lcl9uYW1lIjogIkFudGhvbnkgRG9zc2V0dCIsICJvcmRlciI6ICJbe1wibmFtZVwiOiBcIkNvcnRhZG9cIiwgXCJzaXplXCI6IFwiTGFyZ2VcIiwgXCJwcmljZVwiOiAyLjM1fV0iLCAicGF5bWVudF9hbW91bnQiOiAyLjM1LCAicGF5bWVudF90eXBlIjogIkNBUkQiLCAiY2FyZF9udW1iZXIiOiAyMDc3NDc4MzMxMzA3OTY2fQ==",
            "kinesisRecordMetadata": {
                "sequenceNumber": "49632147152541681040660808910683433819429361226392535042",
                "subsequenceNumber": 0,
                "partitionKey": "Chester",
                "shardId": "shardId-000000000000",
                "approximateArrivalTimestamp": 1660075311470,
            },
        },
        {
            "recordId": "49632147152229470607881380185535320347684166805558919202000000",
            "approximateArrivalTimestamp": 1660075307334,
            "data": "eyJkYXRlIjogIjA5LzA4LzIwMjIgMDk6MDAiLCAic3RvcmUiOiAiQ2FtYnJpZGdlIiwgImN1c3RvbWVyX25hbWUiOiAiU3RldmVuIENhcm9kaW5lIiwgIm9yZGVyIjogIlt7XCJuYW1lXCI6IFwiRmxhdm91cmVkIGhvdCBjaG9jb2xhdGUgLSBDYXJhbWVsXCIsIFwic2l6ZVwiOiBcIlJlZ3VsYXJcIiwgXCJwcmljZVwiOiAyLjZ9LCB7XCJuYW1lXCI6IFwiRmxhdm91cmVkIGhvdCBjaG9jb2xhdGUgLSBDYXJhbWVsXCIsIFwic2l6ZVwiOiBcIkxhcmdlXCIsIFwicHJpY2VcIjogMi45fSwge1wibmFtZVwiOiBcIkVzcHJlc3NvXCIsIFwic2l6ZVwiOiBcIkxhcmdlXCIsIFwicHJpY2VcIjogMS44fSwge1wibmFtZVwiOiBcIkZsYXZvdXJlZCBsYXR0ZSAtIFZhbmlsbGFcIiwgXCJzaXplXCI6IFwiTGFyZ2VcIiwgXCJwcmljZVwiOiAyLjg1fV0iLCAicGF5bWVudF9hbW91bnQiOiAxMC4xNSwgInBheW1lbnRfdHlwZSI6ICJDQVJEIiwgImNhcmRfbnVtYmVyIjogMjkxNjYxMzc4MDYzMjEzN30=",
            "kinesisRecordMetadata": {
                "sequenceNumber": "49632147152229470607881380185535320347684166805558919202",
                "subsequenceNumber": 0,
                "partitionKey": "Cambridge",
                "shardId": "shardId-000000000002",
                "approximateArrivalTimestamp": 1660075307334,
            },
        },
    ],
}
context = {}
firehose_lambda.lambda_handler(test_event, context)
