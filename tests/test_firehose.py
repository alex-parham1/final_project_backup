import sys

sys.path.append("../")
sys.path.append("../src/scripts")
from src.scripts import firehose_lambda

test_event = {'invocationId': 'invocationIdExample', 'deliveryStreamArn': 'arn:aws:kinesis:EXAMPLE', 'region': 'us-east-1', 'records': [{'recordId': '49546986683135544286507457936321625675700192471156785154', 'approximateArrivalTimestamp': 1495072949453, 'data': 'ewogICAgImRhdGUiOiAiMDkvMDgvMjAyMiAxNjo0NCIsCiAgICAic3RvcmUiOiAiUGVydGgiLAogICAgImN1c3RvbWVyX25hbWUiOiAiV2FsdGVyIEJhcnRlbHNvbiIsCiAgICAib3JkZXIiOiAiW3tcIm5hbWVcIjogXCJGcmFwcGVzIC0gQ29mZmVlXCIsIFwic2l6ZVwiOiBcIkxhcmdlXCIsIFwicHJpY2VcIjogMy4yNX0sIHtcIm5hbWVcIjogXCJFc3ByZXNzb1wiLCBcInNpemVcIjogXCJMYXJnZVwiLCBcInByaWNlXCI6IDEuOH0sIHtcIm5hbWVcIjogXCJFc3ByZXNzb1wiLCBcInNpemVcIjogXCJMYXJnZVwiLCBcInByaWNlXCI6IDEuOH0sIHtcIm5hbWVcIjogXCJIb3QgQ2hvY29sYXRlXCIsIFwic2l6ZVwiOiBcIkxhcmdlXCIsIFwicHJpY2VcIjogMS43fSwge1wibmFtZVwiOiBcIkZyYXBwZXMgLSBDb2ZmZWVcIiwgXCJzaXplXCI6IFwiTGFyZ2VcIiwgXCJwcmljZVwiOiAzLjI1fV0iLAogICAgInBheW1lbnRfYW1vdW50IjogMTEuOCwKICAgICJwYXltZW50X3R5cGUiOiAiQ0FTSCIsCiAgICAiY2FyZF9udW1iZXIiOiBudWxsCn0K'}]}
context = {}
firehose_lambda.lambda_handler(test_event, context)