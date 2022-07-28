# import unittest
# from unittest import mock
# import os
# from dotenv import load_dotenv
# create sample event to pass through
import sys
sys.path.append("../")
sys.path.append("../src/scripts")
from src.scripts import extraction_lambda
import json


with open ("test_lambda_event.json") as f:
    event = f.read()
    event = json.loads(event) 

print(event)
print(type(event))


def test_extraction_lambda():

    result = extraction_lambda.lambda_handler(event, None)
    print("************************")
    print(result)
    assert result == True
    