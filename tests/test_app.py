import sys
sys.path.append("../")
sys.path.append("../src/scripts")
from src.scripts import app as ap
import boto3
from unittest.mock import Mock
import pytest
import json
from moto import mock_s3

test_s3_event = {
    "Records": [{
        "s3": {
            'bucket': {'name': 'test_bucket'},
            'object': {'key': 'example/s3/path/key/test_data.csv'}
        }
    }]}

@mock_s3
def test_lambda_handler_db():
    s3_client = boto3.client('s3')
    test_bucket = 'test_bucket'
    test_data = b'date,location,customer_name,products,total,payment_type,card_number\n"17/07/2022 09:00",Belfast,"Harold Moore","Large - Speciality Tea - Green - 1.60",1.6,CARD,5403564732653579\
    \n"17/07/2022 09:02",Belfast,"Leonard Bishop","Regular - Filter coffee - 1.50, Large - Flavoured iced latte - Caramel - 3.25, Large - Glass of milk - 1.10",5.85,CARD,8774971145767913\n'
    s3_client.create_bucket(Bucket=test_bucket)
    s3_client.put_object(Body=test_data, Bucket=test_bucket, Key=f'example/s3/path/key/test_data.csv')
    
    m_clean = Mock(side_effect=['Test'])
    m_etl = Mock()
    m_t_and_b = Mock()

    expected = {
            "headers": {"Content-Type": "application/json"},
            "statusCode": 200,
            "body": json.dumps({"message": "successful upload", "event": test_s3_event}),
        }



    response = ap.lambda_handler(event=test_s3_event,
        context={},
        s3=s3_client,
        clean_the_data=m_clean,
        etl=m_etl,
        t_and_b=m_t_and_b
    )

    assert response == expected