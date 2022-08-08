import sys
sys.path.append("../")
sys.path.append("../src/scripts")
from src.scripts import extraction_lambda as el
import botocore.exceptions as be
import boto3
import pytest
from moto import mock_s3

test_s3_event = {
    "Records": [{
        "s3": {
            'bucket': {'name': 'test_bucket'},
            'object': {'key': 'example/s3/path/key/test_data.csv'}
        }
    }]}

@mock_s3
def test_lambda_handler_extraction():
    s3_client = boto3.client('s3')
    test_bucket = 'test_bucket'
    test_data = b'"17/07/2022 09:00",Belfast,"Harold Moore","Large Speciality Tea - Green - 1.60",1.6,CARD,5403564732653579\
    \n"17/07/2022 09:02",Belfast,"Leonard Bishop","Regular Filter coffee - 1.50, Large Flavoured iced latte - Caramel - 3.25, Large Glass of milk - 1.10",5.85,CARD,8774971145767913\n'
    s3_client.create_bucket(Bucket=test_bucket)
    s3_client.create_bucket(Bucket="team-yogurt-cleaned-data")
    s3_client.put_object(Body=test_data, Bucket=test_bucket, Key=f'example/s3/path/key/test_data.csv')
    
    response = el.lambda_handler(event=test_s3_event,context={},s3=s3_client)

    if s3_client.get_object(Bucket="team-yogurt-cleaned-data",Key='example/s3cleaned_/path/key/test_data.csv'):
        print('file found ?')
        file_found = True

    assert response
    assert file_found

@mock_s3
def test_lambda_handler_extraction():
    s3_client = boto3.client('s3')
    test_bucket = 'test_bucket'
    test_data = b'"17/07/2022 09:00",Belfast,"Harold Moore","Large Speciality Tea - Green - 1.60",1.6,CARD,5403564732653579\
    \n"17/07/2022 09:02",Belfast,"Leonard Bishop","Regular Filter coffee - 1.50, Large Flavoured iced latte - Caramel - 3.25, Large Glass of milk - 1.10",5.85,CARD,8774971145767913\n'
    s3_client.create_bucket(Bucket=test_bucket)
    s3_client.create_bucket(Bucket="team-yogurt-cleaned-data")
    s3_client.put_object(Body=test_data, Bucket=test_bucket, Key=f'example/s3/path/key/test_data.csv')
    
    response = el.lambda_handler(event=test_s3_event,context={},s3=s3_client)

    if s3_client.get_object(Bucket="team-yogurt-cleaned-data",Key='example/s3cleaned_/path/key/test_data.csv'):
        print('file found ?')
        file_found = True

    assert response
    assert file_found

@mock_s3
def test_lambda_handler_extraction_bad_file():
    s3_client = boto3.client('s3')
    test_bucket = 'test_bucket'
    test_data = b'"17/07/2022 09:00",Belfast,"Harold Moore","Large Speciality Tea - Green - 1.60",1.6,CARD,5403564732653579\
    \n"17/07/2022 09:02",Belfast,"Leonard Bishop","Regular Filter coffee - 1.50, Large Flavoured iced latte - Caramel - 3.25, Large Glass of milk - 1.10",5.85,CARD,8774971145767913\n'
    s3_client.create_bucket(Bucket=test_bucket)
    s3_client.create_bucket(Bucket="team-yogurt-cleaned-data")
    s3_client.put_object(Body=test_data, Bucket=test_bucket, Key=f'example/s3/path/key/teest_data.csv')
    
    with pytest.raises(be.ClientError):
        response = el.lambda_handler(event=test_s3_event,context={},s3=s3_client)
    
@mock_s3
def test_lambda_handler_extraction_bad_bucket():
    s3_client = boto3.client('s3')
    test_bucket = 'test_buccket'
    test_data = b'"17/07/2022 09:00",Belfast,"Harold Moore","Large Speciality Tea - Green - 1.60",1.6,CARD,5403564732653579\
    \n"17/07/2022 09:02",Belfast,"Leonard Bishop","Regular Filter coffee - 1.50, Large Flavoured iced latte - Caramel - 3.25, Large Glass of milk - 1.10",5.85,CARD,8774971145767913\n'
    s3_client.create_bucket(Bucket=test_bucket)
    s3_client.create_bucket(Bucket="team-yogurt-cleaned-data")
    s3_client.put_object(Body=test_data, Bucket=test_bucket, Key=f'example/s3/path/key/test_data.csv')
    
    with pytest.raises(be.ClientError):
        response = el.lambda_handler(event=test_s3_event,context={},s3=s3_client)

    