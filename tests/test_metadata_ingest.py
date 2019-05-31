from moto import mock_ssm, mock_sqs, mock_events, mock_s3
import sys
import os
import pytest
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from bucket_handler_lambda.bucket_event_lambda_handler import *


@mock_s3
def test_fetch_s3_details_from_event():
    file_name = "tests/data/event_input.json"
    bucket_name = "dev-dot-sdc-raw-submissions-911061262852-us-east-1"
    s3_key = "waze/state=LA/type=alert/year=2019/month=05/day=30/hour=16/minute=42/abe30a9f-2295-4f9c-96e2-333a6e68eb4f.json"
    data = open(file_name, 'r').read()
    data_obj = json.loads(data)
    metadata_obj = HandleBucketEvent()
    bucket, key = metadata_obj.fetchS3DetailsFromEvent(data_obj)
    assert bucket == bucket_name
    assert key == s3_key


@mock_s3
def test_fetch_s3_details_from_event_exception():
    with pytest.raises(Exception):
        file_name = "tests/data/event_input.json"
        bucket_name = "dev-dot-sdc-raw-submissions-911061262852-us-east-1"
        s3_key = "waze/state=LA/type=alert/year=2019/month=05/day=30/hour=16/minute=42/abe30a9f-2295-4f9c-96e2-333a6e68eb4f.json"
        data = open(file_name, 'r').read()
        metadata_obj = HandleBucketEvent()
        bucket, key = metadata_obj.fetchS3DetailsFromEvent(data)

@mock_s3
def test_get_s3_head_object():
    file_name = "tests/data/event_input.json"
    bucket_name = "dev-dot-sdc-raw-submissions-911061262852-us-east-1"
    s3_key = "waze/state=LA/type=alert/year=2019/month=05/day=30/hour=16/minute=42/abe30a9f-2295-4f9c-96e2-333a6e68eb4f.json"
    s3_client = boto3.client('s3', region_name='us-east-1')
    s3_client.create_bucket(Bucket=bucket_name)
    s3_client.put_object(Bucket=bucket_name, Body=file_name, Key=s3_key)
    metadata_obj = HandleBucketEvent()
    response = metadata_obj.getS3HeadObject(bucket_name, s3_key)
    assert True


@mock_s3
def test_get_s3_head_object_exception():
    with pytest.raises(Exception):
        file_name = "tests/data/event_input.json"
        bucket_name = "dev-dot-sdc-raw-submissions-911061262852-us-east-1"
        s3_key = "waze/state=LA/type=alert/year=2019/month=05/day=30/hour=16/minute=42/abe30a9f-2295-4f9c-96e2-333a6e68eb4f.json"
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket=bucket_name)
        metadata_obj = HandleBucketEvent()
        metadata_obj.getS3HeadObject(bucket_name, s3_key)


# @mock_events
# def test_create_metadata_object_waze():
#     input_file_name = "tests/data/head_object_input.json"
#     metadata_output_file_name = "tests/data/metadata_output.json"
#     s3_key = "waze/state=AK/type=alert/year=2018/month=01/day=02/hour=00/minute=00/0f039a78-26d8-46c4-9017-155906241701.json"
#     data = open(input_file_name, 'r').read()
#     head_object = json.loads(data)
#     print(head_object)
#
#     metadata = open(metadata_output_file_name, 'r').read()
#     metadata_output = json.loads(metadata)
#
#     metadata_obj = HandleBucketEvent()
#     metadata_response = metadata_obj.createMetadataObject(head_object, s3_key)
#     assert metadata_response == metadata_output





