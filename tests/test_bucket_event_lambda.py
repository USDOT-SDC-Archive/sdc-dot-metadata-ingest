import datetime
import json
import os

import boto3
import pytest
from dateutil.tz import tzutc
from mock import Mock
from moto import mock_cloudwatch, mock_events, mock_s3
from bucket_handler_lambda.bucket_event_lambda_handler import HandleBucketEvent

os.environ["SUBMISSIONS_BUCKET_NAME"] = "SUBMISSIONS_BUCKET_NAME"
os.environ["WAZE_SUBMISSIONS_COUNT_METRIC"] = "WAZE_SUBMISSIONS_COUNT_METRIC"
os.environ["ELASTICSEARCH_ENDPOINT"] = "ELASTICSEARCH_ENDPOINT"
os.environ["ENVIRONMENT_NAME"] = "dev"
os.environ["CV_SUBMISSIONS_COUNTS_METRIC"] = "CV_SUBMISSIONS_COUNTS_METRIC"
os.environ["WAZE_ZERO_BYTE_SUBMISSIONS_COUNT_METRIC"] = "WAZE_ZERO_BYTE_SUBMISSIONS_COUNT_METRIC"
os.environ["CURATED_BUCKET_NAME"] = "CURATED_BUCKET_NAME"
os.environ["WAZE_CURATED_COUNTS_METRIC"] = "WAZE_CURATED_BUCKET_METRIC"

# aws login configuration
os.environ["AWS_ACCESS_KEY_ID"] = "thisisaccesskey"
os.environ["AWS_SECRET_ACCESS_KEY"] = "thisissecretkey"
os.environ["AWS_SESSION_TOKEN"] = "thisissessiontokenvariable"
os.environ["AWS_REGION"] = "us-east-1"


event_input_file_name = "tests/data/event_input.json"

# key built from the tests/data/event_input.json data
s3_key_event_input = "waze/state=LA/type=alert/year=2010/month=05/day=30/hour=16/minute=42/blahblah3.json"
metadata_file_name = "tests/data/metadata_output_raw.json"

@mock_s3
def test_fetch_s3_details_from_event():
    bucket_name = "bucket_name"  # "bucket_name"
    data = open(event_input_file_name, 'r').read()
    data_obj = json.loads(data)
    metadata_obj = HandleBucketEvent()
    bucket, key = metadata_obj.fetch_s3_details_from_event(data_obj)
    assert bucket == bucket_name
    assert key == s3_key_event_input


@mock_s3
def test_fetch_s3_details_from_event_exception():
    with pytest.raises(Exception):
        data = open(event_input_file_name , 'r').read()
        metadata_obj = HandleBucketEvent()
        metadata_obj.fetch_s3_details_from_event(data)


@mock_s3
def test_get_s3_head_object():
    bucket_name = "bucket_name"
    s3_client = boto3.client('s3', region_name='us-east-1')
    s3_client.create_bucket(Bucket=bucket_name)
    s3_client.put_object(Bucket=bucket_name, Body=event_input_file_name, Key=s3_key_event_input)
    metadata_obj = HandleBucketEvent()
    metadata_obj.get_s3_head_object(bucket_name, s3_key_event_input)


@mock_s3
def test_get_s3_head_object_exception():
    with pytest.raises(Exception):
        bucket_name = "bucket_name"
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket=bucket_name)
        metadata_obj = HandleBucketEvent()
        metadata_obj.get_s3_head_object(bucket_name, s3_key_event_input)


@mock_events
def test_create_metadata_object_waze_raw():
    input_file_name = "tests/data/head_object_input_raw.json"
    s3_key = "waze/state=ME/type=jam/year=2010/month=06/day=28/hour=07/minute=54/foo5.json"
    data = open(input_file_name, 'r').read()
    head_object = json.loads(data)
    head_object["LastModified"] = datetime.datetime(2010, 6, 28, 7, 55, 52, tzinfo=tzutc())
    metadata = open(metadata_file_name, 'r').read()
    metadata_output = json.loads(metadata)
    metadata_obj = HandleBucketEvent()
    metadata_response = metadata_obj.create_metadata_object(head_object, s3_key)
    assert metadata_response == metadata_output


@mock_events
def test_create_metadata_object_waze_curated():
    input_file_name = "tests/data/head_object_input_curated.json"
    metadata_output_file_name = "tests/data/metadata_output_curated.json"
    s3_key = "waze/version=20180720/content/state=IA/table=alert/projection=redshift/year=2010/month=06/day=28/hour=07/minute=55/foo4.csv.gz"
    data = open(input_file_name, 'r').read()
    head_object = json.loads(data)
    head_object["LastModified"] = datetime.datetime(2010, 6, 28, 7, 55, 50, tzinfo=tzutc())
    metadata = open(metadata_output_file_name, 'r').read()
    metadata_output = json.loads(metadata)
    metadata_obj = HandleBucketEvent()
    metadata_response = metadata_obj.create_metadata_object(head_object, s3_key)
    assert metadata_response == metadata_output


@mock_events
def test_create_metadata_object_cv():
    input_file_name = "tests/data/head_object_input_cv.json"
    metadata_output_file_name = "tests/data/metadata_output_cv.json"
    s3_key = "cv/thea/Bluetooth/2010/06/28/tmp-Bluetooth_RAW_11_26_18.csv_0"
    data = open(input_file_name, 'r').read()
    head_object = json.loads(data)
    head_object["LastModified"] = datetime.datetime(2010, 6, 28, 5, 00, 52, tzinfo=tzutc())
    metadata = open(metadata_output_file_name, 'r').read()
    metadata_output = json.loads(metadata)
    metadata_obj = HandleBucketEvent()
    metadata_response = metadata_obj.create_metadata_object(head_object, s3_key)
    assert metadata_response == metadata_output


@mock_events
def test_push_metadata_to_elasticsearch_endpoint_exception():
    with pytest.raises(Exception):
        bucket_name = "bucket_name"
        data = open(metadata_file_name, 'r').read()
        metadata = json.loads(data)
        elasticsearch_client = Mock()
        metadata_obj = HandleBucketEvent()
        metadata_obj.es_client = elasticsearch_client
        metadata_obj.push_metadata_to_elasticsearch(bucket_name, metadata)


@mock_events
def test_push_metadata_to_elasticsearch_exception():
    with pytest.raises(Exception):
        bucket_name = "bucket_name"
        data = open(metadata_file_name, 'r').read()
        metadata = json.loads(data)
        elasticsearch_client = Mock()
        metadata_obj = HandleBucketEvent()
        metadata_obj.es_client = elasticsearch_client
        metadata_obj.push_metadata_to_elasticsearch(bucket_name, metadata)


@mock_events
def test_push_metadata_to_elasticsearch_index_exception():
    with pytest.raises(Exception):
        bucket_name = "bucket_name"
        data = open(metadata_file_name, 'r').read()
        metadata = json.loads(data)
        elasticsearch_client = Mock()
        metadata_obj = HandleBucketEvent()
        metadata_obj.es_client = elasticsearch_client
        metadata_obj.push_metadata_to_elasticsearch(bucket_name, metadata)


@mock_cloudwatch
def test_push_metadata_to_elasticsearch():
    elasticsearch_endpoint = None
    if os.environ.get("SUBMISSIONS_BUCKET_NAME", None):
        elasticsearch_endpoint = os.environ.pop("ELASTICSEARCH_ENDPOINT")
    metadata_file_name_2 = "tests/data/metadata_output_curated.json"
    bucket_name = "bucket_name"
    data = open(metadata_file_name_2, 'r').read()
    metadata = json.loads(data)
    metadata_obj = HandleBucketEvent()

    try:
        with pytest.raises(KeyError):
            metadata_obj.push_metadata_to_elasticsearch(bucket_name, metadata)

    finally:
        os.environ["ELASTICSEARCH_ENDPOINT"] = elasticsearch_endpoint


@mock_cloudwatch
def test_push_metrics_to_cloudwatch_waze_raw():
    bucket_name = "bucket_name"
    data = open(metadata_file_name, 'r').read()
    metadata = json.loads(data)
    cloudwatch_client = boto3.client('cloudwatch', region_name='us-east-1')
    cloudwatch_client.put_metric_data(
        Namespace=os.environ["WAZE_SUBMISSIONS_COUNT_METRIC"],
        MetricData=[
            {
                'MetricName': 'Counts by state and traffic type',
                'Dimensions': [
                    {
                        'Name': 'State',
                        'Value': metadata["State"]
                    },
                    {
                        'Name': 'TrafficType',
                        'Value': metadata["TrafficType"]
                    }
                ],
                'Value': 1,
                'Unit': 'Count'
            },
        ]
    )
    metadata_obj = HandleBucketEvent()
    metadata_obj.publish_custom_metrics_to_cloudwatch(bucket_name, metadata)


@mock_cloudwatch
def test_push_metrics_to_cloudwatch_cv():
    metadata_file_name_3 = "tests/data/metadata_output_cv.json"
    bucket_name = "bucket_name"
    data = open(metadata_file_name_3, 'r').read()
    metadata = json.loads(data)
    cloudwatch_client = boto3.client('cloudwatch', region_name='us-east-1')
    cloudwatch_client.put_metric_data(
        Namespace=os.environ["CV_SUBMISSIONS_COUNTS_METRIC"],
        MetricData=[
            {
                'MetricName': 'Counts by provider and datatype',
                'Dimensions': [
                    {
                        'Name': 'DataProvider',
                        'Value': metadata["DataProvider"]
                    },
                    {
                        'Name': 'DataType',
                        'Value': metadata["DataType"]
                    }
                ],
                'Value': 1,
                'Unit': 'Count'
            },
        ]
    )
    metadata_obj = HandleBucketEvent()
    metadata_obj.publish_custom_metrics_to_cloudwatch(bucket_name, metadata)


@mock_cloudwatch
def test_push_metrics_to_cloudwatch_zero_byte_submissions():
    metadata_file_name_4 = "tests/data/metadata_output_zero_byte.json"
    bucket_name = "bucket_name"
    data = open(metadata_file_name_4, 'r').read()
    metadata = json.loads(data)
    cloudwatch_client = boto3.client('cloudwatch', region_name='us-east-1')
    cloudwatch_client.put_metric_data(
        Namespace=os.environ["WAZE_ZERO_BYTE_SUBMISSIONS_COUNT_METRIC"],
        MetricData=[
            {
                'MetricName': 'Zero Byte Submissions by State and traffic type',
                'Dimensions': [
                    {
                        'Name': 'State',
                        'Value': metadata["State"]
                    },
                    {
                        'Name': 'TrafficType',
                        'Value': metadata["TrafficType"]
                    }
                ],
                'Value': 1,
                'Unit': 'Count'
            },
        ]
    )
    metadata_obj = HandleBucketEvent()
    metadata_obj.publish_custom_metrics_to_cloudwatch(bucket_name, metadata)


@mock_cloudwatch
def test_push_metrics_to_cloudwatch_waze_curated():
    metadata_file_name_5 = "tests/data/metadata_output_curated.json"
    bucket_name = "bucket_name"
    data = open(metadata_file_name_5, 'r').read()
    metadata = json.loads(data)
    cloudwatch_client = boto3.client('cloudwatch', region_name='us-east-1')
    cloudwatch_client.put_metric_data(
        Namespace=os.environ["WAZE_CURATED_COUNTS_METRIC"],
        MetricData=[
            {
                'MetricName': 'Counts by state and traffic type',
                'Dimensions': [
                    {
                        'Name': 'State',
                        'Value': metadata["State"]
                    },
                    {
                        'Name': 'TableName',
                        'Value': metadata["TableName"]
                    }
                ],
                'Value': 1,
                'Unit': 'Count'
            },
        ]
    )
    metadata_obj = HandleBucketEvent()
    metadata_obj.publish_custom_metrics_to_cloudwatch(bucket_name, metadata)


@mock_events
def test_handle_bucket_event():
    with pytest.raises(Exception):
        assert HandleBucketEvent().handle_bucket_event(None) is None
