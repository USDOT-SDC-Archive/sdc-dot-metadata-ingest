from moto import mock_cloudwatch, mock_events, mock_s3
import sys
import os
import datetime
from dateutil.tz import tzutc
import pytest
from mock import Mock
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


@mock_events
def test_create_metadata_object_waze_raw():
    input_file_name = "tests/data/head_object_input_raw.json"
    metadata_output_file_name = "tests/data/metadata_output_raw.json"
    s3_key = "waze/state=ME/type=jam/year=2019/month=06/day=28/hour=07/minute=54/219ee0da-89c7-40c8-bb3a-1a4175be608f.json"
    data = open(input_file_name, 'r').read()
    head_object = json.loads(data)
    head_object["LastModified"] = datetime.datetime(2019, 6, 28, 7, 55, 52, tzinfo=tzutc())
    metadata = open(metadata_output_file_name, 'r').read()
    metadata_output = json.loads(metadata)
    os.environ["ENVIRONMENT_NAME"] = "dev"
    metadata_obj = HandleBucketEvent()
    metadata_response = metadata_obj.createMetadataObject(head_object, s3_key)
    assert metadata_response == metadata_output


@mock_events
def test_create_metadata_object_waze_curated():
    input_file_name = "tests/data/head_object_input_curated.json"
    metadata_output_file_name = "tests/data/metadata_output_curated.json"
    s3_key = "waze/version=20180720/content/state=IA/table=alert/projection=redshift/year=2019/month=06/day=28/hour=07/minute=55/79a3a32d-73a3-4152-adf7-495929d50c3e.csv.gz"
    data = open(input_file_name, 'r').read()
    head_object = json.loads(data)
    head_object["LastModified"] = datetime.datetime(2019, 6, 28, 7, 55, 50, tzinfo=tzutc())
    metadata = open(metadata_output_file_name, 'r').read()
    metadata_output = json.loads(metadata)
    os.environ["ENVIRONMENT_NAME"] = "dev"
    metadata_obj = HandleBucketEvent()
    metadata_response = metadata_obj.createMetadataObject(head_object, s3_key)
    assert metadata_response == metadata_output


@mock_events
def test_create_metadata_object_cv():
    input_file_name = "tests/data/head_object_input_cv.json"
    metadata_output_file_name = "tests/data/metadata_output_cv.json"
    s3_key = "cv/thea/Bluetooth/2019/06/28/tmp-Bluetooth_RAW_11_26_18.csv_0"
    data = open(input_file_name, 'r').read()
    head_object = json.loads(data)
    head_object["LastModified"] = datetime.datetime(2019, 6, 28, 5, 00, 52, tzinfo=tzutc())
    metadata = open(metadata_output_file_name, 'r').read()
    metadata_output = json.loads(metadata)
    os.environ["ENVIRONMENT_NAME"] = "dev"
    metadata_obj = HandleBucketEvent()
    metadata_response = metadata_obj.createMetadataObject(head_object, s3_key)
    assert metadata_response == metadata_output


@mock_events
def test_push_metadata_to_elasticsearch_endpoint_exception():
    with pytest.raises(Exception):
        metadata_file_name = "tests/data/metadata_output_raw.json"
        bucket_name = "dev-dot-sdc-raw-submissions-911061262852-us-east-1"
        data = open(metadata_file_name, 'r').read()
        metadata = json.loads(data)
        elasticsearch_client = Mock()
        metadata_obj = HandleBucketEvent()
        metadata_obj.es_client = elasticsearch_client
        metadata_obj.pushMetadataToElasticsearch(bucket_name, metadata)


@mock_events
def test_push_metadata_to_elasticsearch_exception():
    with pytest.raises(Exception):
        metadata_file_name = "tests/data/metadata_output_raw.json"
        bucket_name = "dev-dot-sdc-raw-submissions-911061262852-us-east-1"
        data = open(metadata_file_name, 'r').read()
        metadata = json.loads(data)
        os.environ["ELASTICSEARCH_ENDPOINT"] = "search-dev-dot-sdc-datalake-es-w7hstpzlwyebvezqnlrbzie4su.us-east-1.es.amazonaws.com"
        elasticsearch_client = Mock()
        metadata_obj = HandleBucketEvent()
        metadata_obj.es_client = elasticsearch_client
        metadata_obj.pushMetadataToElasticsearch(bucket_name, metadata)


@mock_events
def test_push_metadata_to_elasticsearch_index_exception():
    with pytest.raises(Exception):
        metadata_file_name = "tests/data/metadata_output_raw.json"
        bucket_name = "dev-dot-sdc-raw-submissions-911061262852-us-east-1"
        data = open(metadata_file_name, 'r').read()
        metadata = json.loads(data)
        os.environ["AWS_ACCESS_KEY_ID"] = "thisisaccesskey"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "thisissecretkey"
        os.environ["AWS_SESSION_TOKEN"] = "thisissessiontokenvariable"
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ["ELASTICSEARCH_ENDPOINT"] = "search-dev-dot-sdc-datalake-es-w7hstpzlwyebvezqnlrbzie4su.us-east-1.es.amazonaws.com"
        elasticsearch_client = Mock()
        metadata_obj = HandleBucketEvent()
        metadata_obj.es_client = elasticsearch_client
        metadata_obj.pushMetadataToElasticsearch(bucket_name, metadata)


@mock_cloudwatch
def test_push_metrics_to_cloudwatch_exception():
    with pytest.raises(Exception):
        metadata_file_name = "tests/data/metadata_output_curated.json"
        bucket_name = "dev-dot-sdc-curated-911061262852-us-east-1"
        data = open(metadata_file_name, 'r').read()
        metadata = json.loads(data)
        metadata_obj = HandleBucketEvent()
        metadata_obj.publishCustomMetricsToCloudwatch(bucket_name, metadata)
        assert True


@mock_cloudwatch
def test_push_metrics_to_cloudwatch_waze_raw():
    metadata_file_name = "tests/data/metadata_output_raw.json"
    bucket_name = "dev-dot-sdc-raw-submissions-911061262852-us-east-1"
    os.environ["SUBMISSIONS_BUCKET_NAME"] = "dev-dot-sdc-raw-submissions-911061262852-us-east-1"
    os.environ["WAZE_SUBMISSIONS_COUNT_METRIC"] = "dev-dot-sdc-waze-submissions-bucket-metric"
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
    metadata_obj.publishCustomMetricsToCloudwatch(bucket_name, metadata)
    assert True


@mock_cloudwatch
def test_push_metrics_to_cloudwatch_cv():
    metadata_file_name = "tests/data/metadata_output_cv.json"
    bucket_name = "dev-dot-sdc-raw-submissions-911061262852-us-east-1"
    os.environ["SUBMISSIONS_BUCKET_NAME"] = "dev-dot-sdc-raw-submissions-911061262852-us-east-1"
    os.environ["CV_SUBMISSIONS_COUNTS_METRIC"] = "dev-dot-sdc-cv-submissions-bucket-metric"
    data = open(metadata_file_name, 'r').read()
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
    metadata_obj.publishCustomMetricsToCloudwatch(bucket_name, metadata)
    assert True


@mock_cloudwatch
def test_push_metrics_to_cloudwatch_zero_byte_submissions():
    metadata_file_name = "tests/data/metadata_output_zero_byte.json"
    bucket_name = "dev-dot-sdc-raw-submissions-911061262852-us-east-1"
    os.environ["SUBMISSIONS_BUCKET_NAME"] = "dev-dot-sdc-raw-submissions-911061262852-us-east-1"
    os.environ["WAZE_ZERO_BYTE_SUBMISSIONS_COUNT_METRIC"] = "dev-dot-sdc-cv-submissions-bucket-metric"
    data = open(metadata_file_name, 'r').read()
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
    metadata_obj.publishCustomMetricsToCloudwatch(bucket_name, metadata)
    assert True


@mock_cloudwatch
def test_push_metrics_to_cloudwatch_waze_curated():
    metadata_file_name = "tests/data/metadata_output_curated.json"
    bucket_name = "dev-dot-sdc-curated-911061262852-us-east-1"
    os.environ["CURATED_BUCKET_NAME"] = "dev-dot-sdc-curated-911061262852-us-east-1"
    os.environ["WAZE_CURATED_COUNTS_METRIC"] = "dev-dot-sdc-waze-curated-bucket-metric"
    data = open(metadata_file_name, 'r').read()
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
    metadata_obj.publishCustomMetricsToCloudwatch(bucket_name, metadata)
    assert True


@mock_events
def test_handle_bucket_event():
    with pytest.raises(Exception):
        assert HandleBucketEvent().handleBucketEvent(None, None) is None












