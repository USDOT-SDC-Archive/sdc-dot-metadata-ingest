import json
import os
import urllib.parse

import boto3
from botocore.exceptions import ClientError
from elasticsearch import ElasticsearchException

from common.constants import Constants
from common.elasticsearch_client import ElasticsearchClient
from common.logger_utility import LoggerUtility


class HandleBucketEvent:

    def fetch_s3_details_from_event(self, event):
        """
        Pull bucket name and key from an event.
        :param event: Json object
        :return: bucket, key
        """
        try:
            sns_message = json.loads(event["Records"][0]["Sns"]["Message"])
            bucket = sns_message["Records"][0]["s3"]["bucket"]["name"]
            key = urllib.parse.unquote_plus(sns_message["Records"][0]["s3"]["object"]["key"])
        except Exception as e:
            LoggerUtility.log_error(str(e))
            LoggerUtility.log_error("Failed to process the event")
            raise e
        else:
            LoggerUtility.log_info("Bucket name: " + bucket)
            LoggerUtility.log_info("Object key: " + key)
            return bucket, key

    def get_s3_head_object(self, bucket_name, object_key):
        """

        :param bucket_name:
        :param object_key:
        :return:
        """
        s3_client = boto3.client('s3', region_name='us-east-1')
        try:
            response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
        except ClientError as e:
            LoggerUtility.log_error(e)
            LoggerUtility.log_error('Error getting object {} from bucket {}. Make sure they exist, '
                                   'your bucket is in the same region as this function and necessary permissions '
                                   'have been granted.'.format(object_key, bucket_name))
            raise e
        else:
            return response

    def create_metadata_object(self, s3_head_object, key):
        metadata = {
            Constants.KEY_REFERENCE: key,
            Constants.CONTENT_LENGTH_REFERENCE: s3_head_object[Constants.CONTENT_LENGTH_REFERENCE],
            Constants.SIZE_MIB_REFERENCE: s3_head_object[Constants.CONTENT_LENGTH_REFERENCE] / 1024 ** 2,
            Constants.LAST_MODIFIED_REFERENCE: s3_head_object[Constants.LAST_MODIFIED_REFERENCE].isoformat(),
            Constants.CONTENT_TYPE_REFERENCE: s3_head_object[Constants.CONTENT_TYPE_REFERENCE],
            Constants.ETAG_REFERENCE: s3_head_object[Constants.ETAG_REFERENCE],
            Constants.DATASET_REFERENCE: key.split('/')[0],
            Constants.ENVIRONMENT_NAME: os.environ["ENVIRONMENT_NAME"]
        }

        if key.split('/')[0] == "waze":
            if 'type' in key:
                type_value = key.split('/type=')[1].split('/')[0]
                type_metadata = {
                    Constants.TRAFFIC_TYPE_REFERENCE: type_value
                }
                metadata.update(type_metadata)

            if 'table' in key:
                table_value = key.split('/table=')[1].split('/')[0]
                table_metadata = {
                    Constants.TABLE_NAME_REFERENCE: table_value
                }
                metadata.update(table_metadata)

            if 'state' in key:
                state_value = key.split('/state=')[1].split('/')[0]
                state_metadata = {
                    Constants.STATE_REFERENCE: state_value
                }
                metadata.update(state_metadata)
        elif key.split('/')[0] == "cv":
            data_provider_type_value = key.split('/')[1]
            data_provider_type_metadata = {
                Constants.DATA_PROVIDER_REFERENCE: data_provider_type_value
            }
            metadata.update(data_provider_type_metadata)

            data_type_value = key.split('/')[2]
            data_type_metadata = {
                Constants.DATA_TYPE_REFERENCE: data_type_value
            }
            metadata.update(data_type_metadata)

        LoggerUtility.log_info("METADATA: " + str(metadata))
        return metadata

    def push_metadata_to_elasticsearch(self, bucket_name, metadata):
        try:
            elasticsearch_endpoint = os.environ[Constants.ES_ENDPOINT_ENV_VAR]
        except KeyError as e:
            LoggerUtility.log_error(str(e) + " not configured")
            raise e
        es_client = ElasticsearchClient.get_client(elasticsearch_endpoint)
        try:
            es_client.index(index=Constants.DEFAULT_INDEX_ID, doc_type=bucket_name, body=json.dumps(metadata))
        except ElasticsearchException as e:
            LoggerUtility.log_error(e)
            LoggerUtility.log_error("Could not index in Elasticsearch")
            raise e

    def publish_custom_metrics_to_cloudwatch(self, bucket_name, metadata):
        cloudwatch_client = boto3.client('cloudwatch', region_name='us-east-1')
        try:
            if bucket_name == os.environ["SUBMISSIONS_BUCKET_NAME"] and metadata["Dataset"] == "waze":
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
                if metadata["ContentLength"] <= 166:
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
            elif bucket_name == os.environ["SUBMISSIONS_BUCKET_NAME"] and metadata["Dataset"] == "cv":
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
            elif bucket_name == os.environ["CURATED_BUCKET_NAME"] and metadata["Dataset"] == "waze":
                cloudwatch_client.put_metric_data(
                    Namespace=os.environ["WAZE_CURATED_COUNTS_METRIC"],
                    MetricData=[
                        {
                            'MetricName': 'Counts by state and table name',
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
        except Exception as e:
            LoggerUtility.log_error(e)
            LoggerUtility.log_error("Failed to publish custom cloudwatch metrics")
            raise e

    def handle_bucket_event(self, event):
        LoggerUtility.set_level()
        bucket_name, object_key = self.fetch_s3_details_from_event(event)
        s3_head_object = self.get_s3_head_object(bucket_name, object_key)
        metadata = self.create_metadata_object(s3_head_object, object_key)
        self.push_metadata_to_elasticsearch(bucket_name, metadata)
        self.publish_custom_metrics_to_cloudwatch(bucket_name, metadata)
