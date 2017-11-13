import os
import boto3
import json
import urllib.parse
from elasticsearch import ElasticsearchException
from botocore.exceptions import ClientError
from common.elasticsearch_client import *
from common.constants import *
from common.logger_utility import *


class HandleBucketEvent():

    def __fetchS3DetailsFromEvent(self, event):
        try:
            sns_message = json.loads(event["Records"][0]["Sns"]["Message"])
            bucket = sns_message["Records"][0]["s3"]["bucket"]["name"]
            key = urllib.parse.unquote_plus(sns_message["Records"][0]["s3"]["object"]["key"])
        except Exception as e:
            LoggerUtility.logError(str(e))
            LoggerUtility.logError("Failed to process the event")
            raise e
        else:
            LoggerUtility.logInfo("Bucket name: " + bucket)
            LoggerUtility.logInfo("Object key: " + key)
            return bucket, key

    def __getS3HeadObject(self, bucket_name, object_key):
        s3_client = boto3.client(Constants.S3_SERVICE_CLIENT)
        try:
            response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
        except ClientError as e:
            LoggerUtility.logError(e)
            LoggerUtility.logError('Error getting object {} from bucket {}. Make sure they exist, your bucket is in the same region as this function and necessary permissions have been granted.'.format(object_key, bucket_name))
            raise e
        else:
            return response

    def __createMetadataObject(self, s3_head_object, key):
        metadata = {
            Constants.KEY_REFERENCE: key,
            Constants.CONTENT_LENGTH_REFERENCE: s3_head_object[Constants.CONTENT_LENGTH_REFERENCE],
            Constants.SIZE_MIB_REFERENCE: s3_head_object[Constants.CONTENT_LENGTH_REFERENCE] / 1024**2,
            Constants.LAST_MODIFIED_REFERENCE: s3_head_object[Constants.LAST_MODIFIED_REFERENCE].isoformat(),
            Constants.CONTENT_TYPE_REFERENCE: s3_head_object[Constants.CONTENT_TYPE_REFERENCE],
            Constants.ETAG_REFERENCE: s3_head_object[Constants.ETAG_REFERENCE],
            Constants.DATASET_REFERENCE: key.split('/')[0],
            Constants.STATE_REFERENCE: key.split('/state=')[1].split('/')[0]
        }

        if 'type' in key:
            typeValue = key.split('/type=')[1].split('/')[0]
            typeMetadata = {
                Constants.TRAFFIC_TYPE_REFERENCE: typeValue
            }
            metadata.update(typeMetadata)
        elif 'table' in key:
            tableValue = key.split('/table=')[1].split('/')[0]
            tableMetadata = {
                Constants.TABLE_NAME_REFERENCE: tableValue
            }
            metadata.update(tableMetadata)

        LoggerUtility.logInfo("METADATA: "+str(metadata))
        return metadata

    def __pushMetadataToElasticsearch(self, bucket_name, metadata):
        try:
            elasticsearch_endpoint = os.environ[Constants.ES_ENDPOINT_ENV_VAR]
        except KeyError as e:
            LoggerUtility.logError(str(e) + " not configured")
            raise e
        es_client = ElasticsearchClient.getClient(elasticsearch_endpoint)
        try:
            es_client.index(index=Constants.DEFAULT_INDEX_ID, doc_type=bucket_name, body=json.dumps(metadata))
        except ElasticsearchException as e:
            LoggerUtility.logError(e)
            LoggerUtility.logError("Could not index in Elasticsearch")
            raise e

    def handleBucketEvent(self, event, context):
        LoggerUtility.setLevel()
        bucket_name, object_key = self.__fetchS3DetailsFromEvent(event)
        s3_head_object = self.__getS3HeadObject(bucket_name, object_key)
        metadata = self.__createMetadataObject(s3_head_object, object_key)
        self.__pushMetadataToElasticsearch(bucket_name, metadata)
