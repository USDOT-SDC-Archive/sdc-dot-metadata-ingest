import os

from aws_requests_auth.aws_auth import AWSRequestsAuth
from elasticsearch import Elasticsearch, RequestsHttpConnection

from common.constants import Constants
from common.logger_utility import LoggerUtility


class ElasticsearchClient:

    @staticmethod
    def get_client(elasticsearch_endpoint):
        LoggerUtility.set_level()
        try:
            awsauth = AWSRequestsAuth(
                aws_access_key=os.environ[Constants.ACCESS_KEY_ENV_VAR],
                aws_secret_access_key=os.environ[Constants.SECRET_KEY_ENV_VAR],
                aws_token=os.environ[Constants.SESSION_TOKEN_ENV_VAR],
                aws_host=elasticsearch_endpoint,
                aws_region=os.environ[Constants.REGION_ENV_VAR],
                aws_service=Constants.ELASTICSEARCH_SERVICE_CLIENT
            )
        except KeyError as e:
            LoggerUtility.log_error(str(e) + " not configured")
            LoggerUtility.log_error("Failed to register kibana dashboard")
            raise e

        return Elasticsearch(
            hosts=['{0}:443'.format(elasticsearch_endpoint)],
            use_ssl=True,
            connection_class=RequestsHttpConnection,
            http_auth=awsauth
        )
