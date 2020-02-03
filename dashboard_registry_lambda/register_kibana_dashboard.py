import json
import os
import re

from elasticsearch import ElasticsearchException

from common.constants import Constants
from common.elasticsearch_client import ElasticsearchClient
from common.logger_utility import LoggerUtility


class RegisterKibanaDashboard:

    def _create_metadata_visualizations(self, es_client):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        file_path = os.path.join(dir_path, Constants.KIBANA_JSON_FILENAME)
        submission_bucket_name = os.environ["SUBMISSIONS_BUCKET_NAME"]
        curated_dataset_bucket_name = os.environ["CURATED_BUCKET_NAME"]
        published_dataset_bucket_name = os.environ["PUBLISHED_BUCKET_NAME"]
        replace_value = "_type: "
        es_client.index(index=Constants.KIBANA_INDEX_NAME, doc_type=Constants.CONFIG_DOCUMENT_TYPE, id='5.1.1',
                        body=json.dumps({Constants.DEFAULT_INDEX_REFERENCE: Constants.DEFAULT_INDEX_ID}))
        data = {
            Constants.TITLE_REFERENCE: Constants.DEFAULT_INDEX_ID,
            Constants.TIME_FIELD_NAME_REFERENCE: Constants.LAST_MODIFIED_REFERENCE
        }
        es_client.index(index=Constants.KIBANA_INDEX_NAME, doc_type=Constants.INDEX_PATTERN_DOCUMENT_TYPE,
                        id=Constants.DEFAULT_INDEX_ID, body=json.dumps(data))
        with open(file_path) as visualizations_file:
            visualizations = json.load(visualizations_file)
        for visualization in visualizations:
            doc_type = visualization['_type']
            source = json.dumps(visualization['_source'])
            if 'visualization' in doc_type:
                if 'datalake-submissions*' in source:
                    source = re.sub("_type: datalake-submissions[*]", replace_value + submission_bucket_name, source)
                elif 'datalake-curated-datasets*' in source:
                    source = re.sub("_type: datalake-curated-datasets[*]", replace_value + curated_dataset_bucket_name,
                                    source)
                elif 'datalake-published-data*' in source:
                    source = re.sub("_type: datalake-published-data[*]", replace_value + published_dataset_bucket_name,
                                    source)

            es_client.index(
                index=Constants.KIBANA_INDEX_NAME,
                doc_type=doc_type,
                id=visualization['_id'],
                body=source
            )

    def register_kibana_dashboard(self):
        LoggerUtility.set_level()
        try:
            es_endpoint = os.environ[Constants.ES_ENDPOINT_ENV_VAR]
        except KeyError as e:
            LoggerUtility.log_error(str(e) + " not configured")
            LoggerUtility.log_error("Failed to register kibana dashboard")
            raise e

        es_client = ElasticsearchClient.get_client(es_endpoint)
        try:
            self._create_metadata_visualizations(es_client)
        except ElasticsearchException as e:
            LoggerUtility.log_error(e)
            LoggerUtility.log_error("Failed to register kibana dashboard")
            raise e
