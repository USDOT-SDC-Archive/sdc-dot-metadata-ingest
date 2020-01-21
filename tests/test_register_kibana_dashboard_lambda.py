import os

import pytest

from dashboard_registry_lambda.register_kibana_dashboard import RegisterKibanaDashboard

os.environ["SUBMISSIONS_BUCKET_NAME"] = "submissions_bucket_name"
os.environ["CURATED_BUCKET_NAME"] = "curated_bucket_name"
os.environ["PUBLISHED_BUCKET_NAME"] = "published_bucket_name"


def test_create_metadata_visualizations():
    class MockEsClient:
        def index(self, *args, **kwargs):
            pass

    register_kibana_dashboard = RegisterKibanaDashboard()

    register_kibana_dashboard._create_metadata_visualizations(MockEsClient())


def test_register_kibana_dashboard_key_error():
    register_kibana_dashboard = RegisterKibanaDashboard()

    if os.environ.get("ELASTICSEARCH_ENDPOINT", None):
        os.environ.pop("ELASTICSEARCH_ENDPOINT")

    with pytest.raises(KeyError):
        register_kibana_dashboard.register_kibana_dashboard()


def test_register_kibana_dashboard():
    def mock_create_metadata_visualizations(*args, **kwargs):
        pass

    register_kibana_dashboard = RegisterKibanaDashboard()
    os.environ["ELASTICSEARCH_ENDPOINT"] = "elasticsearch_endpoint"
    register_kibana_dashboard._create_metadata_visualizations = mock_create_metadata_visualizations

    register_kibana_dashboard.register_kibana_dashboard()
