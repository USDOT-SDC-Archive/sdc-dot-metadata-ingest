from dashboard_registry_lambda.register_kibana_dashboard import *


def lambda_handler(event, context):
    register_kibana_dashboard = RegisterKibanaDashboard()
    register_kibana_dashboard.register_kibana_dashboard()
