from bucket_handler_lambda.bucket_event_lambda_handler import *


def lambda_handler(event, context):
    handle_bucket_event = HandleBucketEvent()
    handle_bucket_event.handleBucketEvent(event, context)
    curation_lambda_function_name = os.environ["CURATION_LAMBDA_FUNCTION_NAME"]
    submissions_bucket_name = os.environ["SUBMISSIONS_BUCKET_NAME"]
    sns_message = json.loads(event["Records"][0]["Sns"]["Message"])
    bucket = sns_message["Records"][0]["s3"]["bucket"]["name"]

    if bucket == submissions_bucket_name:
        client = boto3.client('lambda')
        LoggerUtility.logInfo("Invoking Curation lambda function")
        response = client.invoke(FunctionName=curation_lambda_function_name,
                                 InvocationType='Event',
                                 Payload=json.dumps(event))
        LoggerUtility.logInfo("Response from invoked lambda function - {}".format(response))

