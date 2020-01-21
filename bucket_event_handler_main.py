from bucket_handler_lambda.bucket_event_lambda_handler import HandleBucketEvent


def lambda_handler(event, context):
    handle_bucket_event = HandleBucketEvent()
    handle_bucket_event.handle_bucket_event(event)
