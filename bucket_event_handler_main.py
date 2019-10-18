from bucket_handler_lambda.bucket_event_lambda_handler import *


def lambda_handler(event, *args, **kwargs):
    handle_bucket_event = HandleBucketEvent()
    handle_bucket_event.handle_bucket_event(event)

