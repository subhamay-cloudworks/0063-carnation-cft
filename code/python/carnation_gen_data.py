import json
import uuid
import logging
import time
import random
import boto3
import os

# Load the exceptions for error handling
from botocore.exceptions import ClientError, ParamValidationError
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

dynamodb_client = boto3.client(
    'dynamodb', region_name=os.environ.get("AWS_REGION"))
sqs_client = boto3.client(
    'sqs', region_name=os.environ.get("AWS_REGION"))

dynamodb_table = os.getenv("DYNAMODB_TABLE_NAME")
dynamodb_table_dummy = os.getenv("DYNAMODB_TABLE_NAME_1")
sqs_queue_url = os.getenv("SQS_QUEUE_URL")
dead_letter_queue_url = os.getenv("DEAD_LETTER_QUEUE_URL")

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def receive_sqs_messages():
    try:

        response = sqs_client.receive_message(
            QueueUrl=sqs_queue_url,
            MaxNumberOfMessages=10
        )

        if "Messages" in response:
            return json.loads(response["Messages"][0]["Body"])
        else:
            return []

    # An error occurred
    except ParamValidationError as e:
        logger.error(f"Parameter validation error: {e}")
        return None
    except ClientError as e:
        logger.error(f"Client error: {e}")
        return None


def handler(event, context):

    logger.info(f"event :: {json.dumps(event)}")
    start_seq = 0 if int(event["start_seq"]) < 0 else int(event["start_seq"])
    end_seq = int(event["end_seq"])

    '''
    Pull the existing messages from the SQS Queue
    '''
    sqs_messages = receive_sqs_messages()

    random_strings = []
    for message in sqs_messages:
        random_string_dict = dict(
                sequence_no=message.get("sequence_no"),
                random_value=message.get("random_value"),
                current_time=message.get("current_time"),
                retry_attempt=message.get("retry_attempt"),
                start_seq=message.get("start_seq"),
                end_seq=message.get("end_seq")
            )
        if int(message.get("retry_attempt")) < 3:
            random_strings.append(random_string_dict)
        else:
            # Push the message to the Dead Letter Queue
            message_attributes=dict(Comment=dict(DataType="String",StringValue="This message failed to get inserted after 3 retry attempts on consecutive days"))
            response = sqs_client.send_message(QueueUrl=dead_letter_queue_url,
                                               DelaySeconds=10,
                                               MessageAttributes=message_attributes,
                                               MessageBody=json.dumps(random_string_dict) 
                                               )
            logger.info(f"DLQ response : {json.dumps(response)}")

    if start_seq < end_seq:
        for i in range(start_seq, end_seq):
            random_string_dict = dict(
                sequence_no=1 if i < 1 else i, 
                random_value=str(uuid.uuid4()),
                current_time=int(time.time()),
                start_seq=start_seq,
                end_seq=end_seq,
                retry_attempt=0
            )
            # time.sleep(random.randint(0, 1))
            random_strings.append(random_string_dict)

    logger.info(f"random_strings :: {json.dumps(random_strings)}")
    return random_strings
