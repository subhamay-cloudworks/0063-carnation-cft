import json
import logging
import boto3
import os
import time
import random

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


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def dynamodb_obj_to_python_obj(dynamodb_obj: dict) -> dict:
    deserializer = TypeDeserializer()
    return {
        k: deserializer.deserialize(v)
        for k, v in dynamodb_obj.items()
    }


def python_obj_to_dynamo_obj(python_obj: dict) -> dict:
    serializer = TypeSerializer()
    return {
        k: serializer.serialize(v)
        for k, v in python_obj.items()
    }


def dynamodb_item_exists(partitionKey, sortKey, table_flag):

    logger.info(f"dynamodb_item_exists :: table_flag = {table_flag}")
    try:
        response = dynamodb_client.get_item(
            TableName=dynamodb_table if table_flag else dynamodb_table_dummy,
            Key={
                'sequenceNo': {'N': partitionKey},
                'generatedUnixTime': {'N': sortKey}
            }
        )

        logger.info(f"response :: {json.dumps(response)}")

        if response:
            if not response.get('Item'):
                logger.info(
                    f"The item {dict(ID=partitionKey)} does not exist.")
                return False
            else:
                dynamodb_item = dynamodb_obj_to_python_obj(response['Item'])
                logger.info(f"The item {dict(ID=partitionKey)} exists.")
                return True
        else:
            return False

    # An error occurred
    except ParamValidationError as e:
        logger.error(f"Parameter validation error: {e}")
        # raise Exception(e)
        return False
    except ClientError as e:
        logger.error(f"Client error: {e}")
        return False
        # raise Exception(e)


def dynamo_db_put_item(item, table_flag):

    try:
        response = dynamodb_client.put_item(
            TableName=dynamodb_table if table_flag else dynamodb_table_dummy,
            Item=item
        )

        return response
    # An error occurred
    except ParamValidationError as e:
        logger.error(f"Parameter validation error: {e}")
    except ClientError as e:
        logger.error(f"Client error: {e}")


def handler(event, context):

    try:
        logger.info(f"event :: {json.dumps(event)}")

        # Check if the specific item exists in the table
        partition_key = str(event.get("sequence_no"))
        sort_key = str(event.get("current_time"))
        start_seq = event.get("start_seq")
        end_seq = event.get("end_seq")

        table_flag=random.choice([True, False])
        item_exists = dynamodb_item_exists(
            partitionKey=partition_key,
            sortKey=sort_key,
            table_flag=table_flag
        )
        logger.info(f"item_exists = {item_exists}")

        if item_exists:
            dynamodb_item = dict(sequenceNo=int(partition_key),
                                 generatedUnixTime=event.get(
                "current_time"),
                createdUnixTime=int(time.time()),
                updatedUnixTime=int(time.time()),
                randomStringValue=event.get(
                "random_value"),
                retryAttempt=event.get("retry_attempt")
            )
        else:
            dynamodb_item = dict(sequenceNo=int(partition_key),
                                 generatedUnixTime=event.get(
                "current_time"),
                createdUnixTime=int(time.time()),
                randomStringValue=event.get(
                "random_value"),
                retryAttempt=event.get("retry_attempt")
            )

        python_obj = python_obj_to_dynamo_obj(dynamodb_item)
        logger.info(f"*****python_obj = {json.dumps(python_obj)}")
        response = dynamo_db_put_item(python_obj, table_flag)
        if response:
            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                return_message = dict(status="Success")
            else:
                return_message = dict(status="Failure", event={key: (value if key != "retry_attempt" else value + 1) for (key,value) in event.items()})
        else:
            return_message = dict(status="Failure", event={key: (value if key != "retry_attempt" else value + 1) for (key,value) in event.items()})

        return return_message

    except ParamValidationError as e:
        logger.error(f"Parameter validation error: {e}")
        raise Exception(e)
    except ClientError as e:
        logger.error(f"Client error: {e}")
        raise Exception(e)
