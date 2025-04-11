import json
import boto3
import os
import datetime
import logging

# Initialize AWS services
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Get environment variables
TABLE_NAME = os.environ['TABLE_NAME']
BUCKET_NAME = os.environ['BUCKET_NAME']

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    """
    Process SQS messages containing S3 events and update DynamoDB with bucket size information.
    This function is triggered by SQS events that originate from S3 events via SNS.
    """
    logger.info(f"Received event: {json.dumps(event)}")
    
    # Process each SQS message (which contains S3 events via SNS)
    for record in event['Records']:
        try:
            # Parse the SQS message body (which contains the SNS message)
            body = json.loads(record['body'])
            
            # Parse the SNS message (which contains the actual S3 event)
            if 'Message' in body:
                s3_event = json.loads(body['Message'])
                logger.info(f"Processing S3 event: {json.dumps(s3_event)}")
                
                # Process S3 event records
                if 'Records' in s3_event:
                    for s3_record in s3_event['Records']:
                        if s3_record['eventSource'] == 'aws:s3':
                            # Skip processing if this is not our target bucket
                            if s3_record['s3']['bucket']['name'] != BUCKET_NAME:
                                logger.info(f"Skipping event for bucket {s3_record['s3']['bucket']['name']}")
                                continue
                            
                            # Calculate total bucket size
                            calculate_and_store_bucket_size(BUCKET_NAME)
        except Exception as e:
            logger.error(f"Error processing SQS message: {e}")
            continue

def calculate_and_store_bucket_size(bucket_name):
    """
    Calculate the total size of all objects in the given S3 bucket
    and store the result in DynamoDB.
    """
    try:
        # Get total size of the bucket
        total_size = 0
        object_count = 0
        
        # Paginate through all objects
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name)
        
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    total_size += obj['Size']
                    object_count += 1
        
        # Get current timestamp
        timestamp = datetime.datetime.now().isoformat()
        
        # Store data in DynamoDB
        table = dynamodb.Table(TABLE_NAME)
        response = table.put_item(
            Item={
                'bucketName': bucket_name,
                'timestamp': timestamp,
                'totalSize': total_size,
                'objectCount': object_count
            }
        )
        
        logger.info(f"Stored bucket size info - Bucket: {bucket_name}, Size: {total_size}, Count: {object_count}")
        return response
    except Exception as e:
        logger.error(f"Error calculating or storing bucket size: {e}")
        raise