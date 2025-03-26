import os
import json
import boto3
import datetime
from decimal import Decimal

# Initialize boto3 clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Get environment variables
table_name = os.environ['DYNAMODB_TABLE_NAME']
table = dynamodb.Table(table_name)

def handler(event, context):
    """
    Lambda function to track S3 bucket size.
    Triggered by S3 events (object creation, update, deletion).
    Calculates total bucket size and stores it in DynamoDB.
    """
    print(f"Received event: {json.dumps(event)}")
    
    # Extract bucket name from the event
    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
    except (KeyError, IndexError):
        print("Error: Unable to extract bucket name from event")
        return {
            'statusCode': 400,
            'body': json.dumps('Invalid event format')
        }
    
    # Get current timestamp in ISO format
    timestamp = datetime.datetime.now().isoformat()
    
    # Calculate total bucket size
    total_size, object_count = calculate_bucket_size(bucket_name)
    
    # Store data in DynamoDB
    store_size_data(bucket_name, timestamp, total_size, object_count)
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'bucket': bucket_name,
            'timestamp': timestamp,
            'totalSize': total_size,
            'objectCount': object_count
        }, default=decimal_default)
    }

def calculate_bucket_size(bucket_name):
    """
    Calculate the total size of all objects in the bucket.
    Returns total size in bytes and object count.
    """
    total_size = 0
    object_count = 0
    
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name)
    
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                total_size += obj['Size']
                object_count += 1
    
    return total_size, object_count

def store_size_data(bucket_name, timestamp, total_size, object_count):
    """
    Store bucket size data in DynamoDB.
    """
    try:
        response = table.put_item(
            Item={
                'bucket_name': bucket_name,
                'timestamp': timestamp,
                'total_size': Decimal(str(total_size)),
                'object_count': object_count
            }
        )
        print(f"Data stored in DynamoDB: {response}")
        return True
    except Exception as e:
        print(f"Error storing data in DynamoDB: {str(e)}")
        return False

def decimal_default(obj):
    """
    Helper function for JSON serialization of Decimal objects.
    """
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError