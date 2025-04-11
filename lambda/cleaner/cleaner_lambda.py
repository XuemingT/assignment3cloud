import json
import boto3
import os
import logging

# Initialize AWS services
s3 = boto3.client('s3')

# Get environment variables
BUCKET_NAME = os.environ['BUCKET_NAME']

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    """
    Delete the largest object in the S3 bucket when triggered by a CloudWatch alarm.
    This function is triggered when the TotalObjectSize metric exceeds the threshold.
    """
    logger.info(f"Received event: {json.dumps(event)}")
    
    try:
        # Find the largest object in the bucket
        largest_object = find_largest_object(BUCKET_NAME)
        
        if largest_object:
            # Delete the largest object
            s3.delete_object(
                Bucket=BUCKET_NAME,
                Key=largest_object['Key']
            )
            
            logger.info(f"Deleted largest object: {largest_object['Key']} with size {largest_object['Size']} bytes")
            return {
                'statusCode': 200,
                'body': json.dumps(f"Deleted object {largest_object['Key']} with size {largest_object['Size']} bytes")
            }
        else:
            logger.warning(f"No objects found in bucket {BUCKET_NAME}")
            return {
                'statusCode': 200,
                'body': json.dumps(f"No objects found in bucket {BUCKET_NAME}")
            }
    except Exception as e:
        logger.error(f"Error cleaning bucket: {e}")
        raise

def find_largest_object(bucket_name):
    """
    Find the largest object in the given S3 bucket.
    
    Returns:
        dict: A dictionary containing the Key and Size of the largest object,
              or None if the bucket is empty.
    """
    try:
        # List all objects in the bucket
        response = s3.list_objects_v2(Bucket=bucket_name)
        
        if 'Contents' not in response:
            return None
        
        # Find the largest object
        largest_object = None
        largest_size = 0
        
        for obj in response['Contents']:
            if obj['Size'] > largest_size:
                largest_size = obj['Size']
                largest_object = {
                    'Key': obj['Key'],
                    'Size': obj['Size']
                }
        
        return largest_object
    except Exception as e:
        logger.error(f"Error finding largest object: {e}")
        raise