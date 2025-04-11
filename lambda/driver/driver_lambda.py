import json
import boto3
import os
import time
import urllib.request
import logging

# Initialize AWS services
s3 = boto3.client('s3')

# Get environment variables
BUCKET_NAME = os.environ['BUCKET_NAME']
API_URL = os.environ['API_URL']

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    """
    Driver function to test the Assignment 4 workflow.
    This function creates and uploads objects to trigger the entire workflow.
    """
    logger.info(f"Starting driver execution for bucket: {BUCKET_NAME}")
    
    try:
        # Step 1: Create assignment1.txt (19 bytes)
        content1 = "Empty Assignment 1"
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key="assignment1.txt",
            Body=content1
        )
        logger.info(f"Created assignment1.txt with content: '{content1}'")
        
        # Wait for SNS/SQS/Lambda processing
        logger.info("Waiting 10 seconds for processing...")
        time.sleep(10)
        
        # Step 2: Create assignment2.txt (28 bytes)
        content2 = "Empty Assignment 2222222222"
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key="assignment2.txt",
            Body=content2
        )
        logger.info(f"Created assignment2.txt with content: '{content2}'")
        
        # Wait for the alarm to trigger and cleaner to delete assignment2.txt
        logger.info("Waiting 30 seconds for alarm and cleaner to process...")
        time.sleep(30)
        
        # Check if assignment2.txt was deleted (this is just for logging)
        try:
            s3.head_object(Bucket=BUCKET_NAME, Key="assignment2.txt")
            logger.info("assignment2.txt still exists - cleaner may not have been triggered")
        except:
            logger.info("assignment2.txt was deleted by the cleaner as expected")
        
        # Step 3: Create assignment3.txt (2 bytes)
        content3 = "33"
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key="assignment3.txt",
            Body=content3
        )
        logger.info(f"Created assignment3.txt with content: '{content3}'")
        
        # Wait for the alarm to trigger and cleaner to delete assignment1.txt
        logger.info("Waiting 30 seconds for alarm and cleaner to process...")
        time.sleep(30)
        
        # Check if assignment1.txt was deleted (this is just for logging)
        try:
            s3.head_object(Bucket=BUCKET_NAME, Key="assignment1.txt")
            logger.info("assignment1.txt still exists - cleaner may not have been triggered")
        except:
            logger.info("assignment1.txt was deleted by the cleaner as expected")
        
        # Call the plotting API
        logger.info(f"Calling plotting API at: {API_URL}")
        with urllib.request.urlopen(API_URL) as response:
            api_response = response.read().decode('utf-8')
            logger.info(f"API Response: {api_response}")
        
        return {
            'statusCode': 200,
            'body': json.dumps('Assignment 4 driver executed successfully')
        }
    except Exception as e:
        logger.error(f"Error in driver execution: {e}")
        raise