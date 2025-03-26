import os
import json
import boto3
import time
import requests
#fail
# Initialize boto3 clients
s3_client = boto3.client('s3')

# Get environment variables
bucket_name = os.environ['S3_BUCKET_NAME']
plotting_api_endpoint = os.environ['PLOTTING_API_ENDPOINT']

def handler(event, context):
    """
    Driver Lambda function to perform a series of operations on the test bucket
    and then call the plotting lambda via API Gateway.
    """
    try:
        print("Starting driver lambda execution...")
        
        # Create object assignment1.txt with content "Empty Assignment 1" (19 bytes)
        print("Creating assignment1.txt...")
        s3_client.put_object(
            Bucket=bucket_name,
            Key='assignment1.txt',
            Body='Empty Assignment 1'
        )
        
        # Sleep for a few seconds
        time.sleep(3)
        
        # Update object assignment1.txt with content "Empty Assignment 2222222222" (28 bytes)
        print("Updating assignment1.txt...")
        s3_client.put_object(
            Bucket=bucket_name,
            Key='assignment1.txt',
            Body='Empty Assignment 2222222222'
        )
        
        # Sleep for a few seconds
        time.sleep(3)
        
        # Delete object assignment1.txt
        print("Deleting assignment1.txt...")
        s3_client.delete_object(
            Bucket=bucket_name,
            Key='assignment1.txt'
        )
        
        # Sleep for a few seconds
        time.sleep(3)
        
        # Create object assignment2.txt with content "33" (2 bytes)
        print("Creating assignment2.txt...")
        s3_client.put_object(
            Bucket=bucket_name,
            Key='assignment2.txt',
            Body='33'
        )
        
        # Sleep for a few seconds
        time.sleep(3)
        
        # Call the plotting lambda via API Gateway
        print(f"Calling plotting API at: {plotting_api_endpoint}")
        response = requests.get(plotting_api_endpoint)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Driver lambda execution completed successfully',
                'plottingApiResponse': response.json()
            })
        }
    except Exception as e:
        print(f"Error in driver lambda: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }