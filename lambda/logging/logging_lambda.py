import json
import boto3
import os
import logging
import datetime

# Initialize AWS services
s3 = boto3.client('s3')
logs_client = boto3.client('logs')
cloudwatch = boto3.client('cloudwatch')

# Get environment variables
BUCKET_NAME = os.environ['BUCKET_NAME']
PUBLISH_METRICS = os.environ.get('PUBLISH_METRICS', 'false').lower() == 'true'

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    """
    Process SQS messages containing S3 events and log object size changes to CloudWatch.
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
                            
                            # Process the S3 event
                            process_s3_event(s3_record)
        except Exception as e:
            logger.error(f"Error processing SQS message: {e}")
            continue

def process_s3_event(s3_record):
    """
    Process an S3 event and log information about object size changes.
    """
    try:
        event_name = s3_record['eventName']
        object_key = s3_record['s3']['object']['key']
        
        if event_name.startswith('ObjectCreated'):
            # For object creation, get the size from the event
            size_delta = s3_record['s3']['object'].get('size', 0)
            log_size_change(object_key, size_delta)
            
        elif event_name.startswith('ObjectRemoved'):
            # For object deletion, we need to find the size of the deleted object
            # This requires searching CloudWatch logs for the previous creation event
            size_delta = find_deleted_object_size(object_key)
            if size_delta > 0:
                log_size_change(object_key, -size_delta)  # Negative size for deletions
    except Exception as e:
        logger.error(f"Error processing S3 event: {e}")
        raise

def log_size_change(object_name, size_delta):
    """
    Log object size changes and publish metric directly to CloudWatch.
    """
    log_data = {
        "object_name": object_name,
        "size_delta": size_delta
    }
    
    # Log as plain string for better visibility in CloudWatch logs
    print(json.dumps(log_data))
    
    # Also log with logger for good measure
    logger.info(json.dumps(log_data))
    
    # Publish metric directly to CloudWatch if enabled
    if PUBLISH_METRICS:
        try:
            cloudwatch.put_metric_data(
                Namespace="Assignment4App",
                MetricData=[
                    {
                        'MetricName': 'TotalObjectSize',
                        'Value': size_delta,
                        'Unit': 'Bytes'
                    }
                ]
            )
            logger.info(f"Published metric: TotalObjectSize = {size_delta}")
        except Exception as e:
            logger.error(f"Error publishing metric: {e}")

def find_deleted_object_size(object_key):
    """
    Search CloudWatch logs to find the size of a deleted object from its creation event.
    """
    try:
        # Get the log group name from the environment variable
        log_group_name = f"/aws/lambda/{os.environ.get('AWS_LAMBDA_FUNCTION_NAME')}"
        
        # Calculate the start time (24 hours ago)
        start_time = int((datetime.datetime.now() - datetime.timedelta(days=1)).timestamp() * 1000)
        
        try:
            # Try to search for logs containing the object creation
            response = logs_client.filter_log_events(
                logGroupName=log_group_name,
                filterPattern=f'{{$.object_name = "{object_key}"}}',
                startTime=start_time
            )
            
            # Find the most recent creation event for this object
            for event in response.get('events', []):
                try:
                    log_message = json.loads(event['message'])
                    if 'object_name' in log_message and log_message['object_name'] == object_key:
                        if 'size_delta' in log_message and log_message['size_delta'] > 0:
                            return log_message['size_delta']
                except json.JSONDecodeError:
                    continue
                    
            # If we can't find the size, log a warning and return 0
            logger.warning(f"Could not find size for deleted object: {object_key}")
            return 0
        except Exception as e:
            logger.warning(f"Error searching logs, using default size 0: {e}")
            return 0
    except Exception as e:
        logger.error(f"Error finding deleted object size: {e}")
        return 0