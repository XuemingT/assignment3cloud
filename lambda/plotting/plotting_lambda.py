import os
import json
import boto3
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt
import datetime
from decimal import Decimal
import io
from boto3.dynamodb.conditions import Key

# Initialize boto3 clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

# Get environment variables - updated variable names
table_name = os.environ['TABLE_NAME']  # Changed from DYNAMODB_TABLE_NAME
bucket_name = os.environ['BUCKET_NAME']  # Changed from S3_BUCKET_NAME
table = dynamodb.Table(table_name)

def handler(event, context):
    """
    Lambda function to generate a plot of S3 bucket size over time.
    Retrieves data from DynamoDB and creates a plot that is saved to S3.
    """
    print(f"Received event: {json.dumps(event)}")
    
    try:
        # Get the current time and calculate the time 10 seconds ago
        current_time = datetime.datetime.now()
        ten_seconds_ago = current_time - datetime.timedelta(seconds=10)
        
        # Get bucket size data for the last 10 seconds
        recent_data = get_recent_bucket_data(bucket_name, ten_seconds_ago.isoformat())
        
        # Get the maximum bucket size ever recorded
        max_size = get_max_bucket_size(bucket_name)
        
        # Generate and save the plot
        plot_url = generate_plot(recent_data, max_size)
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'message': 'Plot generated successfully',
                'plotUrl': plot_url
            })
        }
    except Exception as e:
        print(f"Error generating plot: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def get_recent_bucket_data(bucket_name, start_timestamp):
    """
    Query DynamoDB to get bucket size data for the last 10 seconds.
    """
    try:
        # Query with bucket_name as partition key
        response = table.query(
            KeyConditionExpression=Key('bucketName').eq(bucket_name) & 
                                  Key('timestamp').gte(start_timestamp)
        )
        
        # Sort data by timestamp
        items = sorted(response['Items'], key=lambda x: x['timestamp'])
        return items
    except Exception as e:
        print(f"Error querying recent bucket data: {str(e)}")
        # Try alternative key names if the first attempt fails
        try:
            response = table.query(
                KeyConditionExpression=Key('bucket_name').eq(bucket_name) & 
                                     Key('timestamp').gte(start_timestamp)
            )
            items = sorted(response['Items'], key=lambda x: x['timestamp'])
            return items
        except Exception as e2:
            print(f"Second attempt failed: {str(e2)}")
            raise

def get_max_bucket_size(bucket_name):
    """
    Query DynamoDB to get the maximum bucket size ever recorded.
    """
    try:
        # Query with bucketName as the partition key
        response = table.query(
            KeyConditionExpression=Key('bucketName').eq(bucket_name)
        )
        
        if not response['Items']:
            return 0
        
        # Make sure size field exists in the items (check both possible field names)
        max_size = 0
        for item in response['Items']:
            # Check for different possible field names
            if 'totalSize' in item:
                size = float(item['totalSize'])
            elif 'total_size' in item:
                size = float(item['total_size'])
            else:
                continue
                
            if size > max_size:
                max_size = size
                
        return max_size
    except Exception as e:
        print(f"Error querying max bucket size: {str(e)}")
        # Try alternative key names if the first attempt fails
        try:
            response = table.query(
                KeyConditionExpression=Key('bucket_name').eq(bucket_name)
            )
            if not response['Items']:
                return 0
                
            max_size = 0
            for item in response['Items']:
                if 'totalSize' in item:
                    size = float(item['totalSize'])
                elif 'total_size' in item:
                    size = float(item['total_size'])
                else:
                    continue
                    
                if size > max_size:
                    max_size = size
                    
            return max_size
        except Exception as e2:
            print(f"Second attempt failed: {str(e2)}")
            print(f"First few items: {json.dumps(response['Items'][:2], default=str) if 'Items' in response and response['Items'] else 'No items'}")
            raise

def generate_plot(data, max_size):
    """
    Generate a plot of bucket size over time and save it to S3.
    """
    try:
        # Extract data for plotting - handle different field names
        timestamps = []
        sizes = []
        
        for item in data:
            timestamps.append(item['timestamp'])
            
            # Handle different possible field names
            if 'totalSize' in item:
                sizes.append(float(item['totalSize']))
            elif 'total_size' in item:
                sizes.append(float(item['total_size']))
            else:
                print(f"Warning: size field not found in item: {json.dumps(item, default=str)}")
                sizes.append(0)  # Default if field not found
        
        # Create plot
        plt.figure(figsize=(10, 6))
        plt.plot(timestamps, sizes, 'bo-', label='Bucket Size')
        
        # Add a horizontal line for max size
        if max_size > 0:
            plt.axhline(y=float(max_size), color='r', linestyle='--', 
                       label=f'Max Size: {float(max_size)} bytes')
        
        # Format the plot
        plt.title('S3 Bucket Size Over Time')
        plt.xlabel('Timestamp')
        plt.ylabel('Size (bytes)')
        plt.xticks(rotation=45)
        plt.legend()
        plt.tight_layout()
        
        # Save the plot to a buffer
        buffer = io.BytesIO()
        plt.savefig(buffer, format='png')
        buffer.seek(0)
        
        # Upload the plot to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key='plot',
            Body=buffer,
            ContentType='image/png'
        )
        
        # Generate a pre-signed URL for the plot (valid for 1 hour)
        plot_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': 'plot'},
            ExpiresIn=3600
        )
        
        plt.close()  # Close the plot to free memory
        
        return plot_url
    except Exception as e:
        print(f"Error generating plot: {str(e)}")
        raise