#!/usr/bin/env python3
from aws_cdk import (
    App,
    Stack,
    RemovalPolicy,
    Duration,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    aws_lambda as _lambda,
    aws_s3_notifications as s3n,
    aws_apigateway as apigw,
    BundlingOptions,
    DockerImage,
)
from constructs import Construct


class CombinedStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create the S3 bucket
        self.bucket = s3.Bucket(
            self,
            "TestBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # Create the DynamoDB table for tracking S3 object size history
        self.table = dynamodb.Table(
            self,
            "S3ObjectSizeHistory",
            partition_key=dynamodb.Attribute(
                name="bucket_name",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="timestamp",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # Add a GSI to query by timestamp for the plotting lambda
        self.table.add_global_secondary_index(
            index_name="TimestampIndex",
            partition_key=dynamodb.Attribute(
                name="bucket_name",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="timestamp",
                type=dynamodb.AttributeType.STRING
            ),
        )

        # Create the size-tracking lambda
        self.size_tracking_lambda = _lambda.Function(
            self,
            "SizeTrackingLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="size_tracking_lambda.handler",
            code=_lambda.Code.from_asset("lambda/size_tracking"),
            timeout=Duration.seconds(30),
            environment={
                "DYNAMODB_TABLE_NAME": self.table.table_name,
            },
        )
        
        # Grant the lambda permissions to access S3 and DynamoDB
        self.bucket.grant_read(self.size_tracking_lambda)
        self.table.grant_write_data(self.size_tracking_lambda)
        
        # Add S3 bucket notifications to trigger the size-tracking lambda
        self.bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(self.size_tracking_lambda)
        )
        self.bucket.add_event_notification(
            s3.EventType.OBJECT_REMOVED,
            s3n.LambdaDestination(self.size_tracking_lambda)
        )
        
        # Create the plotting lambda
        self.plotting_lambda = _lambda.Function(
            self,
            "PlottingLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="plotting_lambda.handler",
            code=_lambda.Code.from_asset("lambda/plotting"),
            timeout=Duration.seconds(60),
            memory_size=512,  # More memory for matplotlib
            environment={
                "DYNAMODB_TABLE_NAME": self.table.table_name,
                "S3_BUCKET_NAME": self.bucket.bucket_name,
            },
        )
        
        # Grant the plotting lambda permissions to access DynamoDB and S3
        self.table.grant_read_data(self.plotting_lambda)
        self.bucket.grant_write(self.plotting_lambda)
        
        # Create the REST API for the plotting lambda
        api = apigw.RestApi(
            self,
            "PlottingApi",
            rest_api_name="S3-Size-Plotting-API",
            description="API for triggering the plotting lambda",
        )

        # Since we can't bundle easily, let's use a simpler approach
        self.driver_lambda = _lambda.Function(
            self,
            "DriverLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="driver_lambda.handler",
            code=_lambda.Code.from_asset("lambda/driver"),
            timeout=Duration.seconds(60),
            environment={
                "S3_BUCKET_NAME": self.bucket.bucket_name,
                "PLOTTING_API_ENDPOINT": f"{api.url}plot"
            },
        )
        
        # Grant the driver lambda permissions to access S3
        self.bucket.grant_read_write(self.driver_lambda)
        
        # Add a resource and method to the API
        plot_resource = api.root.add_resource("plot")
        plot_integration = apigw.LambdaIntegration(self.plotting_lambda)
        plot_resource.add_method("GET", plot_integration)


# Create the CDK app
app = App()

# Create an instance of the combined stack
CombinedStack(app, "S3SizeTrackerStack")

# Synthesize the CloudFormation template
app.synth()