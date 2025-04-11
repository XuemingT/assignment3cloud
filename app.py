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
    aws_sns as sns,
    aws_sqs as sqs,
    aws_sns_subscriptions as sns_subscriptions,
    aws_lambda_event_sources as lambda_event_sources,
    aws_logs as logs,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cloudwatch_actions,
    aws_iam as iam,
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
                name="bucketName",
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
                name="bucketName",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="timestamp",
                type=dynamodb.AttributeType.STRING
            ),
        )

        # NEW - SNS Topic for S3 events
        self.s3_event_topic = sns.Topic(self, "S3EventTopic")
        
        # NEW - SQS Queues for consumers
        self.size_tracking_queue = sqs.Queue(
            self, 
            "SizeTrackingQueue",
            visibility_timeout=Duration.seconds(300)
        )
        
        self.logging_queue = sqs.Queue(
            self, 
            "LoggingQueue",
            visibility_timeout=Duration.seconds(300)
        )
        
        # NEW - Subscribe queues to SNS topic
        self.s3_event_topic.add_subscription(
            sns_subscriptions.SqsSubscription(self.size_tracking_queue)
        )
        self.s3_event_topic.add_subscription(
            sns_subscriptions.SqsSubscription(self.logging_queue)
        )
        
        # NEW - Configure S3 to publish events to SNS (instead of directly to lambda)
        self.bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED, 
            s3n.SnsDestination(self.s3_event_topic)
        )
        self.bucket.add_event_notification(
            s3.EventType.OBJECT_REMOVED, 
            s3n.SnsDestination(self.s3_event_topic)
        )

        # Create the size-tracking lambda (updated to consume from SQS)
        self.size_tracking_lambda = _lambda.Function(
            self,
            "SizeTrackingLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="size_tracking_lambda.handler",
            code=_lambda.Code.from_asset("lambda/size_tracking"),
            timeout=Duration.seconds(30),
            environment={
                "TABLE_NAME": self.table.table_name,
                "BUCKET_NAME": self.bucket.bucket_name,
            },
        )
        
        # NEW - Add SQS as event source for size-tracking lambda
        self.size_tracking_lambda.add_event_source(
            lambda_event_sources.SqsEventSource(self.size_tracking_queue)
        )
        
        # Grant the lambda permissions to access S3 and DynamoDB
        self.bucket.grant_read(self.size_tracking_lambda)
        self.table.grant_write_data(self.size_tracking_lambda)
        
        # NEW - Create the logging lambda
        self.logging_lambda = _lambda.Function(
            self,
            "LoggingLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="logging_lambda.handler",
            code=_lambda.Code.from_asset("lambda/logging"),
            timeout=Duration.seconds(30),
            environment={
                "BUCKET_NAME": self.bucket.bucket_name,
                "PUBLISH_METRICS": "true"
            },
        )
        
        # NEW - Add SQS as event source for logging lambda
        self.logging_lambda.add_event_source(
            lambda_event_sources.SqsEventSource(self.logging_queue)
        )
        
        # Grant permissions
        self.bucket.grant_read(self.logging_lambda)
        
        # Give the logging Lambda permission to publish metrics
        self.logging_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["cloudwatch:PutMetricData"],
                resources=["*"]
            )
        )
        
        # NEW - Create log group for logging lambda
        self.log_group = logs.LogGroup(
            self, 
            "LoggingLambdaLogGroup",
            log_group_name=f"/aws/lambda/{self.logging_lambda.function_name}",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY
        )

        # NEW - Create alarm for total object size
        self.size_alarm = cloudwatch.Alarm(
            self, 
            "TotalObjectSizeAlarm",
            metric=cloudwatch.Metric(
                namespace="Assignment4App",
                metric_name="TotalObjectSize",
                statistic="Sum",
                period=Duration.minutes(1)  # Adjust based on testing
            ),
            threshold=20,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD
        )

        # NEW - Cleaner Lambda
        self.cleaner_lambda = _lambda.Function(
            self,
            "CleanerLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset("lambda/cleaner"),
            handler="cleaner_lambda.handler",
            environment={
                "BUCKET_NAME": self.bucket.bucket_name
            },
            timeout=Duration.seconds(30)
        )
        
        # Grant permissions
        self.bucket.grant_read_write(self.cleaner_lambda)
        
        # NEW - Configure alarm to trigger cleaner lambda
        self.size_alarm.add_alarm_action(
            cloudwatch_actions.LambdaAction(self.cleaner_lambda)
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
                "TABLE_NAME": self.table.table_name,
                "BUCKET_NAME": self.bucket.bucket_name,
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

        # Update driver lambda with new test sequence and longer timeout
        self.driver_lambda = _lambda.Function(
            self,
            "DriverLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="driver_lambda.handler",
            code=_lambda.Code.from_asset("lambda/driver"),
            timeout=Duration.seconds(300),  # Increased timeout for waiting
            environment={
                "BUCKET_NAME": self.bucket.bucket_name,
                "API_URL": f"{api.url}plot"
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