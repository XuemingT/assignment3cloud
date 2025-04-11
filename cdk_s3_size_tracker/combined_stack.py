from aws_cdk import (
    Duration,
    Stack,
    aws_s3 as s3,
    aws_lambda as lambda_,
    aws_dynamodb as dynamodb,
    aws_apigateway as apigateway,
    RemovalPolicy,
    CfnOutput,
    aws_s3_notifications as s3n,
    aws_sns as sns,
    aws_sqs as sqs,
    aws_sns_subscriptions as sns_subscriptions,
    aws_lambda_event_sources as lambda_event_sources,
    aws_logs as logs,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cloudwatch_actions
)
from constructs import Construct

class CdkS3SizeTrackerStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create the S3 bucket
        test_bucket = s3.Bucket(self, "TestBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        # Create DynamoDB table
        s3_object_size_history = dynamodb.Table(self, "S3ObjectSizeHistory",
            partition_key=dynamodb.Attribute(
                name="bucketName",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="timestamp",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY
        )

        # NEW - SNS Topic for S3 events
        s3_event_topic = sns.Topic(self, "S3EventTopic")
        
        # NEW - SQS Queues for consumers
        size_tracking_queue = sqs.Queue(self, "SizeTrackingQueue",
            visibility_timeout=Duration.seconds(300)
        )
        
        logging_queue = sqs.Queue(self, "LoggingQueue",
            visibility_timeout=Duration.seconds(300)
        )
        
        # NEW - Subscribe queues to SNS topic
        s3_event_topic.add_subscription(
            sns_subscriptions.SqsSubscription(size_tracking_queue)
        )
        s3_event_topic.add_subscription(
            sns_subscriptions.SqsSubscription(logging_queue)
        )
        
        # Configure S3 to publish events to SNS
        test_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED, 
            s3n.SnsDestination(s3_event_topic)
        )
        test_bucket.add_event_notification(
            s3.EventType.OBJECT_REMOVED, 
            s3n.SnsDestination(s3_event_topic)
        )

        # Size tracking lambda
        size_tracking_lambda = lambda_.Function(self, "SizeTrackingLambda",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("lambda/size_tracking"),
            handler="size_tracking_lambda.handler",
            environment={
                "TABLE_NAME": s3_object_size_history.table_name,
                "BUCKET_NAME": test_bucket.bucket_name
            },
            timeout=Duration.seconds(30)
        )
        
        # NEW - Add SQS as event source for size-tracking lambda
        size_tracking_lambda.add_event_source(
            lambda_event_sources.SqsEventSource(size_tracking_queue)
        )
        
        # Grant permissions
        test_bucket.grant_read(size_tracking_lambda)
        s3_object_size_history.grant_read_write_data(size_tracking_lambda)

        # NEW - Logging Lambda
        logging_lambda = lambda_.Function(self, "LoggingLambda",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("lambda/logging"),
            handler="logging_lambda.handler",
            environment={
                "BUCKET_NAME": test_bucket.bucket_name,
            },
            timeout=Duration.seconds(30)
        )
        
        # NEW - Add SQS as event source for logging lambda
        logging_lambda.add_event_source(
            lambda_event_sources.SqsEventSource(logging_queue)
        )
        
        # Grant permissions
        test_bucket.grant_read(logging_lambda)
        
        # NEW - Create log group for logging lambda
        log_group = logs.LogGroup(self, "LoggingLambdaLogGroup",
            log_group_name=f"/aws/lambda/{logging_lambda.function_name}",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY
        )

        # NEW - Create metric filter for object size changes
        metric_filter = logs.MetricFilter(self, "ObjectSizeDeltaMetricFilter",
            log_group=log_group,
            filter_pattern=logs.FilterPattern.literal('{ $.object_name = * $.size_delta = * }'),
            metric_namespace="Assignment4App",
            metric_name="TotalObjectSize",
            metric_value="$.size_delta",
            default_value=0
        )

        # NEW - Create alarm for total object size
        size_alarm = cloudwatch.Alarm(self, "TotalObjectSizeAlarm",
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
        cleaner_lambda = lambda_.Function(self, "CleanerLambda",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("lambda/cleaner"),
            handler="cleaner_lambda.handler",
            environment={
                "BUCKET_NAME": test_bucket.bucket_name
            },
            timeout=Duration.seconds(30)
        )
        
        # Grant permissions
        test_bucket.grant_read_write(cleaner_lambda)
        
        # NEW - Configure alarm to trigger cleaner lambda
        size_alarm.add_alarm_action(
            cloudwatch_actions.LambdaAction(cleaner_lambda)
        )

        # Plotting lambda
        plotting_lambda = lambda_.Function(self, "PlottingLambda",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("lambda/plotting"),
            handler="plotting_lambda.handler",
            environment={
                "TABLE_NAME": s3_object_size_history.table_name,
                "BUCKET_NAME": test_bucket.bucket_name
            },
            timeout=Duration.seconds(30),
            memory_size=256,
            layers=[
                lambda_.LayerVersion.from_layer_version_arn(
                    self, "MatplotlibLayer",
                    "arn:aws:lambda:us-east-1:770693421928:layer:Klayers-p39-matplotlib:5"
                )
            ]
        )
        
        # Grant permissions
        test_bucket.grant_read_write(plotting_lambda)
        s3_object_size_history.grant_read_data(plotting_lambda)
        
        # Create API Gateway
        api = apigateway.RestApi(self, "PlottingApi",
            rest_api_name="Assignment4 Plotting API",
            description="API for plotting S3 bucket size changes"
        )
        
        plotting_resource = api.root.add_resource("plot")
        plotting_resource.add_method("GET", apigateway.LambdaIntegration(plotting_lambda))

        # Driver Lambda
        driver_lambda = lambda_.Function(self, "DriverLambda",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("lambda/driver"),
            handler="driver_lambda.handler",
            environment={
                "BUCKET_NAME": test_bucket.bucket_name,
                "API_URL": f"{api.url}plot"
            },
            timeout=Duration.seconds(300)
        )
        
        # Grant permissions
        test_bucket.grant_read_write(driver_lambda)

        # Outputs
        CfnOutput(self, "BucketName", value=test_bucket.bucket_name)
        CfnOutput(self, "TableName", value=s3_object_size_history.table_name)
        CfnOutput(self, "ApiUrl", value=api.url)