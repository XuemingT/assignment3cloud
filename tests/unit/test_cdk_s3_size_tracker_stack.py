import aws_cdk as core
import aws_cdk.assertions as assertions

from cdk_s3_size_tracker.cdk_s3_size_tracker_stack import CdkS3SizeTrackerStack

# example tests. To run these tests, uncomment this file along with the example
# resource in cdk_s3_size_tracker/cdk_s3_size_tracker_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = CdkS3SizeTrackerStack(app, "cdk-s3-size-tracker")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
