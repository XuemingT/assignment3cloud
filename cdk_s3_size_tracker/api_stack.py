from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_apigateway as apigw,
)
from constructs import Construct


class ApiStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        plotting_lambda: _lambda.Function,
        driver_lambda: _lambda.Function,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # Create the REST API
        api = apigw.RestApi(
            self,
            "PlottingApi",
            rest_api_name="S3-Size-Plotting-API",
            description="API for triggering the plotting lambda",
        )
        
        # Add a resource and method to the API
        plot_resource = api.root.add_resource("plot")
        plot_integration = apigw.LambdaIntegration(plotting_lambda)
        plot_resource.add_method("GET", plot_integration)
        
        # Add the API endpoint URL to the driver lambda environment
        driver_lambda.add_environment(
            "PLOTTING_API_ENDPOINT",
            f"{api.url}plot"
        )