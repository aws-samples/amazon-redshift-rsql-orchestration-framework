# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

#!/usr/bin/env python3
import os

import aws_cdk as cdk
from cdk.cloudwatch_stack import CloudWatchStack
from cdk.dynamodb_stack import DynamodbStack
from cdk.lambda_stack import LambdaStack
from cdk.permission_stack import PermissionStack
from cdk.step_function_stack import StepFunctionStack

# from cdk_nag import (AwsSolutionsChecks, NagSuppressions)


app = cdk.App()

permission_stack = PermissionStack(app, "RSQLPermissionStack")
cloudwatch_stack = CloudWatchStack(app, "RSQLCloudwatchStack")
dynamodb_stack = DynamodbStack(app, "RSQLDynamodbStack")
lambda_stack = LambdaStack(app, "RSQLLambdaStack")
step_function_stack = StepFunctionStack(app, "RSQLStepFunctionStack", lambda_stack)

cloudwatch_stack.add_dependency(permission_stack)
dynamodb_stack.add_dependency(cloudwatch_stack)
lambda_stack.add_dependency(dynamodb_stack)
step_function_stack.add_dependency(lambda_stack)

# cdk.Aspects.of(app).add(AwsSolutionsChecks())
app.synth()
