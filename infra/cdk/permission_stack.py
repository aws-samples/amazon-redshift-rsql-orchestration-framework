# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from aws_cdk import CustomResource, Duration, Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as _lambda
from aws_cdk import custom_resources as crs
from constructs import Construct


class PermissionStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self._add_iam_policies()

    def _add_iam_policies(self) -> None:

        environment_params = self.node.try_get_context("environment")
        instance_id = environment_params["ec2_instance_id"]
        secret_name = environment_params["redshift_secret_id"]

        blog_iam_permission_lambda: _lambda.Function = _lambda.Function(
            self,
            "blog_iam_permission_lambda",
            function_name="rsql-blog-iam-permission-lambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            timeout=Duration.seconds(500),
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("../lambdas/blog-add-iam-permissions"),
        )

        if blog_iam_permission_lambda.role:
            blog_iam_permission_lambda.role.add_to_principal_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    resources=[
                        f"arn:{self.partition}:ec2:{self.region}:{self.account}:instance/{instance_id}"
                    ],
                    actions=[
                        "ec2:DescribeIamInstanceProfileAssociations",
                        "ec2:AssociateIamInstanceProfile",
                        "ec2:DisassociateIamInstanceProfile",
                        "ec2:CreateInstanceProfile",
                    ],
                )
            )

            blog_iam_permission_lambda.role.add_to_principal_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    resources=["*"],
                    actions=["ec2:DescribeInstances"],
                )
            )

            blog_iam_permission_lambda.role.add_to_principal_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    resources=[
                        f"arn:{self.partition}:secretsmanager:{self.region}:{self.account}:secret:{secret_name}*"
                    ],
                    actions=["secretsmanager:DescribeSecret"],
                )
            )

            blog_iam_permission_lambda.role.add_to_principal_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    resources=["*"],
                    actions=[
                        "iam:CreateRole",
                        "iam:GetRole",
                        "iam:GetRolePolicy",
                        "iam:ListRolePolicies",
                        "iam:ListPolicies",
                        "iam:CreatePolicy",
                        "iam:PutRolePolicy",
                        "iam:CreatePolicyVersion",
                        "iam:CreateInstanceProfile",
                        "iam:GetInstanceProfile",
                        "iam:ListInstanceProfiles",
                        "iam:AddRoleToInstanceProfile",
                        "iam:PassRole"
                    ],
                )
            )

        permissionProvider = crs.Provider(
            self,
            "PermissionProvider",
            on_event_handler=blog_iam_permission_lambda,
        )

        # invoking the custom resource
        lambda_custom_resource = CustomResource(
            self,
            "PermissionAdder",
            service_token=permissionProvider.service_token,
            properties={"InstanceID": instance_id, "SecretName": secret_name},
        )
