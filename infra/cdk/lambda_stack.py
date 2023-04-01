# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from typing import Any

from aws_cdk import Duration, Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_ssm as ssm
from constructs import Construct


class LambdaStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.lambda_layer = self._create_lambda_layer()

        self.blog_rsql_invoke_lambda: _lambda.IFunction = (
            self._create_blog_rsql_invoke_function(self.lambda_layer)
        )
        # self.blog_rsql_config_parser_lambda : _lambda.IFunction = self._create_blog_rsql_config_parser_function(lambda_layer)
        self.blog_update_ddb_function: _lambda.IFunction = (
            self._create_blog_update_audit_ddb_function(self.lambda_layer)
        )

        self.blog_master_iterator_lambda: _lambda.IFunction = (
            self._create_blog_master_iterator_function()
        )
        self.blog_parallel_load_check_lambda: _lambda.IFunction = (
            self._create_blog_parallel_load_check_function()
        )
        self.blog_payload_generator_lambda: _lambda.IFunction = (
            self._create_blog_payload_generator_function()
        )
        self.blog_sequential_iterator_lambda: _lambda.IFunction = (
            self._create_blog_sequential_iterator_function()
        )

    def _create_lambda_layer(self) -> _lambda.ILayerVersion:

        lambda_layer: _lambda.ILayerVersion = _lambda.LayerVersion(
            self,
            "rsql_lambda_layer",
            layer_version_name="blog_rsql_lambda_layer",
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_9],
            code=_lambda.Code.from_asset("../lambdas/lambda-layer/lambda-layer.zip"),
        )
        return lambda_layer

    def _create_blog_master_iterator_function(self) -> _lambda.IFunction:

        blog_master_iterator_lambda: _lambda.Function = _lambda.Function(
            self,
            "blog_master_iterator_lambda",
            function_name="rsql-blog-master-iterator-lambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            timeout=Duration.seconds(60),
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("../lambdas/blog-master-iterator"),
        )

        return blog_master_iterator_lambda

    def _create_blog_parallel_load_check_function(self) -> _lambda.IFunction:

        blog_parallel_load_check_lambda: _lambda.Function = _lambda.Function(
            self,
            "blog_parallel_load_check_lambda",
            function_name="rsql-blog-parallel-load-check-lambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            timeout=Duration.seconds(60),
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("../lambdas/blog-parallel-load-check"),
        )

        return blog_parallel_load_check_lambda

    def _create_blog_payload_generator_function(self) -> _lambda.IFunction:

        blog_payload_generator_lambda: _lambda.Function = _lambda.Function(
            self,
            "blog_payload_generator_lambda",
            function_name="rsql-blog-payload-generator-lambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            timeout=Duration.seconds(60),
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("../lambdas/blog-payload-generator"),
        )

        return blog_payload_generator_lambda

    def _create_blog_sequential_iterator_function(self) -> _lambda.IFunction:

        blog_sequential_iterator_lambda: _lambda.Function = _lambda.Function(
            self,
            "blog_sequential_iterator_lambda",
            function_name="rsql-blog-sequential-iterator-lambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            timeout=Duration.seconds(60),
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("../lambdas/blog-sequential-iterator"),
        )

        return blog_sequential_iterator_lambda

    def _create_blog_rsql_invoke_function(
        self, lambda_layer: _lambda.ILayerVersion
    ) -> None:

        # get the list of environment variables from cdk.json
        environment_params = self.node.try_get_context("environment")
        job_audit_tbl = ssm.StringParameter.from_string_parameter_attributes(
            self,
            "JobAuditTableParameter",
            parameter_name="/blog/rsql/JobAuditTableParameter",
        ).string_value

        rsql_log_group = ssm.StringParameter.from_string_parameter_attributes(
            self,
            "LogGroupParameter",
            parameter_name="/blog/rsql/LogGroupParameter",
        ).string_value

        blog_rsql_invoke_lambda: _lambda.Function = _lambda.Function(
            self,
            "blog_rsql_invoke_lambda",
            function_name="rsql-blog-rsql-invoke-lambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            timeout=Duration.seconds(300),
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("../lambdas/blog-rsql-invoke"),
            layers=[lambda_layer],
            environment={
                "secret_id": environment_params["redshift_secret_id"],
                "rsql_path": environment_params["rsql_script_path"],
                "log_path": environment_params["rsql_log_path"],
                "instance_id": environment_params["ec2_instance_id"],
                "job_audit_table": job_audit_tbl,
                "rsql_log_group": rsql_log_group,
                "rsql_trigger": environment_params["rsql_script_wrapper"],
            },
        )

        if blog_rsql_invoke_lambda.role:
            blog_rsql_invoke_lambda.role.add_to_principal_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    resources=[
                        f"arn:{self.partition}:dynamodb:{self.region}:{self.account}:table/rsql*"
                    ],
                    actions=[
                        "dynamodb:BatchGetItem",
                        "dynamodb:BatchWriteItem",
                        "dynamodb:DescribeTable",
                        "dynamodb:GetItem",
                        "dynamodb:GetRecords",
                        "dynamodb:ListTables",
                        "dynamodb:PartiQLInsert",
                        "dynamodb:PartiQLSelect",
                        "dynamodb:PartiQLUpdate",
                        "dynamodb:PutItem",
                        "dynamodb:Query",
                        "dynamodb:Scan",
                        "dynamodb:UpdateItem",
                        "dynamodb:DescribeTable",
                    ],
                )
            )
            # blog_rsql_invoke_lambda.role.add_managed_policy(
            #     iam.ManagedPolicy.from_aws_managed_policy_name(
            #         "AmazonDynamoDBFullAccess"
            #     )
            # )

            # blog_rsql_invoke_lambda.role.add_managed_policy(
            #     iam.ManagedPolicy.from_aws_managed_policy_name(
            #         "AmazonSSMFullAccess"
            #     )
            # )

            blog_rsql_invoke_lambda.role.add_to_principal_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    resources=[
                        f"arn:{self.partition}:ec2:{self.region}:{self.account}:instance/{environment_params['ec2_instance_id']}",
                        f"arn:{self.partition}:ssm:{self.region}::document/AWS-RunShellScript",
                        f"arn:{self.partition}:s3:::ssm-onboarding-bucket*",
                    ],
                    actions=[
                        "ssm:SendCommand",
                        "ssm:DescribeAssociation",
                        "ssm:ListDocumentVersions",
                        "ssm:DescribeDocument",
                        "ssm:DescribeDocumentParameters",
                        "ssm:GetDocument",
                        "ssm:UpdateAssociation",
                        "ssm:StartSession",
                    ],
                )
            )

        return blog_rsql_invoke_lambda

    # def _create_blog_rsql_config_parser_function(self, lambda_layer : _lambda.ILayerVersion) -> _lambda.IFunction :

    #     workflow_audit_tbl =ssm.StringParameter.from_string_parameter_attributes(
    #         self, 'WorkflowAuditTableParameter', parameter_name = "/blog/rsql/WorkflowAuditTableParameter"
    #         ).string_value

    #     config_tbl =ssm.StringParameter.from_string_parameter_attributes(
    #         self, 'ConfigTableParameter', parameter_name = "/blog/rsql/ConfigTableParameter"
    #         ).string_value

    #     blog_rsql_config_parser_lambda : _lambda.Function =_lambda.Function(
    #         self,"blog_rsql_config_parser_lambda",
    #         function_name = 'rsql-blog-rsql-config-parser-lambda',
    #         runtime = _lambda.Runtime.PYTHON_3_8,
    #         timeout = Duration.seconds(300),
    #         handler = 'lambda_function.lambda_handler',
    #         code = _lambda.Code.from_asset('../lambdas/blog-rsql-config-parser'),
    #         layers = [lambda_layer],
    #         environment =
    #                 {
    #                     'workflow_audit_table' : workflow_audit_tbl,
    #                     'config_table' : config_tbl

    #                 }
    #     )

    #     if(blog_rsql_config_parser_lambda.role):
    #         blog_rsql_config_parser_lambda.role.add_managed_policy(
    #             iam.ManagedPolicy.from_aws_managed_policy_name('AmazonDynamoDBFullAccess'))

    #     return blog_rsql_config_parser_lambda

    def _create_blog_update_audit_ddb_function(
        self, lambda_layer: _lambda.ILayerVersion
    ) -> _lambda.IFunction:

        workflow_audit_tbl = ssm.StringParameter.from_string_parameter_attributes(
            self,
            "WorkflowAuditTableParameter2",
            parameter_name="/blog/rsql/WorkflowAuditTableParameter",
        ).string_value

        blog_update_audit_ddb_lambda: _lambda.Function = _lambda.Function(
            self,
            "blog_update_audit_ddb_lambda",
            function_name="rsql-blog-update-audit-ddb-lambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            timeout=Duration.seconds(300),
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("../lambdas/blog-update-audit-ddb-table"),
            layers=[lambda_layer],
            environment={
                "workflow_audit_table": workflow_audit_tbl,
            },
        )

        if blog_update_audit_ddb_lambda.role:
            blog_update_audit_ddb_lambda.role.add_to_principal_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    resources=[
                        f"arn:{self.partition}:dynamodb:{self.region}:{self.account}:table/rsql*"
                    ],
                    actions=[
                        "dynamodb:BatchGetItem",
                        "dynamodb:BatchWriteItem",
                        "dynamodb:DescribeTable",
                        "dynamodb:GetItem",
                        "dynamodb:GetRecords",
                        "dynamodb:ListTables",
                        "dynamodb:PartiQLInsert",
                        "dynamodb:PartiQLSelect",
                        "dynamodb:PartiQLUpdate",
                        "dynamodb:PutItem",
                        "dynamodb:Query",
                        "dynamodb:Scan",
                        "dynamodb:UpdateItem",
                        "dynamodb:DescribeTable",
                    ],
                )
            )

            # blog_update_audit_ddb_lambda.role.add_managed_policy(
            #     iam.ManagedPolicy.from_aws_managed_policy_name(
            #         "AmazonDynamoDBFullAccess"
            #     )
            # )
        return blog_update_audit_ddb_lambda
