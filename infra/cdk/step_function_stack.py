# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from aws_cdk import Aspects, Duration, Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_ssm as ssm
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct


class StepFunctionStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, lambda_stack, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        rsql_parallel_load = self._create_rsql_parallel_load(lambda_stack)
        rsql_sequential_load = self._create_rsql_sequential_load(lambda_stack)

        rsql_master_state_machine_arn = self._create_rsql_master_load(
            lambda_stack, rsql_sequential_load, rsql_parallel_load
        )

        self._create_blog_rsql_workflow_trigger_function(
            lambda_stack, rsql_master_state_machine_arn
        )

    def _create_rsql_parallel_load(self, lambda_stack) -> sfn.IStateMachine:

        rsql_invoke_lambda_task = tasks.LambdaInvoke(
            self,
            "rsql_invoke_lambda_task",
            lambda_function=lambda_stack.blog_rsql_invoke_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "token": sfn.JsonPath.task_token,
                    "workflow_id": sfn.JsonPath.string_at("$.workflow_id"),
                    "script": sfn.JsonPath.string_at("$.script"),
                    "workflow_execution_id": sfn.JsonPath.string_at(
                        "$.workflow_execution_id"
                    ),
                }
            ),
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            output_path="$",
        )

        rsql_parallel_invoke_map_task = sfn.Map(
            self,
            "rsql_invoke_map_task",
            max_concurrency=40,
            items_path=sfn.JsonPath.string_at("$.Payload.parallel_details"),
            result_path="$.parallel_output",
        )

        rsql_parallel_invoke_map_task.iterator(rsql_invoke_lambda_task)

        rsql_parallel_load_check_task = tasks.LambdaInvoke(
            self,
            "rsql_parallel_load_check_task",
            lambda_function=lambda_stack.blog_parallel_load_check_lambda,
            payload=sfn.TaskInput.from_object(
                {"parallel_output": sfn.JsonPath.string_at("$.parallel_output")}
            ),
            invocation_type=tasks.LambdaInvocationType.REQUEST_RESPONSE,
            result_path="$.parallel_check",
        )

        rsql_worklow_audit_table_update_task = tasks.LambdaInvoke(
            self,
            "rsql_worklow_audit_table_update_task",
            lambda_function=lambda_stack.blog_update_ddb_function,
            payload=sfn.TaskInput.from_object(
                {
                    "status": "failed",
                    "workflow_id": sfn.JsonPath.string_at("$.workflow_id"),
                }
            ),
            invocation_type=tasks.LambdaInvocationType.REQUEST_RESPONSE,
            output_path="$.Payload",
        )

        parallel_success_task = sfn.Succeed(self, "parallel_success_task")
        parallel_fail_task = sfn.Fail(self, "parallel_fail_task")

        rsql_worklow_audit_table_update_task.next(parallel_fail_task)

        parallel_load_definition = rsql_parallel_invoke_map_task.next(
            rsql_parallel_load_check_task
        ).next(
            sfn.Choice(self, "parallel_load_check_status")
            .when(
                sfn.Condition.string_equals(
                    "$.parallel_check.Payload.parallel_load_status",
                    "successful",
                ),
                parallel_success_task,
            )
            .otherwise(rsql_worklow_audit_table_update_task)
        )

        rsql_parallel_state_machine = sfn.StateMachine(
            self,
            "rsql_parallel_state_machine",
            state_machine_name="rsql-parallel-state-machine",
            definition=parallel_load_definition,
            timeout=Duration.minutes(1440),
        )

        return rsql_parallel_state_machine

    def _create_rsql_sequential_load(self, lambda_stack) -> sfn.IStateMachine:

        rsql_sequential_iterator_lambda_task = tasks.LambdaInvoke(
            self,
            "rsql_sequential_iterator_lambda_task",
            lambda_function=lambda_stack.blog_sequential_iterator_lambda,
            invocation_type=tasks.LambdaInvocationType.REQUEST_RESPONSE,
            output_path="$.Payload",
        )

        rsql_sequential_lambda_invoke_task = tasks.LambdaInvoke(
            self,
            "rsql_sequential_lambda_invoke_task",
            lambda_function=lambda_stack.blog_rsql_invoke_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "token": sfn.JsonPath.task_token,
                    "workflow_id": sfn.JsonPath.string_at("$.workflow_id"),
                    "script": sfn.JsonPath.string_at("$.execution_details.script"),
                    "workflow_execution_id": sfn.JsonPath.string_at(
                        "$.execution_details.workflow_execution_id"
                    ),
                }
            ),
            result_path="$.result",
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
        )

        rsql_sequential_lambda_invoke_task.next(rsql_sequential_iterator_lambda_task)

        rsql_sequential_completed_task = sfn.Pass(
            self, "rsql_sequential_completed_task"
        )

        rsql_sequential_load_definition = rsql_sequential_iterator_lambda_task.next(
            sfn.Choice(self, "sequential_payload_count_check")
            .when(
                sfn.Condition.number_less_than_json_path("$.index", "$.count"),
                rsql_sequential_lambda_invoke_task,
            )
            .otherwise(rsql_sequential_completed_task)
        )

        rsql_sequential_state_machine: sfn.IStateMachine = sfn.StateMachine(
            self,
            "rsql_sequential_state_machine",
            state_machine_name="rsql-sequential-state-machine",
            definition=rsql_sequential_load_definition,
            timeout=Duration.minutes(1440),
        )

        return rsql_sequential_state_machine

    def _create_rsql_master_load(
        self,
        lambda_stack,
        rsql_sequential_state_machine,
        rsql_parallel_state_machine,
    ) -> str:

        # rsql_ddb_config_parser_task = tasks.LambdaInvoke(self,'rsql_ddb_config_parser_task',
        #         lambda_function = lambda_stack.blog_rsql_config_parser_lambda,
        #         invocation_type = tasks.LambdaInvocationType.REQUEST_RESPONSE,
        #         output_path = '$.Payload')

        rsql_master_iterator_task = tasks.LambdaInvoke(
            self,
            "rsql_master_iterator_task",
            lambda_function=lambda_stack.blog_master_iterator_lambda,
            invocation_type=tasks.LambdaInvocationType.REQUEST_RESPONSE,
            payload=sfn.TaskInput.from_object(
                {
                    "workflow_id": sfn.JsonPath.string_at("$.workflow_id"),
                    "stage_details": sfn.JsonPath.string_at("$.stage_details"),
                    "count": sfn.JsonPath.string_at("$.count"),
                    "index": sfn.JsonPath.string_at("$.index"),
                    "workflow_execution_id": sfn.JsonPath.string_at(
                        "$.workflow_execution_id"
                    ),
                }
            ),
            result_path="$.current_stage_details",
        )

        rsql_generate_payload_parallel_task = tasks.LambdaInvoke(
            self,
            "rsql_generate_payload_parallel_task",
            lambda_function=lambda_stack.blog_payload_generator_lambda,
            invocation_type=tasks.LambdaInvocationType.REQUEST_RESPONSE,
            payload=sfn.TaskInput.from_object(
                {
                    "execution_mode": sfn.JsonPath.string_at(
                        "$.current_stage_details.Payload.execution_details.execution_mode"
                    ),
                    "workflow_id": sfn.JsonPath.string_at("$.workflow_id"),
                    "workflow_execution_id": sfn.JsonPath.string_at(
                        "$.workflow_execution_id"
                    ),
                    "execution_details": sfn.JsonPath.string_at(
                        "$.current_stage_details.Payload.execution_details"
                    ),
                }
            ),
            result_path="$.parallel_stage_details",
        )

        rsql_generate_payload_sequence_task = tasks.LambdaInvoke(
            self,
            "rsql_generate_payload_sequence_task",
            lambda_function=lambda_stack.blog_payload_generator_lambda,
            invocation_type=tasks.LambdaInvocationType.REQUEST_RESPONSE,
            payload=sfn.TaskInput.from_object(
                {
                    "execution_mode": sfn.JsonPath.string_at(
                        "$.current_stage_details.Payload.execution_details.execution_mode"
                    ),
                    "workflow_id": sfn.JsonPath.string_at("$.workflow_id"),
                    "workflow_execution_id": sfn.JsonPath.string_at(
                        "$.workflow_execution_id"
                    ),
                    "execution_details": sfn.JsonPath.string_at(
                        "$.current_stage_details.Payload.execution_details"
                    ),
                }
            ),
            result_path=sfn.JsonPath.DISCARD,
        )

        rsql_parallel_load_task = tasks.StepFunctionsStartExecution(
            self,
            "rsql_parallel_load_task",
            state_machine=rsql_parallel_state_machine,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            associate_with_parent=True,
            input=sfn.TaskInput.from_object(
                {
                    "Payload": sfn.JsonPath.string_at(
                        "$.parallel_stage_details.Payload"
                    ),
                    "workflow_id": sfn.JsonPath.string_at("$.workflow_id"),
                    "workflow_execution_id": sfn.JsonPath.string_at(
                        "$.workflow_execution_id"
                    ),
                }
            ),
            result_path=sfn.JsonPath.DISCARD,
        )

        rsql_generate_payload_parallel_task.next(rsql_parallel_load_task)

        rsql_sequential_load_task = tasks.StepFunctionsStartExecution(
            self,
            "rsql_sequential_load_task",
            state_machine=rsql_sequential_state_machine,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            associate_with_parent=True,
            input=sfn.TaskInput.from_object(
                {
                    "sequential": sfn.JsonPath.string_at(
                        "$.current_stage_details.Payload.execution_details.scripts"
                    ),
                    "workflow_id": sfn.JsonPath.string_at("$.workflow_id"),
                    "workflow_execution_id": sfn.JsonPath.string_at(
                        "$.workflow_execution_id"
                    ),
                    "index": -1,
                }
            ),
            result_path=sfn.JsonPath.DISCARD,
        )

        rsql_generate_payload_sequence_task.next(rsql_sequential_load_task)

        rsql_transform_payload_pass_state = sfn.Pass(
            self,
            "rsql_transform_payload_pass_state",
            parameters={
                "statusCode": 200,
                "workflow_id": sfn.JsonPath.string_at("$.workflow_id"),
                "workflow_execution_id": sfn.JsonPath.string_at(
                    "$.workflow_execution_id"
                ),
                "index": sfn.JsonPath.string_at(
                    "$.current_stage_details.Payload.index"
                ),
                "count": sfn.JsonPath.string_at(
                    "$.current_stage_details.Payload.count"
                ),
                "stage_details": sfn.JsonPath.string_at("$.stage_details"),
            },
            output_path="$",
        )

        rsql_parallel_load_task.next(rsql_transform_payload_pass_state)
        rsql_sequential_load_task.next(rsql_transform_payload_pass_state)

        rsql_worklow_audit_table_success_task = tasks.LambdaInvoke(
            self,
            "rsql_worklow_audit_table_success_task",
            lambda_function=lambda_stack.blog_update_ddb_function,
            payload=sfn.TaskInput.from_object(
                {
                    "status": "successful",
                    "workflow_id": sfn.JsonPath.string_at("$.workflow_id"),
                    "workflow_execution_id": sfn.JsonPath.string_at(
                        "$.workflow_execution_id"
                    ),
                }
            ),
            invocation_type=tasks.LambdaInvocationType.REQUEST_RESPONSE,
            output_path="$.Payload",
        )

        rsql_master_load_failure = sfn.Fail(self, "rsql_failure")
        rsql_master_load_success = sfn.Succeed(self, "rsql_success")

        rsql_worklow_audit_table_success_task.next(rsql_master_load_success)

        rsql_transform_payload_pass_state.next(
            sfn.Choice(self, "rsql_master_check-count")
            .when(
                sfn.Condition.number_less_than_json_path("$.index", "$.count"),
                rsql_master_iterator_task,
            )
            .otherwise(rsql_worklow_audit_table_success_task)
        )

        # rsql_master_load_definition = rsql_ddb_config_parser_task\

        rsql_master_load_definition = rsql_master_iterator_task.next(
            sfn.Choice(self, "rsql_load_type_check")
            .when(
                sfn.Condition.string_equals(
                    "$.current_stage_details.Payload.execution_details.execution_mode",
                    "parallel",
                ),
                rsql_generate_payload_parallel_task,
            )
            .when(
                sfn.Condition.string_equals(
                    "$.current_stage_details.Payload.execution_details.execution_mode",
                    "sequential",
                ),
                rsql_generate_payload_sequence_task,
            )
            .otherwise(rsql_master_load_failure)
        )

        rsql_master_state_machine = sfn.StateMachine(
            self,
            "rsql_master_state_machine",
            state_machine_name="rsql-master-state-machine",
            definition=rsql_master_load_definition,
            timeout=Duration.minutes(1440),
        )

        return rsql_master_state_machine.state_machine_arn

    def _create_blog_rsql_workflow_trigger_function(
        self, lambda_stack, rsql_master_state_machine_arn
    ) -> None:

        workflow_audit_tbl = ssm.StringParameter.from_string_parameter_attributes(
            self,
            "WorkflowAuditTableParameter",
            parameter_name="/blog/rsql/WorkflowAuditTableParameter",
        ).string_value

        config_tbl = ssm.StringParameter.from_string_parameter_attributes(
            self,
            "ConfigTableParameter",
            parameter_name="/blog/rsql/ConfigTableParameter",
        ).string_value

        blog_rsql_workflow_trigger_lambda: _lambda.Function = _lambda.Function(
            self,
            "blog_rsql_workflow_trigger_lambda",
            function_name="rsql-blog-rsql-workflow-trigger-lambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            timeout=Duration.seconds(300),
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("../lambdas/blog-rsql-config-parser"),
            layers=[lambda_stack.lambda_layer],
            environment={
                "workflow_audit_table": workflow_audit_tbl,
                "config_table": config_tbl,
                "rsql_master_step_function": rsql_master_state_machine_arn,
            },
        )

        if blog_rsql_workflow_trigger_lambda.role:
            blog_rsql_workflow_trigger_lambda.role.add_to_principal_policy(
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
            # blog_rsql_workflow_trigger_lambda.role.add_managed_policy(
            #     iam.ManagedPolicy.from_aws_managed_policy_name(
            #         "AmazonDynamoDBFullAccess"
            #     )
            # )

            blog_rsql_workflow_trigger_lambda.role.add_to_principal_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    resources=[
                        f"arn:{self.partition}:states:{self.region}:{self.account}:stateMachine:rsql*"
                    ],
                    actions=[
                        "states:DescribeExecution",
                        "states:DescribeStateMachine",
                        "states:DescribeStateMachineForExecution",
                        "states:GetExecutionHistory",
                        "states:ListExecutions",
                        "states:ListStateMachines",
                        "states:SendTaskFailure",
                        "states:SendTaskHeartbeat",
                        "states:SendTaskSuccess",
                        "states:StartExecution",
                        "states:StartSyncExecution",
                    ],
                )
            )

            # blog_rsql_workflow_trigger_lambda.role.add_managed_policy(
            #     iam.ManagedPolicy.from_aws_managed_policy_name(
            #         "AWSStepFunctionsFullAccess"
            #     )
            # )
