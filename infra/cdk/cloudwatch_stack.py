# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from aws_cdk import Stack
from aws_cdk import aws_logs as logs
from aws_cdk import aws_ssm as ssm
from aws_cdk import RemovalPolicy
from constructs import Construct




class CloudWatchStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        log_group = "/ops/rsql-logs/"

        self._create_log_group(log_group)
        self._create_ssm_parameters(log_group)

    def _create_log_group(self, log_group: str) -> None:

        rsql_log_group: logs.LogGroup = logs.LogGroup(
            self,
            "rsql_log_group",
            log_group_name=log_group,
            retention=logs.RetentionDays.THREE_MONTHS,
            removal_policy = RemovalPolicy.DESTROY
        )


    def _create_ssm_parameters(self, log_group) -> None:

        log_group_ssm_param: ssm.StringListParameter = ssm.StringParameter(
            self,
            "LogGroupParameter",
            parameter_name="/blog/rsql/LogGroupParameter",
            allowed_pattern=".*",
            description="RSQL Cloudwatch Log Group",
            string_value=log_group,
            tier=ssm.ParameterTier.ADVANCED,
        )
