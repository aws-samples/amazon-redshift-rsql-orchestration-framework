# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from aws_cdk import Stack
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_ssm as ssm
from constructs import Construct


class DynamodbStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        config_tbl = "rsql-blog-rsql-config-table"
        workflow_audit_tbl = "rsql-blog-rsql-workflow-audit-table"
        job_audit_tbl = "rsql-blog-rsql-job-audit-table"

        self._create_config_table(config_tbl)
        self._create_audit_tables(workflow_audit_tbl, job_audit_tbl)
        self._create_ssm_parameters(config_tbl, workflow_audit_tbl, job_audit_tbl)

    def _create_config_table(self, config_tbl: str) -> None:

        rsql_config_table: dynamodb.Table = dynamodb.Table(
            self,
            "rsql_config_table",
            table_name=config_tbl,
            partition_key=dynamodb.Attribute(
                name="workflow_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PROVISIONED,
        )

    def _create_audit_tables(self, workflow_audit_tbl: str, job_audit_tbl: str) -> None:

        rsql_workflow_audit_table: dynamodb.Table = dynamodb.Table(
            self,
            "rsql_workflow_audit_table",
            table_name=workflow_audit_tbl,
            partition_key=dynamodb.Attribute(
                name="workflow_execution_id",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="workflow_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PROVISIONED,
        )

        rsql_job_audit_table: dynamodb.Table = dynamodb.Table(
            self,
            "rsql_job_audit_table",
            table_name=job_audit_tbl,
            partition_key=dynamodb.Attribute(
                name="job_name", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="workflow_execution_id",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PROVISIONED,
        )

    def _create_ssm_parameters(
        self, config_tbl, workflow_audit_tbl, job_audit_tbl
    ) -> None:

        config_tbl_ssm_param: ssm.StringListParameter = ssm.StringParameter(
            self,
            "ConfigTableParameter",
            parameter_name="/blog/rsql/ConfigTableParameter",
            allowed_pattern=".*",
            description="RSQL DDB Config Table",
            string_value=config_tbl,
            tier=ssm.ParameterTier.ADVANCED,
        )

        workflow_audit_tbl_ssm_param: ssm.StringListParameter = ssm.StringParameter(
            self,
            "WorkflowAuditTableParameter",
            parameter_name="/blog/rsql/WorkflowAuditTableParameter",
            allowed_pattern=".*",
            description="RSQL DDB Workflow Audit Table",
            string_value=workflow_audit_tbl,
            tier=ssm.ParameterTier.ADVANCED,
        )

        job_audit_tbl_ssm_param: ssm.StringListParameter = ssm.StringParameter(
            self,
            "JobAuditTableParameter",
            parameter_name="/blog/rsql/JobAuditTableParameter",
            allowed_pattern=".*",
            description="RSQL DDB Job Audit Table",
            string_value=job_audit_tbl,
            tier=ssm.ParameterTier.ADVANCED,
        )
