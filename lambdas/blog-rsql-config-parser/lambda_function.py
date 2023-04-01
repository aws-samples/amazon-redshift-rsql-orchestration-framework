# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import os
from datetime import datetime
from json import dumps

import boto3
from audit_operations import add_record_to_workflow_audit_tbl
from dynamodb_interfaces import _deserialize, query_dynamodb


def read_config_from_tbl(config_table: str, workflow_id: str) -> list:
    partiql_statement = (
        """SELECT * FROM "table_name" WHERE "workflow_id" = 'partition_value'"""
    )
    partiql_statement = partiql_statement.replace("table_name", config_table).replace(
        "partition_value", workflow_id
    )

    config_data = query_dynamodb(partiql_statement)

    print(config_data)

    return config_data


def get_workflow_stages(config_data: list) -> list:
    workflow_stages_list = []
    config_data_dict = _deserialize(config_data[0])

    print(config_data_dict)

    workflow_stages_info = config_data_dict["workflow_stages"]

    for workflow_detail in workflow_stages_info:
        if workflow_detail["execution_flag"].lower() == "y":
            workflow_stages_list.append(workflow_detail)

    return workflow_stages_list


def get_parallel_script_details(config_data: list) -> list:

    parallel_script_list = []

    config_data_dict = config_data[0]
    parallel_script_info = config_data_dict["parallel"]["L"]

    for script_detail in parallel_script_info:
        parallel_script_list.append(script_detail["S"])

    print(parallel_script_list)

    return parallel_script_list


def get_sequential_script_details(config_data: list) -> list:

    sequential_script_list = []

    config_data_dict = config_data[0]
    parallel_script_info = config_data_dict["sequential"]["L"]

    for script_detail in parallel_script_info:
        sequential_script_list.append(script_detail["S"])

    print(sequential_script_list)

    return sequential_script_list


def create_workflow_audit_details(
    workflow_id: str, workflow_stage_list: list, workflow_execution_id: str
) -> dict:

    rsql_jobs_list = []
    for stage in workflow_stage_list:
        rsql_jobs_list = rsql_jobs_list + stage["scripts"]

    workflow_audit_map = {
        "workflow_id": workflow_id,
        "workflow_execution_id": workflow_execution_id,
        "rsql_jobs": rsql_jobs_list,
        "execution_status": "running",
        "execution_start_ts": datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S"),
    }

    return workflow_audit_map


def trigger_step_function(
    input_payload: dict,
    master_step_function_arn: str,
    workflow_execution_id: str,
) -> object:

    sfn_client = boto3.client("stepfunctions")

    sfn_response = sfn_client.start_execution(
        stateMachineArn=master_step_function_arn,
        name=workflow_execution_id,
        input=dumps(input_payload),
    )
    response_code = sfn_response["ResponseMetadata"]["HTTPStatusCode"]

    if response_code == 200:
        execution_arn = sfn_response["executionArn"]
        print("RSQL Master Step Function Execution ARN : " + execution_arn)
        return execution_arn
    else:
        return None


def lambda_handler(event, context):

    print(event)

    workflow_id = event["workflow_id"]
    workflow_execution_id = event["workflow_execution_id"]

    config_tbl = os.environ["config_table"]
    config_data = read_config_from_tbl(config_tbl, workflow_id)
    workflow_stage_list = get_workflow_stages(config_data)

    workflow_audit_tbl = os.environ["workflow_audit_table"]
    workflow_audit_detail = create_workflow_audit_details(
        workflow_id, workflow_stage_list, workflow_execution_id
    )
    add_record_to_workflow_audit_tbl(workflow_audit_detail, workflow_audit_tbl)

    master_step_function_arn = os.environ["rsql_master_step_function"]
    input_payload = {
        "statusCode": 200,
        "workflow_id": workflow_id,
        "workflow_execution_id": workflow_execution_id,
        "stage_details": workflow_stage_list,
        "index": -1,
        "count": len(workflow_stage_list) - 1,
    }

    print("Triggering RSQL Master Step Function")

    execution_arn = trigger_step_function(
        input_payload, master_step_function_arn, workflow_execution_id
    )

    if execution_arn:
        return {
            "statusCode": 200,
            "status": "Execution ARN : " + execution_arn,
        }
    else:
        return {
            "statusCode": 500,
            "status": "Master Step Function Executin Failed",
        }
