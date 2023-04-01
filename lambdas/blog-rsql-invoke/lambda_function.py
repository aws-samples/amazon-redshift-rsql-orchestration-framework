# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import datetime
import json
import os
import time
from datetime import datetime

import boto3
from audit_operations import add_record_in_file_audit_tbl

ssm_client = boto3.client("ssm")


def run_shellscript(
    script_name,
    instance_id,
    token,
    workflow_id,
    secret_id,
    rsql_path,
    log_path,
    workflow_execution_id,
    job_audit_tbl,
    rsql_log_group,
    rsql_trigger,
):

    current_time = datetime.utcnow().strftime("%m-%d-%y-%H-%M-%S")
    log_file_name = script_name + "-" + current_time + ".log"

    instance_code_dir = os.path.dirname(rsql_trigger)
    aws_region = os.environ["AWS_REGION"]

    # cmd = "sh +x "+rsql_path+script_name+ " '" + token + "' " +" " + workflow_id + " " + " " + workflow_execution_id + " " + script_name + " " + instance_id + " " + secret_id + " " + log_path+log_file_name + " " + job_audit_tbl + " '" + rsql_log_group + "' " + " > " + log_path+log_file_name + " 2>&1 "
    cmd = (
        "nohup sh +x "
        + rsql_trigger
        + " '"
        + token
        + "' "
        + " "
        + workflow_id
        + " "
        + " "
        + workflow_execution_id
        + " "
        + rsql_path
        + script_name
        + " "
        + instance_id
        + " "
        + secret_id
        + " "
        + log_path
        + log_file_name
        + " "
        + job_audit_tbl
        + " '"
        + rsql_log_group
        + "' "
        + " "
        + instance_code_dir
        + " "
        + aws_region
        + " "
        + " > "
        + log_path
        + log_file_name
        + " 2>&1 &"
    )
    print(f"rsql script invoke command is {cmd}")

    response = ssm_client.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        CloudWatchOutputConfig={
            "CloudWatchLogGroupName": "/aws/ssm/AWS-RunShellScript",
            "CloudWatchOutputEnabled": True,
        },
        Parameters={"commands": [cmd]},
    )
    command_id = response["Command"]["CommandId"]

    print(f"response is {response}")
    print(f"command is {command_id}")

    return command_id


def create_job_audit_details(
    script_name,
    workflow_id,
    workflow_execution_id,
    instance_id,
    ssm_command_id,
):

    job_audit_map = {
        "job_name": script_name,
        "workflow_id": workflow_id,
        "workflow_execution_id": workflow_execution_id,
        "execution_status": "triggered",
        "execution_start_ts": datetime.utcnow().strftime("%m-%d-%y-%H-%M-%S"),
        "instance_id": instance_id,
        "ssm_command_id": ssm_command_id,
    }

    return job_audit_map


def lambda_handler(event, context):

    print(f"printing event -- {event}")
    print(f'printing event -- {event["token"]}')
    print(f'printing script name -- {event["script"]}')

    script_name = event["script"]
    workflow_id = event["workflow_id"]
    workflow_execution_id = event["workflow_execution_id"]
    sfn_token = event["token"]

    secret_id = os.environ["secret_id"]
    rsql_path = os.environ["rsql_path"]
    log_path = os.environ["log_path"]
    instance_id = os.environ["instance_id"]
    job_audit_tbl = os.environ["job_audit_table"]
    rsql_log_group = os.environ["rsql_log_group"]
    rsql_trigger = os.environ["rsql_trigger"]

    print(f"running shell script")

    ssm_command_id = run_shellscript(
        script_name,
        instance_id,
        sfn_token,
        workflow_id,
        secret_id,
        rsql_path,
        log_path,
        workflow_execution_id,
        job_audit_tbl,
        rsql_log_group,
        rsql_trigger,
    )

    job_audit_map = create_job_audit_details(
        script_name,
        workflow_id,
        workflow_execution_id,
        instance_id,
        ssm_command_id,
    )

    add_record_in_file_audit_tbl(job_audit_map, job_audit_tbl)

    return {
        "statusCode": 200,
        "body": json.dumps("Shell Script Triggered"),
        "ssm_command_id": json.dumps(ssm_command_id),
    }
