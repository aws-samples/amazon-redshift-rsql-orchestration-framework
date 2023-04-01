# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json


def lambda_handler(event, context):

    parallel_execution_details = event["parallel_output"]

    parallel_load_status = ""
    parallel_script_list = []

    for script_detail in parallel_execution_details:
        parallel_script_list.append(script_detail["job_name"])
        if script_detail["status"] != "completed":
            parallel_load_status = "failed"
        else:
            parallel_load_status = "successful"

    return {
        "statusCode": 200,
        "parallel_load_status": parallel_load_status,
        "parallel_script_list": parallel_script_list,
    }
