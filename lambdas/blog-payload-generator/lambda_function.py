# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json


def lambda_handler(event, context):

    print(event)

    execution_mode = event["execution_mode"]
    workflow_id = event["workflow_id"]
    execution_details = event["execution_details"]
    workflow_execution_id = event["workflow_execution_id"]

    print(execution_details)

    if execution_mode == "parallel":
        # generate payload for parallel load

        parallel_details = []
        for script in execution_details["scripts"]:
            parallel_map = {
                "workflow_id": workflow_id,
                "script": script,
                "workflow_execution_id": workflow_execution_id,
            }

            parallel_details.append(parallel_map)

        response = {
            "statusCode": 200,
            "workflow_id": workflow_id,
            "parallel_details": parallel_details,
        }

    elif execution_mode == "sequential":
        # generate payload for sequential load

        response = {
            "statusCode": 200,
            "workflow_id": workflow_id,
            "sequential": execution_details["scripts"],
            "workflow_execution_id": workflow_execution_id,
            "index": -1,
        }

    else:

        response = {"statusCode": 500, "errorMessage": "Invalid execution"}

    return response
