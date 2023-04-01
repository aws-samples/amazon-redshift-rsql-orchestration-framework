# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json


def lambda_handler(event, context):

    sequential_rsql_list = event["sequential"]
    count = len(sequential_rsql_list)
    index = event["index"]
    workflow_id = (event["workflow_id"],)
    workflow_execution_id = event["workflow_execution_id"]

    if index + 1 == count:
        script = "NA"

    else:
        script = sequential_rsql_list[index + 1]

    execution_details = {
        "script": script,
        "workflow_id": workflow_id[0],
        "workflow_execution_id": workflow_execution_id,
    }

    return {
        "sequential": sequential_rsql_list,
        "count": count,
        "index": index + 1,
        "execution_details": execution_details,
        "workflow_id": workflow_id[0],
        "workflow_execution_id": workflow_execution_id,
    }
