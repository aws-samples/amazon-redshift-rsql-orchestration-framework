# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json


def lambda_handler(event, context):

    workflow_stage_list = event["stage_details"]
    count = event["count"]
    index = event["index"]
    workflow_id = event["workflow_id"]
    workflow_execution_id = event["workflow_execution_id"]

    execution_details = {
        "execution_mode": workflow_stage_list[index + 1]["execution_type"],
        "scripts": workflow_stage_list[index + 1]["scripts"],
    }

    return {
        "count": count,
        "index": index + 1,
        "execution_details": execution_details,
        "workflow_id": workflow_id,
        "workflow_execution_id": workflow_execution_id,
    }
