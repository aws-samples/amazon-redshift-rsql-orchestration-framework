# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import os
from datetime import datetime

from audit_operations import update_records_in_workflow_audit_tbl


def create_workflow_audit_record(
    workflow_id: str, workflow_execution_id: str, execution_status: str
):

    workflow_audit_record = {
        "workflow_id": workflow_id,
        "workflow_execution_id": workflow_execution_id,
        "execution_status": execution_status,
        "execution_end_ts": datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S"),
    }

    return workflow_audit_record


def lambda_handler(event, context):

    print(event)

    workflow_id = event["workflow_id"]
    workflow_execution_id = event["workflow_execution_id"]
    execution_status = event["status"]

    workflow_audit_tbl = os.environ["workflow_audit_table"]

    workflow_audit_record = create_workflow_audit_record(
        workflow_id, workflow_execution_id, execution_status
    )
    update_records_in_workflow_audit_tbl(workflow_audit_record, workflow_audit_tbl)
    return {
        "statusCode": 200,
        "body": json.dumps("workflow audit table updated"),
    }
