import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import boto3
from framework.audit_operations import update_records_in_file_audit_tbl
from framework.cloudwatch_interfaces import send_logs


def create_job_audit_details(
    job_name, workflow_execution_id, status, error_msg=None
) -> dict:

    if status == "succesful":

        job_audit_map = {
            "job_name": job_name,
            "workflow_execution_id": workflow_execution_id,
            "execution_status": status,
            "execution_end_ts": datetime.utcnow().strftime("%m-%d-%y-%H-%M-%S"),
        }
    else:
        job_audit_map = {
            "job_name": job_name,
            "workflow_execution_id": workflow_execution_id,
            "execution_status": status,
            "execution_end_ts": datetime.utcnow().strftime("%m-%d-%y-%H-%M-%S"),
            "error_message": error_msg,
        }

    return job_audit_map


def get_error_message(log_file_path: str) -> str:
    """
    Parses the log file and returns the error message

    :param str log file name including the absolute path
    :return: error log Messages within the log file
    :rtype: str

    """

    error_pattern = "Error Code"
    log_file_lines = []

    try:
        print("Parsing the log file for errors : " + log_file_path)

        with open(Path(log_file_path)) as f:
            for line in f:
                log_file_lines.append(line)

        i = 0
        for i in range(0, len(log_file_lines) - 1):
            match = re.match(error_pattern, log_file_lines[i])
            if match:
                sql_error_code = log_file_lines[i + 1]
                sql_error_message = log_file_lines[i + 2]

                error_msg = (
                    "Error Code : "
                    + sql_error_code
                    + "\n"
                    + "Error Message : "
                    + sql_error_message
                )

                break
            else:
                error_msg = ""

        return error_msg

    except Exception as e:
        print("Exception occured : " + str(e))
        raise


def send_token(
    token,
    job_name,
    error_code,
    log_file_name,
    workflow_execution_id,
    audit_ddb_tbl,
    log_group,
    region,
):

    # print(error_code)
    # print(type(error_code))

    # success task token
    if error_code == "0":
        print(f"sending success response using token which is {token}")

        current_time = datetime.utcnow().strftime("%m-%d-%y %H:%M:%S")
        msg = job_name + "execution succeeded at " + current_time

        job_audit_map = create_job_audit_details(
            job_name, workflow_execution_id, "successful"
        )
        update_records_in_file_audit_tbl(job_audit_map, audit_ddb_tbl, region)

        send_logs(log_group, log_file_name, workflow_execution_id, region)

        response = sfn_client.send_task_success(
            taskToken=token,
            output=json.dumps(
                {"job_name": job_name, "status": "completed", "message": msg}
            ),
        )

    # failure task token
    else:
        print(f"sending failure response using token which is {token}")

        error_msg = get_error_message(log_file_name)

        job_audit_map = create_job_audit_details(
            job_name, workflow_execution_id, "failed", error_msg
        )
        update_records_in_file_audit_tbl(job_audit_map, audit_ddb_tbl)

        send_logs(log_group, log_file_name, workflow_execution_id)

        response = sfn_client.send_task_failure(
            taskToken=token, error=str(error_code), cause=error_msg
        )

    print(f"send token response is {response}")
    print(f"token return completed")


print(f"entering python script")
n = len(sys.argv)
for i in range(1, n):
    print(sys.argv[i], end=" ")

token = sys.argv[1]

job_name = sys.argv[2]
error_code = sys.argv[3]
log_file_name = sys.argv[4]
# workflow_id = sys.argv[5]
workflow_execution_id = sys.argv[5]
audit_ddb_table = sys.argv[6]
log_group = sys.argv[7]
region = sys.argv[8]

print(f"received the token {token}")
print(f"sub token return")

sfn_client = boto3.client("stepfunctions", region_name=region)
ssm_client = boto3.client("ssm", region_name=region)

send_token(
    token,
    job_name,
    error_code,
    log_file_name,
    workflow_execution_id,
    audit_ddb_table,
    log_group,
    region,
)
print(f"sub token return done")
