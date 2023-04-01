import json
import sys
import time
from datetime import datetime
from pathlib import Path

import boto3


def create_logstream(log_group: str, log_stream: str, cwlog_client: object) -> None:

    """
    Creates a logstream per log file

    :param str log_group cloudwatch log group name
    :param str log_stream logstream to be created
    """

    response = cwlog_client.create_log_stream(
        logGroupName=log_group, logStreamName=log_stream
    )


def put_logs(
    log_group: str, log_stream: str, log_message: str, seq_token, cwlog_client: object
) -> None:

    log_event = {
        "logGroupName": log_group,
        "logStreamName": log_stream,
        "logEvents": [
            {
                "timestamp": int(round(time.time() * 1000)),
                "message": log_message,
            }
        ],
    }
    if seq_token:
        log_event["sequenceToken"] = seq_token

    response = cwlog_client.put_log_events(**log_event)

    seq_token = response["nextSequenceToken"]

    print(seq_token)
    return response


# code execution starts here


def send_logs(
    log_group_name: str, log_path: str, workflow_execution_id: str, region: str
) -> None:

    cwlog_client = boto3.client("logs", region_name=region)

    seq_token = None

    try:
        log_stream_name = workflow_execution_id + "-" + log_path.split("/")[-1]
        create_logstream(log_group_name, log_stream_name, cwlog_client)
    except Exception as e:
        print(e)
        raise

    try:
        with open(log_path) as f:
            while True:
                # Added this there is a threshold for  PutLogEvents API Call
                # https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
                log_file = f.read(252144)
                if not log_file:
                    break
                response = put_logs(
                    log_group_name, log_stream_name, log_file, seq_token, cwlog_client
                )
                seq_token = response["nextSequenceToken"]
            print("RSQL Logs Published Successfully to CloudWatch")
    except Exception as e:
        print("RSQL Logs not published: ", e)
        raise
