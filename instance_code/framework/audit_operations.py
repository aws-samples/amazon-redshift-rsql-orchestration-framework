import logging
import os
import time
from datetime import datetime
from decimal import Decimal
from typing import List

from .dynamodb_interfaces import put_item_into_dynamodb, update_dynamodb_items

logger = logging.getLogger()

#######################################################################################################################
#################################################### DynamoDB Interfaces ##############################################
#######################################################################################################################


def add_record_to_workflow_audit_tbl(
    workflow_audit_item: dict, workflow_ddb_tbl: str, region: str
) -> None:
    """Puts item into Workflow Audit DynamoDB table

    :param dict workflow_audit_item:
    :param str ddb_tbl:
    :return: None
    :rtype: None
    """

    try:
        workflow_audit_item["execution_start_ts"] = datetime.utcnow().strftime(
            "%m/%d/%Y %H:%M:%S"
        )
        put_item_into_dynamodb(workflow_ddb_tbl, workflow_audit_item, region)
    except Exception as e:
        logger.error(
            "Error while adding record to the Workflow Audit Table : " + str(e)
        )
        raise


def add_record_in_workflow_audit_tbl(
    workflow_audit_item: dict, workflow_ddb_tbl: str, region: str
) -> None:
    """Puts item into Workflow Audit DynamoDB table

    :param dict workflow_audit_item:
    :param str ddb_tbl:
    :return: None
    :rtype: None
    """

    try:
        # workflow_audit_item['execution_start_ts'] = datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")
        put_item_into_dynamodb(workflow_ddb_tbl, workflow_audit_item, region)
    except Exception as e:
        logger.error(
            "Error while adding record to the Workflow Audit Table : " + str(e)
        )
        raise


def update_records_in_workflow_audit_tbl(
    workflow_audit_item: dict, workflow_ddb_tbl: str, region: str
) -> None:
    """Updates item into Workflow Audit DynamoDB table

    :param dict workflow_audit_item:
    :param str ddb_tbl:
    :return: None
    :rtype: None
    """
    try:
        # workflow_audit_item['execution_end_ts'] = datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")
        update_dynamodb_items(workflow_ddb_tbl, [workflow_audit_item], region)
    except Exception as e:
        logger.error(
            "Error while updating record to the Workflow Audit Table : " + str(e)
        )
        raise


def add_record_in_file_audit_tbl(
    file_audit_item: dict, file_ddb_tbl: str, region: str
) -> None:
    """Puts item into File Audit DynamoDB table

    :param dict workflow_audit_item:
    :param str ddb_tbl:
    :return: None
    :rtype: None
    """

    try:

        # file_audit_item['execution_start_ts'] = datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")
        put_item_into_dynamodb(file_ddb_tbl, file_audit_item, region)
    except Exception as e:
        logger.error("Error while adding record to the File Audit Table : " + str(e))
        raise


def update_records_in_file_audit_tbl(
    file_audit_item: dict, file_ddb_tbl: str, region: str
) -> None:
    """Updates item into Workflow Audit DynamoDB table

    :param dict workflow_audit_item:
    :param str ddb_tbl:
    :return: None
    :rtype: None
    """
    try:

        # workflow_audit_item['execution_end_ts'] = datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")
        update_dynamodb_items(file_ddb_tbl, [file_audit_item], region)
    except Exception as e:
        logger.error("Error while updating record to the File Audit Table : " + str(e))
        raise
