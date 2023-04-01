import logging
import os
import time
from datetime import datetime
from decimal import Decimal
from typing import List

import boto3
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore import client
from botocore.exceptions import ClientError

logger = logging.getLogger()
serializer = TypeSerializer()
deserializer = TypeDeserializer()

#######################################################################################################################
#################################################### DynamoDB Interfaces ##############################################
#######################################################################################################################


def check_if_tbl_exists_dynamodb(ddb_tbl: str, ddb_client: object) -> bool:
    """Checks for the existence of a Dynamodb table

    :param str ddb_tbl:
    :param obj ddb_client:
    :return: Returns True if tbale exists
    :rtype: bool
    :raises: ClientError
    """
    try:
        response = ddb_client.describe_table(TableName=ddb_tbl)
        return True
    except ddb_client.exceptions.ResourceNotFoundException:
        # do something here as you require
        logger.error("DynamoDB Table Doesn't Exist")
        raise


def put_item_into_dynamodb(ddb_tbl: str, item: dict, region: str) -> None:
    """Puts item into DynamoDB table

    :param str ddb_tbl:
    :param dict item:
    :param str region
    :return: None
    :rtype: None
    :raises: AssertionError
    """
    ddb_client = boto3.client("dynamodb", region_name=region)
    ddb_resource = boto3.resource("dynamodb", region_name=region)

    try:
        assert check_if_tbl_exists_dynamodb(ddb_tbl, ddb_client) is True
        table = ddb_resource.Table(ddb_tbl)
        table.put_item(Item=item)
    except ClientError as e:
        logger.error(e.response["Error"]["Message"])
        raise
    except AssertionError as a:
        logger.error("Table doesn't exist")
        raise


def update_dynamodb_items(
    table: str,
    items: list,
    region: str,
    table_key: dict = None,
):

    ddb_client = boto3.client("dynamodb", region_name=region)

    if table_key is None:
        table_key = get_dynamodb_table_key_dict(table, ddb_client, region)
    item: dict
    for item in items:
        ddb_item = _serialize(item)
        key = _serialize({k: item[k] for k in table_key.keys()})
        expression_attribute_values = {}
        for k, v in ddb_item.items():
            if k not in table_key:
                expression_attribute_values[":" + k] = v
        more_params = {}
        expression_attribute_names = {}
        attributes = []
        for k in item.keys():
            if k in table_key:
                continue
            # if k in reserved_words:
            #     p = ('#'+k, k)
            #     expression_attribute_names[p[0]] = p[1]
            else:
                p = (k, k)
            attributes.append(p)
        if len(expression_attribute_names) > 0:
            more_params["ExpressionAttributeNames"] = expression_attribute_names
        update_expression = "set " + ",".join(
            map(lambda x: f"{x[0]} = :{x[1]}", attributes)
        )
        print(
            f"""update_item(
            TableName={table},
            Key={key},
            UpdateExpression={update_expression},
            ExpressionAttributeValues={expression_attribute_values},
            {more_params}
        )"""
        )
        response = ddb_client.update_item(
            TableName=table,
            Key=key,
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            **more_params,
        )


def _serialize(item: dict) -> dict:
    return {k: serializer.serialize(v) for k, v in item.items()}


def _deserialize(ddb_item: dict) -> dict:
    # return {k: deserializer.deserialize(v) for k,v in ddb_item.items()}
    d = deserializer.deserialize({"M": ddb_item})
    # dynamodb deserializer return ALL numbers as decimal.Decimal (https://github.com/boto/boto3/issues/369)
    for k, v in d.items():
        if isinstance(v, Decimal):
            if v % 1 == 0:
                d[k] = int(v)
    return d


def query_dynamodb(
    partiql_statement: str,
    region: str,
    is_strong_consistency: bool = False,
) -> list:
    """Allows to perform reads and singleton writes on data stored in DynamoDB, using PartiQL

    :param str partiql_statement:
    :param obj ddb_client:
    :return: Return the result of reads and singleton writes on data stored in DynamoDB
    :rtype: dict
    :raises: Exception
    """
    ddb_client = boto3.client("dynamodb", region_name=region)

    try:
        statement = partiql_statement
        response = ddb_client.execute_statement(
            Statement=statement, ConsistentRead=is_strong_consistency
        )
        return response["Items"]
    except Exception as e:
        print("Incorrect partiQL statement")
        raise


def get_dynamodb_table_key_dict(table: str, ddb_client: object, region: str):

    ddb_client = boto3.client("dynamodb", region_name=region)
    response = ddb_client.describe_table(TableName=table)
    result = {}
    for key_schema_record in response["Table"]["KeySchema"]:
        result[key_schema_record["AttributeName"]] = key_schema_record["KeyType"]
    return result
