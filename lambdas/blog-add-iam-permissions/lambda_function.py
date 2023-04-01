# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
from typing import Any, Dict
import time

import boto3

ec2 = boto3.client("ec2")
iam = boto3.client("iam")
secret_manager = boto3.client("secretsmanager")


def add_iam_permissions(
    ec2_service_role_arn, instance_id, secret_manager_arn, aws_account_id, aws_region
) -> str:


    rsql_policy_dict = {
        "SecretManagerRead": f"""{{
                        "Version": "2012-10-17",
                        "Statement": [{{
                            "Action": [
                            "secretsmanager:DescribeSecret",
                            "secretsmanager:GetSecretValue",
                            "secretsmanager:ListSecretVersionIds"
                            ],
                            "Effect": "Allow",
                            "Resource": "{secret_manager_arn}"
                            }}]}}""",
        "DynamoDBRead": f"""{{
                        "Version": "2012-10-17",
                        "Statement": [{{
                            "Action": [
                            "dynamodb:BatchGetItem",
                            "dynamodb:BatchWriteItem",
                            "dynamodb:DescribeTable",
                            "dynamodb:GetItem",
                            "dynamodb:GetRecords",
                            "dynamodb:ListTables",
                            "dynamodb:PartiQLInsert",
                            "dynamodb:PartiQLSelect",
                            "dynamodb:PartiQLUpdate",
                            "dynamodb:PutItem",
                            "dynamodb:Query",
                            "dynamodb:Scan",
                            "dynamodb:UpdateItem"
                            ],
                            "Effect": "Allow",
                            "Resource": "arn:aws:dynamodb:{aws_region}:{aws_account_id}:table/rsql*"
                        }}]}}""",
        "StepFunctionRead": f"""{{
                        "Version": "2012-10-17",
                        "Statement": [
                            {{
                            "Action": [
                            "states:DescribeExecution",
                            "states:DescribeStateMachine",
                            "states:DescribeStateMachineForExecution",
                            "states:GetExecutionHistory",
                            "states:ListExecutions",
                            "states:ListStateMachines",
                            "states:SendTaskFailure",
                            "states:SendTaskHeartbeat",
                            "states:SendTaskSuccess",
                            "states:StartExecution",
                            "states:StartSyncExecution"
                            ],
                            "Effect": "Allow",
                            "Resource": "arn:aws:states:{aws_region}:{aws_account_id}:stateMachine:rsql*"
                            }}]}}""",
        "CloudWatchLogsRead": f"""{{
                        "Version": "2012-10-17",
                        "Statement": [
                            {{
                            "Effect": "Allow",
                            "Action": [
                            "logs:CreateLogStream",
                            "logs:DescribeLogGroups",
                            "logs:DescribeLogStreams",
                            "logs:GetLogEvents",
                            "logs:PutLogEvents",
                            "logs:GetLogRecord"
                            ],
                            "Resource": [
                            "arn:aws:logs:{aws_region}:{aws_account_id}:log-group:/*rsql*/:*",
                            "arn:aws:logs:{aws_region}:{aws_account_id}:log-group:*:log-stream:*"
                            ]
                            }}]}} """,
        "SSMInstancePolicy": f"""{{
                        "Version": "2012-10-17",
                        "Statement": [
                            {{
                                "Effect": "Allow",
                                "Action": [
                                    "ssm:DescribeAssociation",
                                    "ssm:GetDeployablePatchSnapshotForInstance",
                                    "ssm:GetDocument",
                                    "ssm:DescribeDocument",
                                    "ssm:GetManifest",
                                    "ssm:GetParameter",
                                    "ssm:GetParameters",
                                    "ssm:ListAssociations",
                                    "ssm:ListInstanceAssociations",
                                    "ssm:PutInventory",
                                    "ssm:PutComplianceItems",
                                    "ssm:PutConfigurePackageResult",
                                    "ssm:UpdateAssociationStatus",
                                    "ssm:UpdateInstanceAssociationStatus",
                                    "ssm:UpdateInstanceInformation"
                                ],
                                "Resource": "*"
                            }},
                            {{
                                "Effect": "Allow",
                                "Action": [
                                    "ssmmessages:CreateControlChannel",
                                    "ssmmessages:CreateDataChannel",
                                    "ssmmessages:OpenControlChannel",
                                    "ssmmessages:OpenDataChannel"
                                ],
                                "Resource": "*"
                            }},
                            {{
                                "Effect": "Allow",
                                "Action": [
                                    "ec2messages:AcknowledgeMessage",
                                    "ec2messages:DeleteMessage",
                                    "ec2messages:FailMessage",
                                    "ec2messages:GetEndpoint",
                                    "ec2messages:GetMessages",
                                    "ec2messages:SendReply"
                                ],
                                "Resource": "*"
                            }}]}} """,
    }

    # print("IAM Policy to be attached : " + rsql_blog_policy)

    if ec2_service_role_arn:
        ec2_role_name = ec2_service_role_arn.split("/")[1]

        for policy in rsql_policy_dict.keys():

            print("IAM Policy to be attached " + rsql_policy_dict[policy])

            iam.put_role_policy(
                RoleName=ec2_role_name,
                PolicyName=policy,
                PolicyDocument=rsql_policy_dict[policy],
            )

        return ec2_service_role_arn
    else:
        print(f"Creating an EC2 Service Role")

        assume_role_policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "ec2.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }

        create_role_response = iam.create_role(
            RoleName="RSQLBlogServiceRole",
            AssumeRolePolicyDocument=json.dumps(assume_role_policy_document),
        )

        role_arn = create_role_response["Role"]["Arn"]

        for policy in rsql_policy_dict.keys():
            iam.put_role_policy(
                RoleName=role_arn.split("/")[1],
                PolicyName = policy,
                PolicyDocument = rsql_policy_dict[policy],
            )
        
        create_instance_profile_response = iam.create_instance_profile(
                    InstanceProfileName='RSQLBlogServiceRole')
        
        
        associate_role_profile_response = iam.add_role_to_instance_profile(
                        InstanceProfileName='RSQLBlogServiceRole',
                        RoleName='RSQLBlogServiceRole')

        # accounting for eventual consistency of IAM Service  
        # https://docs.aws.amazon.com/IAM/latest/UserGuide/troubleshoot_general.html#troubleshoot_general_eventual-consistency
        time.sleep(120)

        get_instance_role_response = iam.get_instance_profile(
            InstanceProfileName = 'RSQLBlogServiceRole'
        )

        instance_profile_arn = get_instance_role_response['InstanceProfile']['Arn']

        iam_attach_response = ec2.associate_iam_instance_profile(
            IamInstanceProfile={
                'Arn': instance_profile_arn,
                'Name': 'RSQLBlogServiceRole'
            },
            InstanceId=instance_id,
        )

        return role_arn


def on_create(
    ec2_service_role_arn: str,
    instance_id: str,
    secret_manager_arn: str,
    aws_account_id: str,
    aws_region: str,
) -> Dict[str, Any]:

    service_role_arn = add_iam_permissions(
        ec2_service_role_arn,
        instance_id,
        secret_manager_arn,
        aws_account_id,
        aws_region,
    )

    return {
        "PhysicalResourceId": instance_id,
        "Data": {"RoleArn": service_role_arn},
    }


def lambda_handler(event, context):

    aws_account_id = context.invoked_function_arn.split(":")[4]
    aws_region = context.invoked_function_arn.split(":")[3]

    print(f"Received event: {event}")

    request_type = event["RequestType"]
    props = event["ResourceProperties"]

    print(f"Received EC2 Properties: {props}")

    instance_id = props["InstanceID"]
    secret_name = props["SecretName"]

    print(f"EC2 Instance ID : {instance_id}")
    print(f"EC2 Instance ID : {secret_name}")

    ec2_response = ec2.describe_instances(InstanceIds=[instance_id])
    print(f"EC2 Describe Instances Response : {ec2_response}")

    secret_manager_response = secret_manager.describe_secret(SecretId=secret_name)
    print(f"Secret Manager Describe  Response : {secret_manager_response}")

    secret_manager_arn = secret_manager_response["ARN"]

    if 'IamInstanceProfile' in ec2_response["Reservations"][0]["Instances"][0]:
        ec2_service_role_arn = ec2_response["Reservations"][0]["Instances"][0][
            "IamInstanceProfile"
        ]["Arn"]

        print(f"EC2 Service Role : {ec2_service_role_arn}")

    else:
        print(
            "IAM Role is not associated with EC2 Instance, Need to create an IAM role"
        )
        ec2_service_role_arn = None

    if request_type == "Create":
        return on_create(
            ec2_service_role_arn,
            instance_id,
            secret_manager_arn,
            aws_account_id,
            aws_region,
        )

    elif request_type == "Update":
        return on_create(
            ec2_service_role_arn,
            instance_id,
            secret_manager_arn,
            aws_account_id,
            aws_region,
        )

    elif request_type == "Delete":
        print(f"No action to be performed for Delete")
        return None

    raise Exception("Invalid request type: %s" % request_type)
