#! /bin/bash

if [[ $# -eq 0 ]] ; then
    echo 'Please provide the needed arguments'
    exit 1
fi

# Input Arguments
# $1: step function callback token
# $2: workflow_id
# $3: workflow_execution_id
# $4: rsql_script_name
# $5: instance_id
# $6: complete_log_file_name
# $7: audit_ddb_tbl
# $8: log_group
# $9: aws_region

token=$1
workflow_id=$2
workflow_execution_id=$3
script_name=$4
instance_id=$5
log_file_name=$6
audit_ddb_table=$7
log_group=$8
region=$9



