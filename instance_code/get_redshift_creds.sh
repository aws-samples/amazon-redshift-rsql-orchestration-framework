#!/bin/bash

# Required argument: secret_id
if [[ $# -eq 0 ]] ; then
    echo 'Please provide secret id'
    exit 1
fi

# Required argument: region
# default region is us-east-1
REGION=${2:-'us-east-1'}

SECRET=$(echo $(aws secretsmanager get-secret-value --secret-id $1 --query 'SecretString' --output text --region $REGION)| awk '{print substr($0, 2, length($0) - 2)}')

USER=$(echo $SECRET | awk -F'"username":|"db_user_name":' '{print $2}'| cut -d, -f 1 | awk '{gsub(/^[ \t]+| [ \t]+$/,""); print}' | awk '{print substr($0, 2, length($0) - 2)}')

PASSWORD=$(echo $SECRET | awk -F'"password":|"db_password":' '{print $2}'| cut -d, -f 1 | awk '{gsub(/^[ \t]+| [ \t]+$/,""); print}'| awk '{print substr($0, 2, length($0) - 2)}')

DB=$(echo $SECRET | awk -F'"dbname":|"db_dbname":' '{print $2}'| cut -d, -f 1 | awk '{gsub(/^[ \t]+| [ \t]+$/,""); print}' | awk '{print substr($0, 2, length($0) - 2)}')

HOST=$(echo $SECRET |  awk -F'"host":|"db_host_name":' '{print $2}'| cut -d, -f 1 | awk '{gsub(/^[ \t]+| [ \t]+$/,""); print}' | awk '{print substr($0, 2, length($0) - 2)}')


# Refer ::https://docs.aws.amazon.com/redshift/latest/mgmt/rsql-query-tool-getting-started.html
export RSPASSWORD=$PASSWORD
export ODBCINI=~/.odbc.ini
export ODBCSYSINI=/opt/amazon/redshiftodbc/Setup
export AMAZONREDSHIFTODBCINI=/opt/amazon/redshiftodbc/lib/64/amazon.redshiftodbc.ini


