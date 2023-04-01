#! /bin/bash

if [[ $# -eq 0 ]] ; then
    echo 'Please provide the needed arguments'
    exit 99
fi


# Input Arguments
# $1: step function callback token
# $2: workflow_id
# $3: workflow_execution_id
# $4: rsql_script_name
# $5: instance_id
# $6: secret_id
# $7: complete_log_path
# $8 : audit_ddb_tbl
# $9 : cloudwatch_log_group
# $10 : instance_code_dir
# $11 : aws_region


secret_id=$6

# absolute path
rsql_script=$4

# file name
script_name_arg=$(basename $rsql_script)
instance_code_dir=${10}


# source /home/ec2-user/blog_test/instance_code/get_redshift_creds.sh $6
source $instance_code_dir/get_orch_params.sh $1 $2 $3 $script_name_arg $5 $7 $8 $9 ${11} 


passed_args=$#
echo "Number of args passed to the wrapper script : $passed_args"

if [[ $# -eq 11 ]] ; then
    echo "No input parameters to be passed to the RSQL Script"

    sh +x "$rsql_script" $instance_code_dir $secret_id

    rsqlexitcode=$?
    echo "$rsql_script exited with $rsqlexitcode"

else
    echo "Input parameters to be passed"

    args='$secret_id'
    arg_count=0

    for arg in "$@"
    do
        arg_count=$(($arg_count+1))

        if [[ arg_count -gt 11 ]] ; then
            echo $arg
            args="${args} $arg"
        fi
    done

    echo "Arguments to be passed : $args"
    sh +x "rsql_script" $args

    rsqlexitcode=$?
    echo "$rsql_script exited with $rsqlexitcode"

fi

unset $RSPASSWORD
python3 $instance_code_dir/send_sfn_token.py $token $script_name $rsqlexitcode $log_file_name $workflow_execution_id $audit_ddb_table $log_group $region
exit $rsqlexitcode



