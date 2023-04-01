source $1/get_redshift_creds.sh $2
rsql -h $HOST -U $USER -d $DB << EOF



\echo ' ------------------------------- '

\echo `date`
\echo Start Time - $(date +"%T.%N")

\echo '\n ******************* MAIN EXECUTION LOG STARTS FROM HERE ********************* \n'
\echo 'rsql_blog_script_1.sh job started'


begin ;

/* SET QUERY Group */
SET query_group to 'rsql_blog_querygroup';

\echo '\n ----Creating the rsql_blog schema--------- \n'

CREATE SCHEMA if not exists rsql_blog ;

\if :ERROR <> 0
 \echo 'Create Schema Statement Failed with Below Details'
 \echo 'Error Code -'
 \echo :ERRORCODE
 \remark :LAST_ERROR_MESSAGE
 \exit 21
\else
 \echo :ACTIVITYCOUNT
 \remark '\n **** Statement Executed Successfully **** \n'
\endif

commit ;

\echo '\n ********** JOB COMPLETED SUCCESSFULLY *********** \n'
\echo '*******************MAIN EXECUTION LOG ENDS IN HERE ********************* \n'
\exit 0


EOF

rsqlexitcode=$?
echo $(date -u)
echo Completion Time - $(date +"%T.%N")

echo "rsql_blog_script_1.sh job exited with error code $rsqlexitcode"
exit $rsqlexitcode
