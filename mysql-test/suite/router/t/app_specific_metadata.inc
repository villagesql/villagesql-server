--source include/have_router.inc

--let $extra_mrs_router_id=1
--source ../include/predefined_setup/configure_router_mrs_root.inc
--source ../include/schema/basic_schema.sql
--source ../include/schema/functions_schema.sql

--source ../include/mrs/start_object_definition.inc

--echo Create a service with NO metadata defined on the service level
--let $mrs_add_service_path="/svc_no_md"
--let $mrs_add_host_name=""
--source ../include/mrs/service/add.inc

--echo Create a service with metadata defined on the service level
--let $mrs_add_service_path="/svc_md"
--let $mrs_add_host_name=""
--let $mrs_add_service_metadata='{"app_specific_md":"service level"}'
--source ../include/mrs/service/add.inc

--echo Create a schema endpoint with NO metadata defined on the schema level
--let $mrs_add_schema=func_schema
--let $mrs_add_schema_path=/func_schema_no_md
--source ../include/mrs/db_schema/add.inc

--echo # Create a function endpoint with NO metadata defined on the object level
--let $mrs_add_db_object=func_sum
--let $mrs_add_db_object_path=/func_sum_no_md
--let $mrs_add_db_object_type=FUNCTION
--let $mrs_add_db_object_format=ITEM
--source ../include/mrs/db_object/add.inc

--echo # Create a function endpoint with metadata defined on the object level
--let $mrs_add_db_object=func_sum
--let $mrs_add_db_object_path=/func_sum_md
--let $mrs_add_db_object_type=FUNCTION
--let $mrs_add_db_object_format=ITEM
--let $mrs_add_db_object_metadata='{"app_specific_md":"function level"}'
--source ../include/mrs/db_object/add.inc

--echo Create a schema endpoint with metadata defined on the schema level
--let $mrs_add_schema=basic_schema
--let $mrs_add_schema_path=/basic_schema_md
--let $mrs_add_schema_metadata='{"app_specific_md":"schema level"}'
--source ../include/mrs/db_schema/add.inc

--echo # Create a table endpoint with NO metadata defined on the object level
--let $mrs_add_db_object=table1
--let $mrs_add_db_object_path=/tab_no_md
--source ../include/mrs/db_object/add.inc

--echo # Create a table endpoint with metadata defined on the object level
--let $mrs_add_db_object=table1
--let $mrs_add_db_object_path=/tab_md
--let $mrs_add_db_object_metadata='{"app_specific_md":"table level"}'
--source ../include/mrs/db_object/add.inc

--echo # Create a procedure endpoint with NO metadata defined on the object level
--let $mrs_add_db_object=proc_int
--let $mrs_add_db_object_path=/proc_int_no_md
--let $mrs_add_db_object_type=PROCEDURE
--source ../include/mrs/db_object/add.inc

--echo # Create a procedure endpoint with metadata defined on the object level
--let $mrs_add_db_object=proc_int
--let $mrs_add_db_object_path=/proc_int_md
--let $mrs_add_db_object_type=PROCEDURE
--let $mrs_add_db_object_metadata='{"app_specific_md":"procedure level"}'
--source ../include/mrs/db_object/add.inc

--source ../include/mrs/end_object_definition.inc

--let $mrs_host_and_port=127.0.0.1:$HTTP_SERVER_PORT

--let MRS_CLIENT_ARGS=$MRS_CLIENT --url https://$mrs_host_and_port

exec $MRS_CLIENT_ARGS --path /svc_md/basic_schema_md/proc_int_md
    --wait-until-found=60 --display REQUEST;

--echo
--echo Check the _metadata on service level
--echo

--echo Check the _metadata for a service that has it set
--replace_regex /^Date.*/Date -> %DATE%/
exec $MRS_CLIENT_ARGS --path /svc_md/_metadata
    --display REQUEST,HEADER,BODY;

--echo Check the _metadata for a service that has it NULL
--replace_regex /^Date.*/Date -> %DATE%/
exec $MRS_CLIENT_ARGS --path /svc_no_md/_metadata
    --display REQUEST,HEADER,BODY;

--echo Check the forbidden PUT method for service _metadata
exec $MRS_CLIENT_ARGS --path /svc_md/_metadata
    -t PUT --payload '{}'  --expected-status Forbidden;

--echo Check the forbidden POST method for service _metadata
exec $MRS_CLIENT_ARGS --path /svc_md/_metadata
    -t POST --expected-status Forbidden;

--echo Check the forbidden DELETE method for service _metadata
exec $MRS_CLIENT_ARGS --path /svc_md/_metadata
    -t DELETE --expected-status Forbidden;

--echo
--echo Check the _metadata on schema level
--echo

--echo Check the _metadata for a schema that has it set
--replace_regex /^Date.*/Date -> %DATE%/
exec $MRS_CLIENT_ARGS --path /svc_md/basic_schema_md/_metadata
    --display REQUEST,HEADER,BODY;

--echo Check the _metadata for a schema that has it NULL
--replace_regex /^Date.*/Date -> %DATE%/
exec $MRS_CLIENT_ARGS --path /svc_md/func_schema_no_md/_metadata
    --display REQUEST,HEADER,BODY;

--echo Check the forbidden PUT method for schema _metadata
exec $MRS_CLIENT_ARGS --path /svc_md/basic_schema_md/_metadata
    -t PUT --payload '{}'  --expected-status Forbidden;

--echo Check the forbidden POST method for schema _metadata
exec $MRS_CLIENT_ARGS --path /svc_md/basic_schema_md/_metadata
    -t POST --expected-status Forbidden;

--echo Check the forbidden DELETE method for schema _metadata
exec $MRS_CLIENT_ARGS --path /svc_md/basic_schema_md/_metadata
    -t DELETE --expected-status Forbidden;

--echo
--echo Check the _metadata on object level
--echo

--echo Check the _metadata for a FUNCTION object that has it set
--replace_regex /^Date.*/Date -> %DATE%/
exec $MRS_CLIENT_ARGS --path /svc_md/func_schema_no_md/func_sum_md/_metadata
    --display REQUEST,HEADER,BODY;

--echo Check the _metadata for a FUNCTION object that has it NULL
--replace_regex /^Date.*/Date -> %DATE%/
exec $MRS_CLIENT_ARGS --path /svc_md/func_schema_no_md/func_sum_no_md/_metadata
    --display REQUEST,HEADER,BODY;

--echo Check the _metadata for a TABLE object that has it set
--replace_regex /^Date.*/Date -> %DATE%/
exec $MRS_CLIENT_ARGS --path /svc_md/basic_schema_md/tab_md/_metadata
  --display REQUEST,HEADER,BODY;

--echo Check the _metadata for a TABLE object that has it NULL
--replace_regex /^Date.*/Date -> %DATE%/
exec $MRS_CLIENT_ARGS --path /svc_md/basic_schema_md/tab_no_md/_metadata
    --display REQUEST,HEADER,BODY;

--echo Check the _metadata for a PROCEDURE object that has it set
--replace_regex /^Date.*/Date -> %DATE%/
exec $MRS_CLIENT_ARGS --path /svc_md/basic_schema_md/proc_int_md/_metadata
    --display REQUEST,HEADER,BODY;

--echo Check the _metadata for a PROCEDURE object that has it NULL
--replace_regex /^Date.*/Date -> %DATE%/
exec $MRS_CLIENT_ARGS --path /svc_md/basic_schema_md/proc_int_no_md/_metadata
    --display REQUEST,HEADER,BODY;

--echo Check the forbidden PUT method for object _metadata
exec $MRS_CLIENT_ARGS --path /svc_md/basic_schema_md/tab_md/_metadata
    -t PUT --payload '{}'  --expected-status Forbidden;

--echo Check the forbidden POST method for object _metadata
exec $MRS_CLIENT_ARGS --path /svc_md/basic_schema_md/tab_md/_metadata
    -t POST --expected-status Forbidden;

--echo Check the forbidden DELETE method for object _metadata
exec $MRS_CLIENT_ARGS --path /svc_md/basic_schema_md/tab_md/_metadata
    -t DELETE --expected-status Forbidden;

# Cleanup
--source ../include/mrs/cleanup.inc
