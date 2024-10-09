if (!$KEEP_ROUTER_FILES)
{

if ($extra_mrs_router_id)
{
   --let $_expected_result=0
   # Disable the MRS service and wait for the MRS to notice it
   if (!$cleanup_arg_mrs_skip_service_disable)
   {
    if (!$cleanup_arg_mrs_existing_service)
    {
      --let $cleanup_arg_mrs_existing_service=/index.html
    }
     --let $mrs_modify_enabled=false
     --source config/modify_enable.inc

     # The http-plugin responses with HTML, the mrs hook overwrites
     # http reponse from 404 html to 404 json. Thus after stopping
     # MRS we will receive plain html.
     --let $mrs_client_arg_dispaly=none
     --let $mrs_client_arg_response_type=raw
     --let $mrs_client_arg_path='$cleanup_arg_mrs_existing_service'
     --let $mrs_client_arg_expected_status=404
     --let $mrs_client_arg_wait_until_status=60
     --source mrs_client.inc
   }
}

let $schemas_to_remove=$router_test_schemas;
while($schemas_to_remove)
{
  let $schema=`SELECT TRIM(REPLACE(SUBSTRING_INDEX("$schemas_to_remove", ";", 1),"\n",""))`;
  let $schemas_to_remove=`SELECT TRIM(SUBSTRING("$schemas_to_remove", LENGTH("$schema") + 2))`;

  eval DROP SCHEMA $schema;
}

DROP SCHEMA mysql_rest_service_metadata;
DROP ROLE mysql_rest_service_admin;
DROP ROLE mysql_rest_service_schema_admin;
DROP ROLE mysql_rest_service_meta_provider;
DROP ROLE mysql_rest_service_data_provider;
DROP ROLE mysql_rest_service_dev;

--disable_query_log
--disable_result_log
DROP USER IF EXISTS 'bs_user';
--enable_query_log
--enable_result_log

if ($router_config_to_remove1)
{
  --remove_file $router_config_to_remove1
}

if ($router_config_to_remove2)
{
  --remove_file $router_config_to_remove2
}

if ($router_config_to_remove3)
{
  --remove_file $router_config_to_remove3
}

if ($restart_router_id)
{
--force-rmdir $MYSQL_TMP_DIR/router.$restart_router_id/data/
}

}


if ($KEEP_ROUTER_FILES == 1)
{
let $schemas_to_remove=$router_test_schemas;
while($schemas_to_remove)
{
  let $schema=`SELECT TRIM(REPLACE(SUBSTRING_INDEX("$schemas_to_remove", ";", 1),"\n",""))`;
  let $schemas_to_remove=`SELECT TRIM(SUBSTRING("$schemas_to_remove", LENGTH("$schema") + 2))`;

  --echo DROP SCHEMA $schema;
}
--echo DROP SCHEMA mysql_rest_service_metadata;
--echo DROP ROLE mysql_rest_service_admin;
--echo DROP ROLE mysql_rest_service_schema_admin;
--echo DROP ROLE mysql_rest_service_meta_provider;
--echo DROP ROLE mysql_rest_service_data_provider;
--echo DROP ROLE mysql_rest_service_dev;
}

--let $cleanup_arg_mrs_existing_service=
--let $$cleanup_arg_mrs_skip_service_disable=
