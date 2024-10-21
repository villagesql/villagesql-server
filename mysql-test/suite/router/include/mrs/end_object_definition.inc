--disable_query_log
--disable_result_log
# workaround to force the MRS to do the full schema refresh when the next
# tests starts
INSERT INTO mysql_rest_service_metadata.audit_log(id, table_name,
            changed_by, changed_at) VALUES(1000, '', '', NOW());
COMMIT;
--enable_query_log
--enable_result_log


if ($extra_mrs_router_id)
{
   if ($_mrs_object_definitions > 0)
   {
     --let $mrs_changes_inc=$_mrs_object_definitions
     --source changes/wait_object.inc
   }

   # Default schema has some "options files", add separate
   # counter for it ?
   #--let $mrs_changes_inc=$_mrs_files_definitions
   #--source changes/wait_files.inc
}
