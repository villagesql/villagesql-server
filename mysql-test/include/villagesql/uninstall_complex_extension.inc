# Uninstall vsql_complex extension after testing
#
# Usage:
#   Standalone tests:
#     --source include/villagesql/uninstall_complex_extension.inc
#
#   Replication tests:
#     --let $rpl_server_number= N
#     --source include/villagesql/uninstall_complex_extension.inc

--disable_query_log
--disable_result_log

# Switch to the correct server connection if in replication mode
if ($rpl_server_number)
{
  --let $rpl_connection_name= server_$rpl_server_number
  --source include/connection.inc
}

# Uninstall the vsql_complex extension (provides COMPLEX type)
UNINSTALL EXTENSION vsql_complex;

--enable_result_log
--enable_query_log
