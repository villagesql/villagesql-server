# query_hidden_tables.inc - Query VillageSQL system tables that are normally hidden
#
# This include file temporarily enables access to hidden system tables
# to allow tests to verify internal state.
#
# Usage:
#   --let $query = SELECT * FROM villagesql.custom_columns WHERE extension_name = 'test'
#   --source include/villagesql/query_hidden_tables.inc
#
# Variables:
#   $query - The SQL query to execute (required)

if (!$query)
{
  --die ERROR: query_hidden_tables.inc requires $query to be set
}

--source include/have_debug.inc

# Enable debug access to skip table access checks
--disable_query_log
SET SESSION debug='+d,skip_dd_table_access_check';
--enable_query_log

# Execute the query
--eval $query

# Disable debug access
--disable_query_log
SET SESSION debug='-d,skip_dd_table_access_check';
--enable_query_log

# Reset variable to avoid pollution
--let $query =
