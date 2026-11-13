# Query villagesql.custom_columns

# Enable Debug access

--disable_query_log
--disable_result_log
--source include/have_debug.inc
SET SESSION debug='+d,skip_dd_table_access_check';
--enable_result_log
--enable_query_log

SELECT * FROM villagesql.custom_columns;

--disable_query_log
--disable_result_log
SET SESSION debug='-d,skip_dd_table_access_check';
--enable_result_log
--enable_query_log
