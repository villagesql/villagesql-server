# Only supported in debug mode
--source include/have_debug.inc

# Enable Debug access
SET SESSION debug='+d,skip_dd_table_access_check';
