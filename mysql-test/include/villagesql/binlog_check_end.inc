# ==== Purpose ====
#
# Display binlog events since the position recorded by binlog_check_begin.inc
# and assert that VillageSQL-specific implementation details don't leak into
# the binlog.
#
# ==== Usage ====
#
# --source include/villagesql/binlog_check_begin.inc
# ... SQL statements to track ...
# --source include/villagesql/binlog_check_end.inc
#
# Parameters:
#
#   $binlog_events_assert_no_match
#     Optional pattern that should not appear in binlog output.
#     If not specified, defaults to "villagesql." (with dot) to catch system
#     table references like "villagesql.custom_columns" while allowing
#     extension type names like "vsql_complex.COMPLEX".
#     Matching is case-insensitive (e.g., "villagesql" matches "VillageSQL").
#     Example: --let $binlog_events_assert_no_match= custom_pattern

# Assert binlog file hasn't rotated since binlog_check_begin.inc
--let $binlog_file_current=query_get_value(SHOW BINARY LOG STATUS, File, 1)
if ($binlog_file != $binlog_file_current)
{
  --echo ERROR: Binlog file rotated during test
  --echo Expected: $binlog_file
  --echo Current:  $binlog_file_current
  --die Binlog file rotated between binlog_check_begin and binlog_check_end
}

--let $mask_binlog_filename= 1
if ($binlog_events_assert_no_match == '')
{
  --let $binlog_events_assert_no_match= villagesql.
}
--source include/rpl/deprecated/show_binlog_events.inc
