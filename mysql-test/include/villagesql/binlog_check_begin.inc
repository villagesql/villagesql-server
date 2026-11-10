# ==== Purpose ====
#
# Start tracking binlog position for later verification.
# Use with include/villagesql/binlog_check_end.inc
#
# ==== Usage ====
#
# --source include/villagesql/binlog_check_begin.inc
# ... SQL statements to track ...
# --source include/villagesql/binlog_check_end.inc

--source include/have_log_bin.inc

--disable_query_log
# Force binlog rotation to start with a fresh file
# This ensures the test's events are isolated and won't cause rotation mid-test
FLUSH BINARY LOGS;

--let $binlog_file=query_get_value(SHOW BINARY LOG STATUS, File, 1)
--let $binlog_start= query_get_value(SHOW BINARY LOG STATUS, Position, 1)

--enable_query_log
