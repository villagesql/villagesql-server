# include/villagesql/wait_innodb_history_purged.inc
#
# SUMMARY
#
#    Waits until the InnoDB history list is empty
#    (INNODB_METRICS trx_rseg_history_len = 0), i.e. every committed
#    transaction's undo has been purged. Unlike
#    include/wait_innodb_all_purged.inc this does not stop early when an
#    old ReadView blocks purge, so it times out if another connection
#    holds an open transaction/view. Works on release builds.
#
# USAGE
#
#    [let $wait_timeout= N]  # seconds, default 30
#    --source include/villagesql/wait_innodb_history_purged.inc
#

# History is trimmed only every innodb_purge_rseg_truncate_frequency purge
# batches (default 128); trim after every batch while waiting.
--let $_whp_orig_freq= `SELECT @@GLOBAL.innodb_purge_rseg_truncate_frequency`
--disable_query_log
SET GLOBAL innodb_purge_rseg_truncate_frequency = 1;
--enable_query_log

--let $wait_condition= SELECT COUNT = 0 FROM information_schema.INNODB_METRICS WHERE NAME = 'trx_rseg_history_len'
--source include/wait_condition.inc

--disable_query_log
--eval SET GLOBAL innodb_purge_rseg_truncate_frequency = $_whp_orig_freq
--enable_query_log
