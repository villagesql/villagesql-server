# Prints whether $knn_query is planned as a KNN scan (EXPLAIN) and whether
# executing it ran one (the extension's scan counter), and the query's error
# number. The query's rows are not printed.
#
# Usage:
#   --let $knn_query = SELECT ...
#   --source suite/knn_review/include/plan_check.inc

--echo # $knn_query
--disable_query_log
--disable_result_log
--eval EXPLAIN FORMAT=JSON INTO @knn_plan $knn_query
SELECT vsql_knn_index_test.kvec_reset_counters();
--error 0, ER_VILLAGESQL_GENERIC_ERROR, ER_ILLEGAL_HA
--eval $knn_query
--let $knn_errno = $mysql_errno
--enable_result_log
--eval SELECT LOCATE('"index_distance"', @knn_plan) > 0 OR LOCATE('"knn_scan"', @knn_plan) > 0 AS plan_uses_knn, vsql_knn_index_test.kvec_scan_begin_count() > 0 AS knn_scan_ran, $knn_errno AS query_errno
--enable_query_log
