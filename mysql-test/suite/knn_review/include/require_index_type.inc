# Skips the test unless the installed vsql_knn_index_test has index type
# $knn_index_type (with profile $knn_profile, which may be empty). Some index
# types exist only in newer versions of the test extension.
#
# Usage, after INSTALL EXTENSION vsql_knn_index_test:
#   --let $knn_index_type = kvec_multi
#   --let $knn_profile = kvec_profile_multi
#   --source suite/knn_review/include/require_index_type.inc

--disable_query_log
--disable_result_log
CREATE TABLE knn_review_probe (v KVECTOR);
--error 0, ER_VILLAGESQL_GENERIC_ERROR
--eval CREATE INDEX i ON knn_review_probe (v $knn_profile) USING EXTENDED(vsql_knn_index_test.$knn_index_type)
--let $knn_have_type = `SELECT $mysql_errno = 0`
DROP TABLE knn_review_probe;
--enable_result_log
--enable_query_log
if (!$knn_have_type) {
  --disable_query_log
  UNINSTALL EXTENSION vsql_knn_index_test;
  --enable_query_log
  --skip vsql_knn_index_test has no index type $knn_index_type
}
