# Skips the test unless the installed vsql_knn_index_test has function
# $knn_function. Some functions exist only in newer versions of the test
# extension.

--let $knn_have_fn = `SELECT LOCATE('"$knn_function"', REGISTRATION_JSON) > 0 FROM INFORMATION_SCHEMA.EXTENSION_REGISTRATION WHERE EXTENSION_NAME = 'vsql_knn_index_test'`
if (!$knn_have_fn) {
  --disable_query_log
  UNINSTALL EXTENSION vsql_knn_index_test;
  --enable_query_log
  --skip vsql_knn_index_test has no function $knn_function
}
