--disable_query_log
if (!$PROMETHEUS_EXPORTER_PLUGIN) {
  --skip prometheus_exporter plugin is not built on this platform or not available in the mysql-test environment
}
--enable_query_log
