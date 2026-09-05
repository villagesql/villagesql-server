# Uninstall vsql_units extension after testing.

--disable_query_log
--disable_result_log

UNINSTALL EXTENSION vsql_units;

--enable_result_log
--enable_query_log
