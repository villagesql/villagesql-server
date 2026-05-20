# Query villagesql.custom_indexes and villagesql.custom_index_columns for the
# current database. index_id is excluded because it is a non-deterministic
# surrogate key.

--disable_query_log
--disable_result_log
# have_debug.inc makes this inc file silently skip on non-debug builds.
# Any test sourcing query_custom_indexes.inc implicitly requires a debug build.
--source include/have_debug.inc
SET SESSION debug='+d,skip_dd_table_access_check';
--enable_result_log

SELECT db_name, table_name, index_name,
       IF(extension_name = '' OR extension_name IS NULL, 'EMPTY', extension_name) AS extension_name,
       IF(extension_version = '' OR extension_version IS NULL, 'EMPTY', extension_version) AS extension_version,
       index_type_name, index_type_parameters
FROM villagesql.custom_indexes
WHERE db_name = DATABASE()
ORDER BY table_name, index_name;

SELECT ci.table_name, ci.index_name,
       cc.key_position,
       IF(cc.column_name = '' OR cc.column_name IS NULL, 'EMPTY', cc.column_name) AS column_name,
       IF(cc.extension_name = '' OR cc.extension_name IS NULL, 'EMPTY', cc.extension_name) AS extension_name,
       IF(cc.extension_version = '' OR cc.extension_version IS NULL, 'EMPTY', cc.extension_version) AS extension_version,
       IF(cc.profile_name = '' OR cc.profile_name IS NULL, 'EMPTY', cc.profile_name) AS profile_name
FROM villagesql.custom_index_columns cc
JOIN villagesql.custom_indexes ci ON cc.index_id = ci.index_id
WHERE ci.db_name = DATABASE()
ORDER BY ci.table_name, ci.index_name, cc.key_position;

--disable_result_log
SET SESSION debug='-d,skip_dd_table_access_check';
--enable_result_log
--enable_query_log
