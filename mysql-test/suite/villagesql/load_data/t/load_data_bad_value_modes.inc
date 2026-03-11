# Include file: runs LOAD DATA across all mode combinations for a given type.
#
# Required variables set by caller:
#   $col_def                  - full column definition, e.g. DOUBLE or DOUBLE NOT NULL or COMPLEX
#   $type_default             - the intrinsic default value, e.g. 0 or (0,0)
#   $bad_value_file           - path to data file with a bad value row
#   $null_value_file          - path to data file with a NULL row
#   $bad_value_strict_error   - error code for strict+no IGNORE+bad value (0 = no error)
#   $null_notnull_strict_error - error code for strict+no IGNORE+NULL on NOT NULL (0 = no error)

# --- strict, no IGNORE, bad value ---
SET sql_mode = 'STRICT_TRANS_TABLES';
--replace_result $col_def <col_def>
--eval CREATE TABLE t (id INT, val $col_def)
--replace_result $MYSQLTEST_VARDIR MYSQLTEST_VARDIR
if ($bad_value_strict_error) {
  --error $bad_value_strict_error
}
--eval LOAD DATA INFILE '$bad_value_file' INTO TABLE t
SELECT COUNT(*) FROM t;
--replace_regex /val = '[^']*'/val = '<type_default>'/
--eval SELECT COUNT(*) FROM t WHERE val = '$type_default'
SELECT COUNT(*) FROM t WHERE val IS NULL;
--replace_regex /val != '[^']*'/val != '<type_default>'/
--eval SELECT COUNT(*) FROM t WHERE val != '$type_default' AND val IS NOT NULL
DROP TABLE t;

# --- strict, no IGNORE, NULL ---
SET sql_mode = 'STRICT_TRANS_TABLES';
--replace_result $col_def <col_def>
--eval CREATE TABLE t (id INT, val $col_def)
--replace_result $MYSQLTEST_VARDIR MYSQLTEST_VARDIR
if ($null_notnull_strict_error) {
  --error $null_notnull_strict_error
}
--eval LOAD DATA INFILE '$null_value_file' INTO TABLE t
SELECT COUNT(*) FROM t;
--replace_regex /val = '[^']*'/val = '<type_default>'/
--eval SELECT COUNT(*) FROM t WHERE val = '$type_default'
SELECT COUNT(*) FROM t WHERE val IS NULL;
--replace_regex /val != '[^']*'/val != '<type_default>'/
--eval SELECT COUNT(*) FROM t WHERE val != '$type_default' AND val IS NOT NULL
DROP TABLE t;

# --- strict, IGNORE, bad value ---
SET sql_mode = 'STRICT_TRANS_TABLES';
--replace_result $col_def <col_def>
--eval CREATE TABLE t (id INT, val $col_def)
--replace_result $MYSQLTEST_VARDIR MYSQLTEST_VARDIR
--eval LOAD DATA INFILE '$bad_value_file' IGNORE INTO TABLE t
SELECT COUNT(*) FROM t;
--replace_regex /val = '[^']*'/val = '<type_default>'/
--eval SELECT COUNT(*) FROM t WHERE val = '$type_default'
SELECT COUNT(*) FROM t WHERE val IS NULL;
--replace_regex /val != '[^']*'/val != '<type_default>'/
--eval SELECT COUNT(*) FROM t WHERE val != '$type_default' AND val IS NOT NULL
DROP TABLE t;

# --- non-strict, no IGNORE, bad value ---
SET sql_mode = '';
--replace_result $col_def <col_def>
--eval CREATE TABLE t (id INT, val $col_def)
--replace_result $MYSQLTEST_VARDIR MYSQLTEST_VARDIR
--eval LOAD DATA INFILE '$bad_value_file' INTO TABLE t
SELECT COUNT(*) FROM t;
--replace_regex /val = '[^']*'/val = '<type_default>'/
--eval SELECT COUNT(*) FROM t WHERE val = '$type_default'
SELECT COUNT(*) FROM t WHERE val IS NULL;
--replace_regex /val != '[^']*'/val != '<type_default>'/
--eval SELECT COUNT(*) FROM t WHERE val != '$type_default' AND val IS NOT NULL
DROP TABLE t;

# --- non-strict, IGNORE, bad value ---
SET sql_mode = '';
--replace_result $col_def <col_def>
--eval CREATE TABLE t (id INT, val $col_def)
--replace_result $MYSQLTEST_VARDIR MYSQLTEST_VARDIR
--eval LOAD DATA INFILE '$bad_value_file' IGNORE INTO TABLE t
SELECT COUNT(*) FROM t;
--replace_regex /val = '[^']*'/val = '<type_default>'/
--eval SELECT COUNT(*) FROM t WHERE val = '$type_default'
SELECT COUNT(*) FROM t WHERE val IS NULL;
--replace_regex /val != '[^']*'/val != '<type_default>'/
--eval SELECT COUNT(*) FROM t WHERE val != '$type_default' AND val IS NOT NULL
DROP TABLE t;
