# Shared SQL test logic for ABI v5 extension tests.
#
# Expects the caller to have already:
#   - set $veb_dir via --let
#   - built abi_v5_test.veb and copied it to $veb_dir
#
# Does NOT handle cleanup of build artifacts or the VEB file — the caller
# is responsible for those after sourcing this file.

--echo # Install the extension
INSTALL EXTENSION abi_v5_test;

--echo # Verify extension is installed
SELECT extension_name, extension_version FROM INFORMATION_SCHEMA.EXTENSIONS
WHERE extension_name = 'abi_v5_test';

--echo # FAULT_BLOB type: encode/decode round-trip via type-method VDFs (v3 feature)

--echo # Round-trip via type-method syntax (v3: :: syntax works for named VDF types)
SELECT abi_v5_test.FAULT_BLOB::to_string(abi_v5_test.FAULT_BLOB::from_string('OK hello')) AS roundtrip;

--echo # test_result_kind VDF: exercises VEF_RESULT_WARNING vs VEF_RESULT_ERROR

--echo # Normal result
SELECT abi_v5_test.test_result_kind('anything') AS result;

--echo # WARNING result
SELECT abi_v5_test.test_result_kind('WARN something went sideways') AS result;

--echo # ERROR result
--error 3200
SELECT abi_v5_test.test_result_kind('ERROR fatal problem') AS result;

--echo # NULL input -> NULL output (IntResult::set_null)
SELECT abi_v5_test.test_result_kind(NULL) AS result;

--echo # real_double VDF: RealArg, RealResult, .deterministic()

--echo # Doubles the input value
SELECT abi_v5_test.real_double(1.5) AS result;

--echo # NULL input -> NULL output (RealResult::set_null)
SELECT abi_v5_test.real_double(NULL) AS result;

--echo # answer VDF: zero-arity, .no_params()
SELECT abi_v5_test.answer() AS result;

--echo # FAULT_BLOB::hash: TypeHashFunc via type builder .hash()

--echo # Hash used implicitly by COUNT(DISTINCT) on a custom type column
CREATE TABLE t_hash (v FAULT_BLOB);
INSERT INTO t_hash VALUES ('OK a'), ('OK b'), ('OK a');
SELECT COUNT(DISTINCT v) AS distinct_count FROM t_hash;
DROP TABLE t_hash;

--echo # PVEC type, int_sum_all varargs VDF, pvec_norm_sq aggregate VDF

--echo # Varargs: sum any number of INT arguments
SELECT abi_v5_test.int_sum_all(1, 2, 3) AS total;
SELECT abi_v5_test.int_sum_all(10) AS total;
SELECT abi_v5_test.int_sum_all(1, 2, 3, 4, 5) AS total;

--echo # Varargs prerun rejects zero arguments
--error ER_CANT_INITIALIZE_UDF
SELECT abi_v5_test.int_sum_all();

--echo # Varargs prerun rejects non-INT arguments
--error ER_CANT_INITIALIZE_UDF
SELECT abi_v5_test.int_sum_all(1.5);

--echo # Parameterized type: PVEC(N) column stores N int16 elements
CREATE TABLE t1 (
  id INT PRIMARY KEY,
  v PVEC(3)
);

INSERT INTO t1 VALUES (1, '[1,2,3]'), (2, '[4,5,6]'), (3, '[-1,0,1]');

SELECT id, v FROM t1 ORDER BY id;

--echo # Aggregate: per-group sum of squared elements
SELECT id, abi_v5_test.pvec_norm_sq(v) AS norm_sq FROM t1 GROUP BY id ORDER BY id;

--echo # Aggregate: whole-table (1^2+2^2+3^2) + (4^2+5^2+6^2) + ((-1)^2+0^2+1^2) = 93
SELECT abi_v5_test.pvec_norm_sq(v) AS total_norm_sq FROM t1;

--echo # Aggregate with HAVING: exclude groups with norm_sq <= 10
SELECT id, abi_v5_test.pvec_norm_sq(v) AS norm_sq
FROM t1
GROUP BY id
HAVING abi_v5_test.pvec_norm_sq(v) > 10
ORDER BY id;

--echo # Aggregate ORDER BY descending
SELECT id, abi_v5_test.pvec_norm_sq(v) AS norm_sq
FROM t1
GROUP BY id
ORDER BY abi_v5_test.pvec_norm_sq(v) DESC;

--echo # NULL handling: NULL rows are skipped by accumulate; only [3,4] contributes 25
CREATE TABLE t_nulls (
  id INT,
  v PVEC(2)
);
INSERT INTO t_nulls VALUES (1, '[3,4]'), (2, NULL);
SELECT abi_v5_test.pvec_norm_sq(v) AS norm_sq FROM t_nulls;

DROP TABLE t1, t_nulls;

--echo # Protocol 5 features

--echo # on_init ran at INSTALL EXTENSION
SELECT abi_v5_test.did_init() AS result;

--echo # BINARY argument and result
SELECT HEX(abi_v5_test.bin_echo(UNHEX('00FF10'))) AS result;
SELECT abi_v5_test.bin_echo(NULL) IS NULL AS result;

--echo # A STRING argument arrives as utf8mb4, converted from its charset
SELECT abi_v5_test.str_hex(_latin1 0xE9) AS result;

--echo # const_args: the prerun admits a NULL literal, but not every STRING
SELECT abi_v5_test.int_or_null_sum(1, NULL, 2) AS total;
--error ER_CANT_INITIALIZE_UDF
SELECT abi_v5_test.int_or_null_sum(1, 'a');

--echo # bind_and_check_types: the hook rejects a constant string argument
CREATE TABLE t_str (s VARCHAR(8));
INSERT INTO t_str VALUES ('column');
SELECT abi_v5_test.str_no_const(s) AS result FROM t_str;
--error ER_VILLAGESQL_GENERIC_ERROR
SELECT abi_v5_test.str_no_const('literal');
DROP TABLE t_str;

--echo # A VDF that throws returns an error to the statement
--error 3200
SELECT abi_v5_test.throws();

--echo # from_binary and to_binary
CREATE TABLE t_bytes (id INT PRIMARY KEY, v BYTES8);
INSERT INTO t_bytes VALUES (1, 'text'), (2, 0x41FF), (3, _binary 'bin');
SELECT id, HEX(abi_v5_test.BYTES8::to_binary(v)) AS bytes FROM t_bytes
ORDER BY id;
SELECT HEX(abi_v5_test.BYTES8::to_binary(abi_v5_test.BYTES8::from_binary(0x0102)))
  AS roundtrip;
DROP TABLE t_bytes;

--echo # A type without from_binary rejects a value in the binary charset
CREATE TABLE t_blob (v FAULT_BLOB);
--error ER_VILLAGESQL_GENERIC_ERROR
INSERT INTO t_blob VALUES (_binary 'OK binary');
DROP TABLE t_blob;

--echo # A variable-length type stores each value at its own length
CREATE TABLE t_var (id INT PRIMARY KEY, v VARTEXT);
INSERT INTO t_var VALUES (1, 'a'), (2, 'abcdefghij'), (3, 'abc');
SELECT id, v, abi_v5_test.vartext_len(v) AS len FROM t_var ORDER BY id;
DROP TABLE t_var;

--echo # Uninstall extension
UNINSTALL EXTENSION abi_v5_test;

--echo # Verify extension was removed
SELECT extension_name FROM INFORMATION_SCHEMA.EXTENSIONS
WHERE extension_name = 'abi_v5_test';
