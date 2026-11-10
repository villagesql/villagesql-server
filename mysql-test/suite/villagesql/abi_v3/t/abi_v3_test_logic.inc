# Shared SQL test logic for ABI v3 extension tests.
#
# Expects the caller to have already:
#   - set $veb_dir via --let
#   - built abi_v3_test.veb and copied it to $veb_dir
#
# Does NOT handle cleanup of build artifacts or the VEB file — the caller
# is responsible for those after sourcing this file.

--echo # Install the extension
INSTALL EXTENSION abi_v3_test;

--echo # Verify extension is installed
SELECT extension_name, extension_version FROM INFORMATION_SCHEMA.EXTENSIONS
WHERE extension_name = 'abi_v3_test';

--echo # FAULT_BLOB type: encode/decode round-trip via type-method VDFs (v3 feature)

--echo # Round-trip via type-method syntax (v3: :: syntax works for named VDF types)
SELECT abi_v3_test.FAULT_BLOB::to_string(abi_v3_test.FAULT_BLOB::from_string('OK hello')) AS roundtrip;

--echo # test_result_kind VDF: exercises VEF_RESULT_WARNING vs VEF_RESULT_ERROR

--echo # Normal result
SELECT abi_v3_test.test_result_kind('anything') AS result;

--echo # WARNING result
SELECT abi_v3_test.test_result_kind('WARN something went sideways') AS result;

--echo # ERROR result
--error 3200
SELECT abi_v3_test.test_result_kind('ERROR fatal problem') AS result;

--echo # NULL input -> NULL output (IntResult::set_null)
SELECT abi_v3_test.test_result_kind(NULL) AS result;

--echo # real_double VDF: RealArg, RealResult, .deterministic()

--echo # Doubles the input value
SELECT abi_v3_test.real_double(1.5) AS result;

--echo # NULL input -> NULL output (RealResult::set_null)
SELECT abi_v3_test.real_double(NULL) AS result;

--echo # answer VDF: zero-arity, .no_params()
SELECT abi_v3_test.answer() AS result;

--echo # FAULT_BLOB::hash: TypeHashFunc via type builder .hash()

--echo # Hash used implicitly by COUNT(DISTINCT) on a custom type column
CREATE TABLE t_hash (v FAULT_BLOB);
INSERT INTO t_hash VALUES ('OK a'), ('OK b'), ('OK a');
SELECT COUNT(DISTINCT v) AS distinct_count FROM t_hash;
DROP TABLE t_hash;

--echo # PVEC type, int_sum_all varargs VDF, pvec_norm_sq aggregate VDF

--echo # Varargs: sum any number of INT arguments
SELECT abi_v3_test.int_sum_all(1, 2, 3) AS total;
SELECT abi_v3_test.int_sum_all(10) AS total;
SELECT abi_v3_test.int_sum_all(1, 2, 3, 4, 5) AS total;

--echo # Varargs prerun rejects zero arguments
--error ER_CANT_INITIALIZE_UDF
SELECT abi_v3_test.int_sum_all();

--echo # Varargs prerun rejects non-INT arguments
--error ER_CANT_INITIALIZE_UDF
SELECT abi_v3_test.int_sum_all(1.5);

--echo # Parameterized type: PVEC(N) column stores N int16 elements
CREATE TABLE t1 (
  id INT PRIMARY KEY,
  v PVEC(3)
);

INSERT INTO t1 VALUES (1, '[1,2,3]'), (2, '[4,5,6]'), (3, '[-1,0,1]');

SELECT id, v FROM t1 ORDER BY id;

--echo # Aggregate: per-group sum of squared elements
SELECT id, abi_v3_test.pvec_norm_sq(v) AS norm_sq FROM t1 GROUP BY id ORDER BY id;

--echo # Aggregate: whole-table (1^2+2^2+3^2) + (4^2+5^2+6^2) + ((-1)^2+0^2+1^2) = 93
SELECT abi_v3_test.pvec_norm_sq(v) AS total_norm_sq FROM t1;

--echo # Aggregate with HAVING: exclude groups with norm_sq <= 10
SELECT id, abi_v3_test.pvec_norm_sq(v) AS norm_sq
FROM t1
GROUP BY id
HAVING abi_v3_test.pvec_norm_sq(v) > 10
ORDER BY id;

--echo # Aggregate ORDER BY descending
SELECT id, abi_v3_test.pvec_norm_sq(v) AS norm_sq
FROM t1
GROUP BY id
ORDER BY abi_v3_test.pvec_norm_sq(v) DESC;

--echo # NULL handling: NULL rows are skipped by accumulate; only [3,4] contributes 25
CREATE TABLE t_nulls (
  id INT,
  v PVEC(2)
);
INSERT INTO t_nulls VALUES (1, '[3,4]'), (2, NULL);
SELECT abi_v3_test.pvec_norm_sq(v) AS norm_sq FROM t_nulls;

DROP TABLE t1, t_nulls;

--echo # Uninstall extension
UNINSTALL EXTENSION abi_v3_test;

--echo # Verify extension was removed
SELECT extension_name FROM INFORMATION_SCHEMA.EXTENSIONS
WHERE extension_name = 'abi_v3_test';
