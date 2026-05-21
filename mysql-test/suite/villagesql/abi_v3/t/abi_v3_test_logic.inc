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

--echo #
--echo # FAULT_BLOB type: encode/decode round-trip via type-method VDFs (v3 feature)
--echo #

--echo # Round-trip via type-method syntax (v3: :: syntax works for named VDF types)
SELECT abi_v3_test.FAULT_BLOB::to_string(abi_v3_test.FAULT_BLOB::from_string('OK hello')) AS roundtrip;

--echo #
--echo # test_result_kind VDF: exercises VEF_RESULT_WARNING vs VEF_RESULT_ERROR
--echo #

--echo # Normal result
SELECT abi_v3_test.test_result_kind('anything') AS result;

--echo # WARNING result
SELECT abi_v3_test.test_result_kind('WARN something went sideways') AS result;

--echo # ERROR result
--error 3200
SELECT abi_v3_test.test_result_kind('ERROR fatal problem') AS result;

--echo # Uninstall extension
UNINSTALL EXTENSION abi_v3_test;

--echo # Verify extension was removed
SELECT extension_name FROM INFORMATION_SCHEMA.EXTENSIONS
WHERE extension_name = 'abi_v3_test';
