# Shared SQL test logic for ABI v1 extension tests.
#
# Expects the caller to have already:
#   - set $veb_dir via --let
#   - built abi_v1_test.veb and copied it to $veb_dir
#
# Does NOT handle cleanup of build artifacts or the VEB file — the caller
# is responsible for those after sourcing this file.

--echo # Install the extension
INSTALL EXTENSION abi_v1_test;

--echo # Verify extension is installed
SELECT extension_name, extension_version FROM INFORMATION_SCHEMA.EXTENSIONS
WHERE extension_name = 'abi_v1_test';

--echo #
--echo # from_string: STRING -> BYTEARRAY callable VDF
--echo #

--echo # Round-trip: from_string then to_string
SELECT abi_v1_test.to_string(abi_v1_test.from_string('hello')) AS roundtrip;

--echo #
--echo # mask: mixed BYTEARRAY + INT parameters
--echo #

--echo # Mask position 0
SELECT abi_v1_test.to_string(abi_v1_test.mask(abi_v1_test.from_string('hello'), 0)) AS result;

--echo # Mask position 1
SELECT abi_v1_test.to_string(abi_v1_test.mask(abi_v1_test.from_string('hello'), 1)) AS result;

--echo # Mask position 7 (last position in 8-byte array)
SELECT abi_v1_test.to_string(abi_v1_test.mask(abi_v1_test.from_string('abcdefgh'), 7)) AS result;

--echo # Out-of-range offset
if ($v1_error_treated_as_warning)
{
  --echo # (v1 behavior: returns NULL with warning)
  SELECT abi_v1_test.to_string(abi_v1_test.mask(abi_v1_test.from_string('hello'), 100)) AS result;
}
if (!$v1_error_treated_as_warning)
{
  --echo # (current behavior: raises an error)
  --error 3200
  SELECT abi_v1_test.to_string(abi_v1_test.mask(abi_v1_test.from_string('hello'), 100)) AS result;
}

--echo #
--echo # NULL handling: each parameter slot independently
--echo #

--echo # NULL BYTEARRAY input
SELECT abi_v1_test.mask(NULL, 0) AS result;

--echo # NULL INT offset
SELECT abi_v1_test.mask(abi_v1_test.from_string('hello'), NULL) AS result;

--echo #
--echo # Same-function chaining against table data
--echo #

CREATE TABLE t (id INT PRIMARY KEY, data BYTEARRAY);
INSERT INTO t VALUES (1, 'password'), (2, 'abcdefgh');

SELECT id,
       abi_v1_test.to_string(
         abi_v1_test.mask(abi_v1_test.mask(data, 0), 1)
       ) AS masked_0_1
FROM t ORDER BY id;

DROP TABLE t;

--echo # Uninstall extension
UNINSTALL EXTENSION abi_v1_test;

--echo # Verify extension was removed
SELECT extension_name FROM INFORMATION_SCHEMA.EXTENSIONS
WHERE extension_name = 'abi_v1_test';
