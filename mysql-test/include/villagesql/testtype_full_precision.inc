# TESTTYPE implementation with full precision (%.17g format)
# This creates a custom type that stores two doubles (real and imaginary)
# and displays them with full double precision (17 significant digits).
#
# Usage:
#   --let $testtype_extension_name = my_extension_name
#   --source include/villagesql/testtype_full_precision.inc
#
# This will create a complete VEB file: $testtype_extension_name.veb
# The type TESTTYPE is registered via VEF with:
# - encode: testtype_from_string
# - decode: testtype_to_string
# - compare: testtype_compare

# Create VEB file with the extension (no install.sql - types registered via VEF)
--let $extension_name = $testtype_extension_name
--let $extension_version = 0.0.1
--let $extension_source = $MYSQL_TEST_DIR/suite/villagesql/std_data/testtype_full.cc
--source include/villagesql/create_extension_sdk.inc
