# compile_extension_expect_fail.inc - Build extension expecting compile failure
#
# Mirrors create_extension_sdk.inc but asserts that compilation fails and that
# the build output contains $expect_fail_pattern.
#
# Usage:
#   --let $extension_name   = my_extension
#   --let $extension_source = $MYSQL_TEST_DIR/.../my_extension.cc
#   --let $expect_fail_pattern = some string that must appear in the error output
#   --source include/villagesql/compile_extension_expect_fail.inc
#
# The include configures and compiles the extension, then:
#   - fails the test if the build unexpectedly succeeds, or
#   - fails the test if $expect_fail_pattern is absent from the build output.
#
# Requirements:
#   - SDK must be built (make sdk)
#   - cmake and make must be available

if (!$extension_name)
{
  --die ERROR: compile_extension_expect_fail.inc requires $extension_name to be set
}

if (!$extension_source)
{
  --die ERROR: compile_extension_expect_fail.inc requires $extension_source to be set
}

if (!$expect_fail_pattern)
{
  --die ERROR: compile_extension_expect_fail.inc requires $expect_fail_pattern to be set
}

--let $build_dir    = `SELECT @@basedir`
--let $sdk_version  = `SELECT @@villagesql_server_version`
--let $sdk_dir      = $build_dir/villagesql-extension-sdk-$sdk_version
--let $ext_build_dir = $MYSQLTEST_VARDIR/tmp/ext_build_$extension_name
--let $cmake_template = $MYSQL_TEST_DIR/suite/villagesql/data/extension_creator/CMakeLists.txt.template

--exec test -d "$sdk_dir" || (echo "ERROR: SDK not found at $sdk_dir. Run 'make sdk' first." && exit 1)

--exec rm -rf $ext_build_dir
--exec mkdir -p $ext_build_dir/src $ext_build_dir/build
--exec cp $extension_source $ext_build_dir/src/extension.cc
--exec printf '%s' '{"name": "$extension_name", "version": "1.0.0"}' > $ext_build_dir/manifest.json
--exec sed 's/@EXTENSION_NAME@/$extension_name/g' $cmake_template > $ext_build_dir/CMakeLists.txt

--exec cd $ext_build_dir/build && cmake -DVillageSQL_SDK_DIR=$sdk_dir -DCMAKE_MODULE_PATH=$sdk_dir/lib/cmake/VillageSQLExtensionFramework -DVSQL_USE_DEV_ABI=ON .. > cmake_output.txt 2>&1 || (cat cmake_output.txt && exit 1)

--exec cd $ext_build_dir/build && make > make_output.txt 2>&1 && { echo "ERROR: Build should have failed with: $expect_fail_pattern"; exit 1; } || grep -q "$expect_fail_pattern" make_output.txt

--exec rm -rf $ext_build_dir

--let $extension_name =
--let $extension_source =
--let $expect_fail_pattern =
