# create_extension_sdk_expect_build_failure.inc - Build extension expecting failure
#
# Mirrors create_extension_sdk.inc but expects the build (make) step to fail.
# The test fails if the build unexpectedly succeeds.
#
# Usage:
#   --let $extension_name = my_extension
#   --let $extension_source = $MYSQL_TEST_DIR/suite/villagesql/std_data/my_extension.cc
#   --let $build_failure_grep = optional string to grep for in build output
#   --source include/villagesql/create_extension_sdk_expect_build_failure.inc
#
# If $build_failure_grep is set, the test also verifies that the build output
# contains the expected string (e.g. a static_assert message).

if (!$extension_name)
{
  --die ERROR: create_extension_sdk_expect_build_failure.inc requires $extension_name to be set
}

if (!$extension_source)
{
  --die ERROR: create_extension_sdk_expect_build_failure.inc requires $extension_source to be set
}

if (!$extension_version)
{
  --let $extension_version = 1.0.0
}

--let $build_dir = `SELECT @@basedir`
# @@villagesql_server_version includes the code base as a prefix, separated
# from the abbreviated version by an underscore (e.g. "mysql-8.4_0.0.5").
# The SDK directory name uses the abbreviated version.
--let $sdk_abbr = `SELECT SUBSTRING_INDEX(@@villagesql_server_version, '_', -1)`
--let $sdk_dir = $build_dir/villagesql-extension-sdk-$sdk_abbr
--let $ext_build_dir = $MYSQLTEST_VARDIR/tmp/ext_build_$extension_name

--exec test -d "$sdk_dir" || (echo "ERROR: SDK not found at $sdk_dir. Run 'make sdk' first." && exit 1)

--exec rm -rf $ext_build_dir
--exec mkdir -p $ext_build_dir/src
--exec mkdir -p $ext_build_dir/build

--exec cp $extension_source $ext_build_dir/src/extension.cc

# Optionally place a shared companion header next to extension.cc so the source
# can #include it.
if ($extension_common_header)
{
  --exec cp $extension_common_header $ext_build_dir/src/
}

--exec printf '%s' '{"name": "$extension_name", "version": "$extension_version"}' > $ext_build_dir/manifest.json

--let $cmake_template = $MYSQL_TEST_DIR/suite/villagesql/data/extension_creator/CMakeLists.txt.template
--exec sed 's/@EXTENSION_NAME@/$extension_name/g' $cmake_template > $ext_build_dir/CMakeLists.txt

--echo Attempting to build $extension_name (expecting build failure)...

# unset LD_PRELOAD so the toolchain sub-build does not run under the
# LD_PRELOAD'd ASan runtime on sanitizer builds; otherwise the uninstrumented
# system gcc trips LeakSanitizer on GCC's own allocations and fails for the
# wrong reason (see create_extension_sdk.inc for the full explanation).
--replace_result $MYSQLTEST_VARDIR MYSQLTEST_VARDIR $build_dir BUILD_DIR
--exec cd $ext_build_dir/build && unset LD_PRELOAD && cmake -DVillageSQL_SDK_DIR=$sdk_dir -DCMAKE_MODULE_PATH=$sdk_dir/lib/cmake/VillageSQLExtensionFramework -DVSQL_USE_DEV_ABI=ON .. > cmake_output.txt 2>&1 || (cat cmake_output.txt && exit 1)

# Run make and capture output; do not fail on non-zero exit.
--exec cd $ext_build_dir/build && unset LD_PRELOAD && make > make_output.txt 2>&1 || true

# Verify the build actually failed (no VEB produced).
--exec test ! -f $ext_build_dir/build/$extension_name.veb || (echo "ERROR: $extension_name.veb was produced; expected build failure" && exit 1)

# If a pattern is specified, verify it appears in the build output.
if ($build_failure_grep)
{
  --exec grep -q "$build_failure_grep" $ext_build_dir/build/make_output.txt || (echo "ERROR: build output missing expected pattern: $build_failure_grep" && cat $ext_build_dir/build/make_output.txt && exit 1)
}

--echo Build failed as expected.

--exec rm -rf $ext_build_dir

--let $extension_name =
--let $extension_version =
--let $extension_source =
--let $extension_common_header =
--let $build_failure_grep =
