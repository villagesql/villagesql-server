# create_extension_sdk.inc - Build extension using VillageSQL SDK
#
# This include file builds extensions using the SDK workflow, which mirrors
# how external users build extensions. It uses find_package(VillageSQL) and
# VEF_CREATE_VEB() to build the extension.
#
# Usage:
#   --let $extension_name = my_extension
#   --let $extension_version = 1.0.0
#   --let $extension_source = $MYSQL_TEST_DIR/suite/villagesql/std_data/my_extension.cc
#   --source include/villagesql/create_extension_sdk.inc
#
# Optional:
#   --let $extension_manifest_extra = , "description": "My extension"
#   --let $extension_versioned = 1
#     When set, copies to {name}-{version}.veb instead of {name}.veb.
#     Use this when the test needs versioned VEB files on disk (e.g. for
#     INSTALL EXTENSION foo VERSION 'x.y.z').
#
# The VEB file will be created and moved to the VEB directory.
#
# Requirements:
#   - SDK must be built (make sdk)
#   - cmake and make must be available

if (!$extension_name)
{
  --die ERROR: create_extension_sdk.inc requires $extension_name to be set
}

if (!$extension_source)
{
  --die ERROR: create_extension_sdk.inc requires $extension_source to be set
}

# Default version if not specified
if (!$extension_version)
{
  --let $extension_version = 1.0.0
}

# Get paths
--let $build_dir = `SELECT @@basedir`
--let $veb_dir = `SELECT @@veb_dir`
# @@villagesql_server_version includes the code base as a prefix, separated
# from the abbreviated version by an underscore (e.g. "mysql-8.4_0.0.5").
# The SDK directory name uses the abbreviated version.
--let $sdk_abbr = `SELECT SUBSTRING_INDEX(@@villagesql_server_version, '_', -1)`
--let $sdk_dir = $build_dir/villagesql-extension-sdk-$sdk_abbr
--let $ext_build_dir = $MYSQLTEST_VARDIR/tmp/ext_build_$extension_name

# Verify SDK exists
--exec test -d "$sdk_dir" || (echo "ERROR: SDK not found at $sdk_dir. Run 'make sdk' first." && exit 1)
--exec test -f "$sdk_dir/bin/villagesql_config" || (echo "ERROR: villagesql_config not found in SDK" && exit 1)

# Create build directory structure
--exec rm -rf $ext_build_dir
--exec mkdir -p $ext_build_dir/src
--exec mkdir -p $ext_build_dir/build

# Copy source file
--exec cp $extension_source $ext_build_dir/src/extension.cc

# Optionally place a shared companion header next to extension.cc so the source
# can #include it. Gated on $extension_common_header, so other consumers of this
# include (which never set it) are unaffected.
if ($extension_common_header)
{
  --exec cp $extension_common_header $ext_build_dir/src/
}

# Create manifest.json
--exec printf '%s' '{"name": "$extension_name", "version": "$extension_version"$extension_manifest_extra}' > $ext_build_dir/manifest.json

# Create CMakeLists.txt from template
--let $cmake_template = $MYSQL_TEST_DIR/suite/villagesql/data/extension_creator/CMakeLists.txt.template
--exec sed 's/@EXTENSION_NAME@/$extension_name/g' $cmake_template > $ext_build_dir/CMakeLists.txt

--echo Creating extension $extension_name using SDK...

# Run cmake
#
# Strip LD_PRELOAD for the toolchain sub-build. On sanitizer builds MTR's
# safe_process LD_PRELOADs the shared ASan runtime into every child process,
# and that propagates through mysqltest into these exec'd commands. The
# uninstrumented system gcc/cc1 then runs under LeakSanitizer, which flags
# GCC's own internal allocations at exit and fails the build with
# LSAN_OPTIONS=exitcode=42. LD_PRELOAD only loads the runtime into the
# compiler process; it does not add -fsanitize instrumentation, so unsetting
# it yields an identical .veb while avoiding the spurious leak reports.
--replace_result $MYSQLTEST_VARDIR MYSQLTEST_VARDIR $build_dir BUILD_DIR
--exec cd $ext_build_dir/build && unset LD_PRELOAD && cmake -DVillageSQL_SDK_DIR=$sdk_dir -DCMAKE_MODULE_PATH=$sdk_dir/lib/cmake/VillageSQLExtensionFramework -DVSQL_USE_DEV_ABI=ON .. > cmake_output.txt 2>&1 || (cat cmake_output.txt && exit 1)

# Run make (see LD_PRELOAD note above for the cmake step).
--replace_result $MYSQLTEST_VARDIR MYSQLTEST_VARDIR
--exec cd $ext_build_dir/build && unset LD_PRELOAD && make > make_output.txt 2>&1 || (cat make_output.txt && exit 1)

# Verify VEB was created
--file_exists $ext_build_dir/build/$extension_name.veb

# Move VEB to server's VEB directory
--exec mkdir -p $veb_dir

if ($extension_versioned)
{
  --exec cp $ext_build_dir/build/$extension_name.veb $veb_dir/$extension_name-$extension_version.veb
  --echo Created $extension_name-$extension_version.veb
}

if (!$extension_versioned)
{
  --exec cp $ext_build_dir/build/$extension_name.veb $veb_dir/
  --echo Created $extension_name.veb
}

# Cleanup build directory
--exec rm -rf $ext_build_dir

# Reset variables
--let $extension_name =
--let $extension_version =
--let $extension_source =
--let $extension_manifest_extra =
--let $extension_versioned =
--let $extension_common_header =
