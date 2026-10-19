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
#   --let $extension_common_header = /path/to/common.h
#     Companion header(s) copied next to extension.cc so the source can
#     #include them. May name several headers separated by spaces, e.g.
#     --let $extension_common_header = /path/a.h /path/b.h
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

# File operations here use mysqltest's built-in commands (--mkdir, --copy_file,
# --force-rmdir, ...) where one exists, and --perl otherwise. Both avoid
# spawning an uninstrumented system binary, which matters on sanitizer builds:
# MTR's safe_process LD_PRELOADs the shared ASan runtime into every child
# process, and that propagates through mysqltest into exec'd commands, so
# binaries like cp and sed get leak-checked at exit and fail the command with
# LSAN_OPTIONS=exitcode=42. Built-ins run in-process, and perl is suppressed in
# mysql-test/lsan.supp.
#
# Only the cmake and make steps below remain as --exec, since they must invoke
# the real toolchain; they strip LD_PRELOAD explicitly.


# Verify SDK exists
--let VSQL_SDK_DIR = $sdk_dir
--perl
  use strict;
  use warnings;
  my $sdk = $ENV{'VSQL_SDK_DIR'};
  die "ERROR: SDK not found at $sdk. Run 'make sdk' first.\n" unless -d $sdk;
  die "ERROR: villagesql_config not found in SDK at $sdk\n"
    unless -f "$sdk/bin/villagesql_config";
EOF

# Create build directory structure.
# --force-rmdir fails if the directory is not there, which
# is the normal case.
--error 0,1
--force-rmdir $ext_build_dir
--mkdir $ext_build_dir
--mkdir $ext_build_dir/src
--mkdir $ext_build_dir/build

# Copy source file
--copy_file $extension_source $ext_build_dir/src/extension.cc

--let $cmake_template = $MYSQL_TEST_DIR/suite/villagesql/data/extension_creator/CMakeLists.txt.template

# Place the optional companion header(s) next to extension.cc so the source can
# #include them, write manifest.json, and expand the CMakeLists.txt template.
# $extension_common_header may name several headers separated by spaces, and is
# unset by consumers that need none; perl handles both cases.
--let VSQL_EXT_BUILD_DIR = $ext_build_dir
--let VSQL_EXT_NAME = $extension_name
--let VSQL_EXT_VERSION = $extension_version
--let VSQL_EXT_MANIFEST_EXTRA = $extension_manifest_extra
--let VSQL_EXT_COMMON_HEADER = $extension_common_header
--let VSQL_CMAKE_TEMPLATE = $cmake_template
--perl
  use strict;
  use warnings;
  use File::Basename;
  use File::Copy;
  my $dir = $ENV{'VSQL_EXT_BUILD_DIR'};
  my $name = $ENV{'VSQL_EXT_NAME'};

  foreach my $header (split(' ', $ENV{'VSQL_EXT_COMMON_HEADER'} // '')) {
    copy($header, "$dir/src/" . basename($header))
      or die "ERROR: failed to copy $header into $dir/src: $!\n";
  }

  open(my $mf, '>', "$dir/manifest.json")
    or die "ERROR: cannot write $dir/manifest.json: $!\n";
  print $mf '{"name": "' . $name . '", "version": "' . $ENV{'VSQL_EXT_VERSION'}
    . '"' . ($ENV{'VSQL_EXT_MANIFEST_EXTRA'} // '') . '}';
  close($mf);

  my $template = $ENV{'VSQL_CMAKE_TEMPLATE'};
  open(my $in, '<', $template) or die "ERROR: cannot read $template: $!\n";
  my $cmake = do { local $/; <$in> };
  close($in);
  $cmake =~ s/\@EXTENSION_NAME\@/$name/g;
  open(my $out, '>', "$dir/CMakeLists.txt")
    or die "ERROR: cannot write $dir/CMakeLists.txt: $!\n";
  print $out $cmake;
  close($out);
EOF

--echo Creating extension $extension_name using SDK...

# Run cmake (see the LD_PRELOAD note above).
--replace_result $MYSQLTEST_VARDIR MYSQLTEST_VARDIR $build_dir BUILD_DIR
--exec cd $ext_build_dir/build && unset LD_PRELOAD && cmake -DVillageSQL_SDK_DIR=$sdk_dir -DCMAKE_MODULE_PATH=$sdk_dir/lib/cmake/VillageSQLExtensionFramework -DVSQL_USE_DEV_ABI=ON .. > cmake_output.txt 2>&1 || (cat cmake_output.txt && exit 1)

# Run make (see the LD_PRELOAD note above).
--replace_result $MYSQLTEST_VARDIR MYSQLTEST_VARDIR
--exec cd $ext_build_dir/build && unset LD_PRELOAD && make > make_output.txt 2>&1 || (cat make_output.txt && exit 1)

# Verify VEB was created
--file_exists $ext_build_dir/build/$extension_name.veb

# Move VEB to server's VEB directory.
--error 0,1
--mkdir $veb_dir

if ($extension_versioned)
{
  --error 0,1
  --remove_file $veb_dir/$extension_name-$extension_version.veb
  --copy_file $ext_build_dir/build/$extension_name.veb $veb_dir/$extension_name-$extension_version.veb
  --echo Created $extension_name-$extension_version.veb
}

if (!$extension_versioned)
{
  --error 0,1
  --remove_file $veb_dir/$extension_name.veb
  --copy_file $ext_build_dir/build/$extension_name.veb $veb_dir/$extension_name.veb
  --echo Created $extension_name.veb
}

# Cleanup build directory
--force-rmdir $ext_build_dir

# Reset variables
--let $extension_name =
--let $extension_version =
--let $extension_source =
--let $extension_manifest_extra =
--let $extension_versioned =
--let $extension_common_header =
