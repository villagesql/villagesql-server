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
# Optional:
#   --let $extension_common_header = /path/to/common.h
#     Companion header(s) copied next to extension.cc so the source can
#     #include them. May name several headers separated by spaces.
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

# File operations here use mysqltest built-ins or --perl rather than --exec, so
# no uninstrumented system binary is spawned (see create_extension_sdk.inc for
# the full explanation). Only cmake and make remain as --exec.
--let VSQL_SDK_DIR = $sdk_dir
--perl
  use strict;
  use warnings;
  my $sdk = $ENV{'VSQL_SDK_DIR'};
  die "ERROR: SDK not found at $sdk. Run 'make sdk' first.\n" unless -d $sdk;
EOF

# --mkdir is not recursive, so each level is created separately. --force-rmdir
# fails if the directory is not there, which is the normal case.
--error 0,1
--force-rmdir $ext_build_dir
--mkdir $ext_build_dir
--mkdir $ext_build_dir/src
--mkdir $ext_build_dir/build

--copy_file $extension_source $ext_build_dir/src/extension.cc

--let $cmake_template = $MYSQL_TEST_DIR/suite/villagesql/data/extension_creator/CMakeLists.txt.template

# Place the optional companion header(s) next to extension.cc, write
# manifest.json, and expand the CMakeLists.txt template.
--let VSQL_EXT_BUILD_DIR = $ext_build_dir
--let VSQL_EXT_NAME = $extension_name
--let VSQL_EXT_VERSION = $extension_version
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
  print $mf '{"name": "' . $name . '", "version": "' . $ENV{'VSQL_EXT_VERSION'} . '"}';
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

--echo Attempting to build $extension_name (expecting build failure)...

--replace_result $MYSQLTEST_VARDIR MYSQLTEST_VARDIR $build_dir BUILD_DIR
--exec cd $ext_build_dir/build && unset LD_PRELOAD && cmake -DVillageSQL_SDK_DIR=$sdk_dir -DCMAKE_MODULE_PATH=$sdk_dir/lib/cmake/VillageSQLExtensionFramework -DVSQL_USE_DEV_ABI=ON .. > cmake_output.txt 2>&1 || (cat cmake_output.txt && exit 1)

# Run make and capture output; do not fail on non-zero exit.
--exec cd $ext_build_dir/build && unset LD_PRELOAD && make > make_output.txt 2>&1 || true

# Verify the build actually failed (no VEB produced), and that the build output
# contains $build_failure_grep when one was given.
--let VSQL_BUILD_FAILURE_GREP = $build_failure_grep
--perl
  use strict;
  use warnings;
  my $dir = $ENV{'VSQL_EXT_BUILD_DIR'};
  my $name = $ENV{'VSQL_EXT_NAME'};
  die "ERROR: $name.veb was produced; expected build failure\n"
    if -f "$dir/build/$name.veb";

  my $pattern = $ENV{'VSQL_BUILD_FAILURE_GREP'} // '';
  if ($pattern ne '') {
    my $log = "$dir/build/make_output.txt";
    open(my $fh, '<', $log) or die "ERROR: cannot read $log: $!\n";
    my $output = do { local $/; <$fh> };
    close($fh);
    die "ERROR: build output missing expected pattern: $pattern\n$output"
      if index($output, $pattern) < 0;
  }
EOF

--echo Build failed as expected.

--force-rmdir $ext_build_dir

--let $extension_name =
--let $extension_version =
--let $extension_source =
--let $extension_common_header =
--let $build_failure_grep =
