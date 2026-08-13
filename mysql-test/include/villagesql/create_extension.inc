# create_extension.inc - Helper for creating malformed/test VEB files
#
# This include file creates VEB files with arbitrary content for testing error
# handling and security. For building real extensions with C++ code, use
# create_extension_sdk.inc instead.
#
# Usage:
#   --let $extension_name = my_test_ext
#   --let $extension_files = --file manifest.json '{"version": "1.0.0"}' --symlink evil ../../../etc/passwd
#   --source include/villagesql/create_extension.inc
#
# The VEB file will be created and automatically moved to the VEB directory

if (!$extension_name)
{
  --die ERROR: create_extension.inc requires $extension_name to be set
}

if (!$extension_files)
{
  --die ERROR: create_extension.inc requires $extension_files to be set with --file arguments
}

--let $create_veb_script = $MYSQL_TEST_DIR/suite/villagesql/data/extension_creator/create_malformed_veb.sh

# Clean up any existing VEB file
--error 0,1
--remove_file $extension_name.veb

--echo Creating test extension: $extension_name.veb
--replace_result $MYSQLTEST_VARDIR MYSQLTEST_VARDIR
--replace_result $MYSQL_TEST_DIR MYSQL_TEST_DIR
--replace_result $MYSQL_BASEDIR MYSQL_BASEDIR
--exec $create_veb_script $extension_name $extension_files

# Get VEB directory from server and ensure it exists.
--let $veb_dir = `SELECT @@veb_dir`
# --mkdir fails if the directory already exists
--error 0,1
--mkdir $veb_dir

# Move the VEB file to the VEB directory
--move_file $MYSQL_TMP_DIR/$extension_name.veb $veb_dir/$extension_name.veb

# Reset variables to avoid pollution
--let $extension_name =
--let $extension_files =
