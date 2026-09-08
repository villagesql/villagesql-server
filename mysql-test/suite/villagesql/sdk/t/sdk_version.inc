# Per-ABI helper for sdk_version.test.
#
# Builds the shipped SDK template against one ABI of the extracted SDK, installs
# it, asserts the extension's reported sdk_version matches the server release
# version, then uninstalls and removes the VEB.
#
# Caller sets before sourcing:
#   $sdk_dir       - extracted SDK root (contains template/, include/, ...)
#   $veb_dir       - server VEB directory
#   $abi_label     - human label for output ("stable" / "dev")
#   $abi_build_dir - build subdirectory name (distinct per ABI)
#   $abi_cmake_arg - extra cmake args ("" for stable, -DVSQL_USE_DEV_ABI=ON for dev)

--echo # Build the template against the $abi_label ABI
--exec rm -rf $sdk_dir/template/$abi_build_dir
--exec mkdir -p $sdk_dir/template/$abi_build_dir
# unset LD_PRELOAD so the toolchain sub-build does not run under the LD_PRELOAD'd
# ASan runtime on sanitizer builds; otherwise the uninstrumented system gcc trips
# LeakSanitizer on its own allocations and fails the build (see
# include/villagesql/create_extension_sdk.inc for the full explanation).
--exec cd $sdk_dir/template/$abi_build_dir && unset LD_PRELOAD && cmake $abi_cmake_arg .. > cmake_output.txt 2>&1
--exec cd $sdk_dir/template/$abi_build_dir && unset LD_PRELOAD && make > make_output.txt 2>&1

--file_exists $sdk_dir/template/$abi_build_dir/my_extension.veb
--exec cp $sdk_dir/template/$abi_build_dir/my_extension.veb $veb_dir/

INSTALL EXTENSION my_extension;

# The template is built from this same server build's SDK, so it must report
# this build's exact version (including any -dev pre-release) as its sdk_version.
# The only representational difference is the code-base prefix that
# villagesql_server_version carries (e.g. "mysql-8.4_"), which the SDK's
# vef_version_t has no field for, so we strip just that prefix and compare the
# remainder exactly.
SELECT JSON_UNQUOTE(JSON_EXTRACT(REGISTRATION_JSON, '$.sdk_version')) INTO @reported_sdk_version
FROM INFORMATION_SCHEMA.EXTENSION_REGISTRATION WHERE EXTENSION_NAME = 'my_extension';
--let $reported_sdk_version = `SELECT @reported_sdk_version`
--let $release_version = `SELECT SUBSTRING_INDEX(@@GLOBAL.villagesql_server_version, '_', -1)`
if ($release_version != $reported_sdk_version)
{
  # --die does not interpolate mysqltest variables, so surface the mismatched
  # values with --echo (which does) before failing with a static message.
  --echo # FAIL ($abi_label ABI): extension reported sdk_version '$reported_sdk_version' but the server release version is '$release_version'
  --die Extension sdk_version does not match the server release version it was built from
}
--echo # OK ($abi_label ABI): extension sdk_version matches the server release version

UNINSTALL EXTENSION my_extension;
--exec rm -f $veb_dir/my_extension.veb
