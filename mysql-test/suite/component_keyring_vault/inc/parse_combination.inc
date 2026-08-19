--disable_query_log
SET @jsonified_version_suffix = CONCAT('["', REPLACE(@@version_suffix, '-', '","'), '"]');
SET @extraction_path = '$[0]';
--let $extracted_mode_component = `SELECT JSON_UNQUOTE(JSON_EXTRACT(@jsonified_version_suffix, @extraction_path))`
SET @extraction_path = '$[1]';
--let $extracted_version_component = `SELECT JSON_UNQUOTE(JSON_EXTRACT(@jsonified_version_suffix, @extraction_path))`
--enable_query_log

--let $mode_component_valid = 0
if ($extracted_mode_component == "mount")
{
  --let $vault_conf_mount_point_directory =
  --let $mode_component_valid = 1
}
if ($extracted_mode_component == "directory")
{
  --let $vault_conf_mount_point_directory = server
  --let $mode_component_valid = 1
}
if (!$mode_component_valid)
{
  -- die invalid extracted mode component
}

--let $version_component_valid = 0
if ($extracted_version_component == "v1")
{
  --let $vault_conf_mount_point_version = 1
  --let $vault_server_mount_point_version = 1
  --let $version_component_valid = 1
}
if ($extracted_version_component == "v2")
{
  --let $vault_conf_mount_point_version = 2
  --let $vault_server_mount_point_version = 2
  --let $version_component_valid = 1
}
if ($extracted_version_component == "auto_v1")
{
  --let $vault_conf_mount_point_version = AUTO
  --let $vault_server_mount_point_version = 1
  --let $version_component_valid = 1
}
if ($extracted_version_component == "auto_v2")
{
  --let $vault_conf_mount_point_version = AUTO
  --let $vault_server_mount_point_version = 2
  --let $version_component_valid = 1
}
if ($extracted_version_component == "empty_v1")
{
  --let $vault_conf_mount_point_version =
  --let $vault_server_mount_point_version = 1
  --let $version_component_valid = 1
}
if ($extracted_version_component == "empty_v2")
{
  --let $vault_conf_mount_point_version =
  --let $vault_server_mount_point_version = 2
  --let $version_component_valid = 1
}
if (!$version_component_valid)
{
  -- die invalid extracted version component
}

# TODO(villagesql-rebase): test Vault. Nothing here has ever run against a live
# Vault server in VillageSQL, so this skip hides whatever these 33 tests would
# say -- including any breakage the Percona merge caused. Provision a Vault
# server (see the README named below) and run the suite for real at least once
# per rebase, rather than letting the skip stand in for a pass.
#
# No Vault environment at all means nobody asked for this suite -- it was swept
# up by --suite=all -- so skip rather than die. Percona dies here because the
# suite is meant to be run deliberately (MTR_VAULT_VERSION=... ./mtr
# --suite=keyring_vault), where a die is the right way to report a
# misconfigured run. That intent is kept below and in the includes that follow:
# a value that is set but invalid still dies, as does a missing
# MTR_VAULT_ADDRESS or token. Only "no Vault configured" becomes a skip.
if (!$MTR_VAULT_VERSION)
{
  --skip Vault server not configured; see components/keyrings/keyring_vault/README
}

--let $vault_version_valid = 0
if ($MTR_VAULT_VERSION == 1)
{
  --let $vault_version_valid = 1
}
if ($MTR_VAULT_VERSION == 2)
{
  --let $vault_version_valid = 1
}
if (!$vault_version_valid)
{
  -- die invalid MTR_VAULT_VERSION value
}

if ($MTR_VAULT_VERSION == 1)
{
  if ($vault_server_mount_point_version == 2)
  {
    --skip this combination cannot be run on V1 Vault server
  }
}
