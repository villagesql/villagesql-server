# Setup test to use keyring component

# Used environment variables:
# KMS_REGION
# KMS_KEY
# KMS_AUTH_KEY
# KMS_SECRET_ACCESS_KEY

# TODO(villagesql-rebase): test KMS for real. Nothing in this suite has ever run
# against a live AWS KMS endpoint in VillageSQL, so the skip below hides whatever
# these tests would say -- including any breakage a Percona merge caused.
# Provision a KMS key and run the suite properly at least once per rebase rather
# than letting the skip stand in for a pass.
#
# No KMS configured at all means nobody asked for this suite -- it was swept up
# by --suite=all. Unlike keyring_vault, nothing here checked, because there is
# nothing to check at load time: the component initialises fine without
# credentials. The first key operation is an HTTPS call to
# kms.$KMS_REGION.amazonaws.com (backend/kms.cc), so with KMS_REGION unset that
# host is unresolvable and every test needing a master key fails -- six of them
# by crashing a debug server, since InnoDB asserts that a loaded keyring is a
# working one. A region that is set but wrong still fails loudly, which is what
# a deliberately configured run wants.
#
# Tests that only load the component and never ask it for a key set
# $kms_backend_not_required and keep running; see load.test and database.test.
if (!$KMS_REGION) {
  if (!$kms_backend_not_required) {
    --skip AWS KMS is not configured; set KMS_REGION, KMS_KEY, KMS_AUTH_KEY and KMS_SECRET_ACCESS_KEY to run this suite
  }
}

--echo # ----------------------------------------------------------------------
--echo # Setup

--let PLUGIN_DIR_OPT = $KEYRING_KMS_COMPONENT_OPT

# Data directory location
--let CURRENT_DATADIR = `SELECT @@datadir`

--let COMPONENT_LIBRARY = `SELECT SUBSTRING_INDEX('$KEYRING_KMS_COMPONENT_LOAD', '=', -1)`
--let COMPONENT_DIR = $KEYRING_KMS_COMPONENT_DIR
--let COMPONENT_NAME = `SELECT SUBSTRING_INDEX('$COMPONENT_LIBRARY', '.', 1)`
--source include/keyring_tests/helper/global_keyring_create_config.inc

# Create local keyring config
--let KEYRING_KMS_PATH = `SELECT CONCAT( '$MYSQLTEST_VARDIR', '/keyring_kms')`
--let KEYRING_CONFIG_CONTENT = `SELECT CONCAT('{ \"path\": \"', '$KEYRING_KMS_PATH', '\", \"region\": \"', '$KMS_REGION', '\", \"kms_key\": \"', '$KMS_KEY', '\", \"auth_key\": \"', '$KMS_AUTH_KEY', '\", \"secret_access_key\": \"', '$KMS_SECRET_ACCESS_KEY', '\", \"read_only\": false }')`
--source include/keyring_tests/helper/local_keyring_create_config.inc

# Create local manifest file for current server instance
--let LOCAL_MANIFEST_CONTENT = `SELECT CONCAT('{ \"components\": \"file://', '$COMPONENT_NAME', '\" }')`
--source include/keyring_tests/helper/instance_create_manifest.inc

# Restart server with manifest file 
--source include/keyring_tests/helper/start_server_with_manifest.inc
--echo # ----------------------------------------------------------------------

