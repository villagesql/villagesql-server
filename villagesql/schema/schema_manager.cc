/* Copyright (c) 2026 VillageSQL Contributors
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see <https://www.gnu.org/licenses/>.
 */

#include "villagesql/schema/schema_manager.h"

#include "lex_string.h"
#include "my_byteorder.h"
#include "my_inttypes.h"
#include "mysqld_error.h"
#include "scope_guard.h"
#include "sql/auth/auth_common.h"
#include "sql/auth/sql_security_ctx.h"
#include "sql/bootstrap.h"
#include "sql/current_thd.h"
#include "sql/dd/cache/dictionary_client.h"
#include "sql/dd/dd.h"
#include "sql/dd/dictionary.h"
#include "sql/dd/impl/bootstrap/bootstrap_ctx.h"
#include "sql/dd/impl/dictionary_impl.h"
#include "sql/dd/impl/upgrade/server.h"
#include "sql/dd/impl/utils.h"
#include "sql/dd/string_type.h"
#include "sql/dd/types/abstract_table.h"
#include "sql/dd/types/schema.h"
#include "sql/dd/types/table.h"
#include "sql/dd/upgrade/server.h"
#include "sql/derror.h"
#include "sql/handler.h"
#include "sql/mysqld.h"
#include "sql/opt_costconstantcache.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/statement/ed_connection.h"
#include "sql/table.h"
#include "sql/thd_raii.h"
#include "sql/transaction.h"
#include "villagesql/include/error.h"
#include "villagesql/include/version.h"
#include "villagesql/schema/systable/helpers.h"
#include "villagesql/schema/system_table_check.h"
#include "villagesql/schema/upgrade.h"
#include "villagesql/schema/victionary_client.h"

// External declaration for VillageSQL schema commands
extern const char *villagesql_schema[];

namespace villagesql {

// Static initializations
const char *SchemaManager::VILLAGESQL_SCHEMA_NAME = "villagesql";
const char *SchemaManager::PROPERTIES_TABLE_NAME = "properties";
const char *SchemaManager::COLUMNS_TABLE_NAME = "custom_columns";
const char *SchemaManager::SP_PARAMS_TABLE_NAME = "custom_sp_params";
const char *SchemaManager::EXTENSIONS_TABLE_NAME = "extensions";
const char *SchemaManager::INDEXES_TABLE_NAME = "custom_indexes";
const char *SchemaManager::INDEX_COLUMNS_TABLE_NAME = "custom_index_columns";

// Define expected structures for all VillageSQL system tables
namespace {

// Represents the internal state of the status manager. It is not exposed in
// the header file to make it easier to reason about the API available
// elsewhere.
class SchemaManagerStatus {
 public:
  /**
   * Read the version of the VillageSQL schema from memory. Only valid
   * after preinit().
   *
   * @return the version of VillageSQL installed.
   */
  static Semver get_version() {
    assert(version);
    return *version;
  }

  /**
   * Was a villagesql upgraded needed when starting up. Only valid after
   * preinit().
   */
  static bool get_upgrade_needed() {
    assert(upgrade_needed);
    return *upgrade_needed;
  }

  /**
   * Record the version of the VillageSQL Schema installed in memory.  This
   * must only be called between preinit() and init().
   *
   * @param[in]  version  The Semver of the schema installed
   */
  static void set_version(const Semver &version);

  /**
   * Read current VillageSQL version from villagesql.properties table.
   * It is not an error if the tables or rows are not present, in these cases
   * version->is_valid() will return false.
   *
   * @param[in]  thd      Thread handle.
   * @param[out] version  Current VillageSQL version stored in the database.
   *
   * @retval false  ON SUCCESS
   * @retval true   ON FAILURE
   */
  static bool read_villagesql_version(THD *thd, Semver *version);

  /**
   * Set VillageSQL version in villagesql.properties table.
   *
   * @param[in]  thd      Thread handle.
   * @param[in]  version  VillageSQL version to store.
   *
   * @retval false  ON SUCCESS
   * @retval true   ON FAILURE
   */
  static bool write_villagesql_version(THD *thd, const Semver &version);

  /**
   * Free resources allocated by SchemaManagerStatus. Called during shutdown.
   */
  static void deinit() {
    delete version;
    version = nullptr;
    delete upgrade_needed;
    upgrade_needed = nullptr;
  }

  // Track initialization state to prevent double-init and to know
  // if the subsystem is ready (same pattern as component system)
  // This is true only when we have fully initialized
  static std::atomic<bool> is_initialized;
  // This is true during the initialzation process itself
  static std::atomic<bool> is_initializing;
  // This is the version of the schema currently applied to the database.
  // The pointer is never null after pre_init(), and is not changed after
  // init(). Memory is managed via set_version() which handles
  // allocation/deallocation.
  static Semver *version;
  // Set during preinit() never changed after that.
  static bool *upgrade_needed;
};

std::atomic<bool> SchemaManagerStatus::is_initialized = false;
std::atomic<bool> SchemaManagerStatus::is_initializing = false;
Semver *SchemaManagerStatus::version = nullptr;
bool *SchemaManagerStatus::upgrade_needed = nullptr;

// Keep these table definitions in sync with
// villagesql/schema/villagesql_schema.sql Any changes to the schema must be
// reflected here for validation to work correctly.
// TODO(villagesql): Consider generating these definitions from the SQL schema
// file in the future.

// Structure to hold table validation info
struct VillageSQL_table {
  const char *table_name;
  const TABLE_FIELD_DEF *expected_def;
};

// Define expected structure for properties table
static const TABLE_FIELD_TYPE properties_fields[] = {
    {{STRING_WITH_LEN("name")},
     {STRING_WITH_LEN("varchar(64)")},
     {STRING_WITH_LEN("utf8mb4")}},
    {{STRING_WITH_LEN("value")},
     {STRING_WITH_LEN("longtext")},
     {STRING_WITH_LEN("utf8mb4")}},
    {{STRING_WITH_LEN("description")},
     {STRING_WITH_LEN("text")},
     {STRING_WITH_LEN("utf8mb4")}}};
static const TABLE_FIELD_DEF properties_def = {3, properties_fields};

// Define expected structure for properties table
static const TABLE_FIELD_TYPE columns_fields[] = {
    {{STRING_WITH_LEN("db_name")},
     {STRING_WITH_LEN("varchar(64)")},
     {STRING_WITH_LEN("utf8mb4")}},
    {{STRING_WITH_LEN("table_name")},
     {STRING_WITH_LEN("varchar(64)")},
     {STRING_WITH_LEN("utf8mb4")}},
    {{STRING_WITH_LEN("column_name")},
     {STRING_WITH_LEN("varchar(64)")},
     {STRING_WITH_LEN("utf8mb4")}},
    {{STRING_WITH_LEN("extension_name")},
     {STRING_WITH_LEN("varchar(64)")},
     {STRING_WITH_LEN("utf8mb4")}},
    {{STRING_WITH_LEN("extension_version")},
     {STRING_WITH_LEN("varchar(64)")},
     {STRING_WITH_LEN("utf8mb4")}},
    {{STRING_WITH_LEN("type_name")},
     {STRING_WITH_LEN("varchar(64)")},
     {STRING_WITH_LEN("utf8mb4")}},
    {{STRING_WITH_LEN("type_parameters")},
     {STRING_WITH_LEN("json")},
     {nullptr, 0}}};
static const TABLE_FIELD_DEF columns_def = {7, columns_fields};

// Define expected structure for custom_sp_params table
static const TABLE_FIELD_TYPE sp_params_fields[] = {
    {{STRING_WITH_LEN("db_name")},
     {STRING_WITH_LEN("varchar(64)")},
     {STRING_WITH_LEN("utf8mb4")}},
    {{STRING_WITH_LEN("sp_name")},
     {STRING_WITH_LEN("varchar(64)")},
     {STRING_WITH_LEN("utf8mb4")}},
    {{STRING_WITH_LEN("param_name")},
     {STRING_WITH_LEN("varchar(64)")},
     {STRING_WITH_LEN("utf8mb4")}},
    {{STRING_WITH_LEN("extension_name")},
     {STRING_WITH_LEN("varchar(64)")},
     {STRING_WITH_LEN("utf8mb4")}},
    {{STRING_WITH_LEN("extension_version")},
     {STRING_WITH_LEN("varchar(64)")},
     {STRING_WITH_LEN("utf8mb4")}},
    {{STRING_WITH_LEN("type_name")},
     {STRING_WITH_LEN("varchar(64)")},
     {STRING_WITH_LEN("utf8mb4")}},
    {{STRING_WITH_LEN("type_parameters")},
     {STRING_WITH_LEN("json")},
     {nullptr, 0}}};
static const TABLE_FIELD_DEF sp_params_def = {7, sp_params_fields};

// Define expected structure for extensions table
static const TABLE_FIELD_TYPE extensions_fields[] = {
    {{STRING_WITH_LEN("extension_name")},
     {STRING_WITH_LEN("varchar(64)")},
     {STRING_WITH_LEN("utf8mb4")}},
    {{STRING_WITH_LEN("extension_version")},
     {STRING_WITH_LEN("varchar(64)")},
     {STRING_WITH_LEN("utf8mb4")}},
    {{STRING_WITH_LEN("veb_sha256")},
     {STRING_WITH_LEN("varchar(64)")},
     {STRING_WITH_LEN("utf8mb4")}},
    {{STRING_WITH_LEN("pending_action")},
     {STRING_WITH_LEN("json")},
     {nullptr, 0}}};
static const TABLE_FIELD_DEF extensions_def = {4, extensions_fields};

// Define expected structure for custom_indexes table
static const TABLE_FIELD_TYPE custom_indexes_fields[] = {
    {{STRING_WITH_LEN("index_id")},
     {STRING_WITH_LEN("bigint unsigned")},
     {nullptr, 0}},
    {{STRING_WITH_LEN("db_name")},
     {STRING_WITH_LEN("varchar(64)")},
     {STRING_WITH_LEN("utf8mb4")}},
    {{STRING_WITH_LEN("table_name")},
     {STRING_WITH_LEN("varchar(64)")},
     {STRING_WITH_LEN("utf8mb4")}},
    {{STRING_WITH_LEN("index_name")},
     {STRING_WITH_LEN("varchar(64)")},
     {STRING_WITH_LEN("utf8mb4")}},
    {{STRING_WITH_LEN("extension_name")},
     {STRING_WITH_LEN("varchar(64)")},
     {STRING_WITH_LEN("utf8mb4")}},
    {{STRING_WITH_LEN("extension_version")},
     {STRING_WITH_LEN("varchar(64)")},
     {STRING_WITH_LEN("utf8mb4")}},
    {{STRING_WITH_LEN("index_type_name")},
     {STRING_WITH_LEN("varchar(64)")},
     {STRING_WITH_LEN("utf8mb4")}},
    {{STRING_WITH_LEN("index_type_parameters")},
     {STRING_WITH_LEN("json")},
     {nullptr, 0}}};
static const TABLE_FIELD_DEF custom_indexes_def = {8, custom_indexes_fields};

// Define expected structure for custom_index_columns table
static const TABLE_FIELD_TYPE custom_index_columns_fields[] = {
    {{STRING_WITH_LEN("index_id")},
     {STRING_WITH_LEN("bigint unsigned")},
     {nullptr, 0}},
    {{STRING_WITH_LEN("key_position")},
     {STRING_WITH_LEN("int unsigned")},
     {nullptr, 0}},
    {{STRING_WITH_LEN("column_name")},
     {STRING_WITH_LEN("varchar(64)")},
     {STRING_WITH_LEN("utf8mb4")}},
    {{STRING_WITH_LEN("extension_name")},
     {STRING_WITH_LEN("varchar(64)")},
     {STRING_WITH_LEN("utf8mb4")}},
    {{STRING_WITH_LEN("extension_version")},
     {STRING_WITH_LEN("varchar(64)")},
     {STRING_WITH_LEN("utf8mb4")}},
    {{STRING_WITH_LEN("profile_name")},
     {STRING_WITH_LEN("varchar(64)")},
     {STRING_WITH_LEN("utf8mb4")}}};
static const TABLE_FIELD_DEF custom_index_columns_def = {
    6, custom_index_columns_fields};

// List of all VillageSQL tables to validate and mark as hidden.
static const VillageSQL_table tables_to_check[] = {
    {SchemaManager::PROPERTIES_TABLE_NAME, &properties_def},
    {SchemaManager::COLUMNS_TABLE_NAME, &columns_def},
    {SchemaManager::SP_PARAMS_TABLE_NAME, &sp_params_def},
    {SchemaManager::EXTENSIONS_TABLE_NAME, &extensions_def},
    {SchemaManager::INDEXES_TABLE_NAME, &custom_indexes_def},
    {SchemaManager::INDEX_COLUMNS_TABLE_NAME, &custom_index_columns_def}};

/**
 * Validate all VillageSQL system tables exist and have the correct structure
 *
 * @param thd Current thread execution context
 * @return Status of validation
 * @retval false All tables valid
 * @retval true Validation failed
 */
static bool validate_villagesql_tables(THD *thd) {
  // Check each table
  for (const auto &table_info : tables_to_check) {
    Table_ref tables(SchemaManager::VILLAGESQL_SCHEMA_NAME,
                     table_info.table_name, TL_READ);

    // DIFFERENCE FROM COMPONENTS: ACL checks
    // Components check access permissions before opening tables. This ensures
    // proper security even during initialization. They check if ACL is enabled
    // (!opt_noacl) and if user has required privileges.
    if (SchemaManager::initialized() && !opt_noacl &&
        check_one_table_access(thd, SELECT_ACL, &tables)) {
      return true;
    }

    TABLE *table = nullptr;

    // Attempt to open the table
    if (!(table =
              open_ltable(thd, &tables, TL_READ, MYSQL_LOCK_IGNORE_TIMEOUT))) {
      // DIFFERENCE FROM COMPONENTS: Error handling philosophy
      // Components handle missing tables gracefully - they issue a warning and
      // return SUCCESS (false), allowing MySQL to start without the component
      // subsystem. We currently FAIL if VillageSQL tables don't exist, making
      // VillageSQL a hard requirement for server startup.
      LogVSQL(ERROR_LEVEL, "Cannot open %s.%s table - schema may not exist",
              tables.db, tables.table_name);
      return true;
    }

    // Use scope_guard to ensure cleanup happens even if exceptions occur
    // or early returns are added later. This follows the same pattern as
    // the component system.
    auto guard =
        create_scope_guard([&thd]() { commit_and_close_mysql_tables(thd); });

    // Validate table structure matches expected schema to catch corruption
    // or version mismatches early (same pattern as component system)
    Village_system_table_intact table_intact(thd);

    if (table_intact.check_with_collation(thd, table,
                                          table_info.expected_def)) {
      // Table structure doesn't match expected schema
      LogVSQL(ERROR_LEVEL, "%s table structure is incorrect",
              table_info.table_name);
      return true;
    }

    LogVSQL(INFORMATION_LEVEL, "Verified table %s.%s",
            SchemaManager::VILLAGESQL_SCHEMA_NAME, table_info.table_name);
  }

  return false;
}

// Run version-specific VillageSQL upgrades, which are in
// villagesql/schema/upgrade.h.
bool run_villagesql_version_upgrades(THD *thd, Semver from_version) {
  // Upgrade from 0.0.1 to 0.0.3: add type_parameters column to custom_columns
  // Build the threshold with the same code base as from_version so the
  // comparison is ordered (versions with differing code bases are unordered).
  Semver version_003;
  version_003.from_components(0, 0, 3, from_version.code_base());
  if (from_version < version_003) {
    // Upgrade from 0.0.1 to 0.0.3: add type_parameters column to
    // custom_columns.
    if (upgrade::upgrade_villagesql_from_0_0_1_to_0_0_3(thd)) return true;
  }
  // Upgrade from 0.0.4 to 0.0.5: add pending_action column to extensions
  Semver version_005;
  version_005.from_components(0, 0, 5, from_version.code_base());
  if (from_version < version_005) {
    if (upgrade::upgrade_villagesql_from_0_0_4_to_0_0_5(thd)) return true;
  }
  // Upgrade from 0.0.5 to 0.0.6: convert system tables to utf8mb4_bin and
  // disable persistent InnoDB statistics on the system tables so the background
  // stats thread stops taking MDL on them.
  Semver version_006;
  version_006.from_components(0, 0, 6, from_version.code_base());
  if (from_version < version_006) {
    if (upgrade::upgrade_villagesql_from_0_0_5_to_0_0_6(thd)) return true;
  }
  // Future versions would be added here

  // Write the current build version so the stored version matches the binary
  if (SchemaManagerStatus::write_villagesql_version(thd, GetBuildVersion()))
    return true;
  return false;
}

static bool do_mark_tables_as_hidden(THD *thd) {
  MDL_ticket *mdl;
  MDL_request mdl_request;
  MDL_REQUEST_INIT(&mdl_request, MDL_key::SCHEMA,
                   SchemaManager::VILLAGESQL_SCHEMA_NAME, "",
                   MDL_INTENTION_EXCLUSIVE, MDL_TRANSACTION);

  if (thd->mdl_context.try_acquire_lock(&mdl_request)) {
    LogVSQL(ERROR_LEVEL, "Failed to acquire exclusive lock for %s",
            SchemaManager::VILLAGESQL_SCHEMA_NAME);
    return true;
  }

  for (const auto &table_info : tables_to_check) {
    if (dd::acquire_exclusive_table_mdl(thd,
                                        SchemaManager::VILLAGESQL_SCHEMA_NAME,
                                        table_info.table_name, true, &mdl)) {
      LogVSQL(ERROR_LEVEL, "Failed to acquire exclusive lock for table %s.%s",
              SchemaManager::VILLAGESQL_SCHEMA_NAME, table_info.table_name);
      return true;
    }

    dd::Table *table_def = nullptr;
    if (thd->dd_client()->acquire_for_modification<dd::Table>(
            SchemaManager::VILLAGESQL_SCHEMA_NAME, table_info.table_name,
            &table_def)) {
      LogVSQL(ERROR_LEVEL, "Failed to acquire table %s.%s for modification",
              SchemaManager::VILLAGESQL_SCHEMA_NAME, table_info.table_name);
      return true;
    }

    if (table_def == nullptr) {
      LogVSQL(ERROR_LEVEL, "Table %s.%s does not exist in DD",
              SchemaManager::VILLAGESQL_SCHEMA_NAME, table_info.table_name);
      return true;
    }

    // Set hidden status
    if (table_def->hidden() != dd::Abstract_table::HT_HIDDEN_SYSTEM) {
      table_def->set_hidden(dd::Abstract_table::HT_HIDDEN_SYSTEM);

      if (thd->dd_client()->update(table_def)) {
        LogVSQL(ERROR_LEVEL, "Failed to update hidden status for %s.%s",
                SchemaManager::VILLAGESQL_SCHEMA_NAME, table_info.table_name);
        return true;
      }

      LogVSQL(INFORMATION_LEVEL, "Marked %s.%s as HT_HIDDEN_SYSTEM",
              SchemaManager::VILLAGESQL_SCHEMA_NAME, table_info.table_name);
    }
  }

  return false;
}

/**
 * Mark all VillageSQL system tables as HT_HIDDEN_SYSTEM.
 *
 * This function should be called in a bootstrap thread where MDL requirements
 * are relaxed. It iterates through all tables in tables_to_check and marks them
 * as hidden system tables.
 *
 * @param thd Current thread execution context
 * @return Status of operation
 * @retval false All tables marked as hidden successfully
 * @retval true Failed to mark one or more tables as hidden
 */
static bool mark_tables_as_hidden(THD *thd) {
  const Disable_autocommit_guard autocommit_guard(thd);
  const dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());

  if (do_mark_tables_as_hidden(thd)) {
    trans_rollback_stmt(thd);
    // Full rollback in case we have THD::transaction_rollback_request.
    trans_rollback(thd);
    thd->mdl_context.release_transactional_locks();
    thd->mdl_context.release_statement_locks();
    return true;
  }

  const bool ret = trans_commit_stmt(thd) || trans_commit(thd);
  thd->mdl_context.release_transactional_locks();
  thd->mdl_context.release_statement_locks();
  return ret;
}

void clear_table_caches(THD *thd) {
  // During server startup, dd::reset_tables_and_tablespaces is called, which
  // calls innobase_dict_cache_reset_tables_and_tablespaces. This tries to clear
  // the open tables cache, which causes an assert. So we force close
  // everything, just like the code in upgrade_system_schemas.
  close_thread_tables(thd);
  close_cached_tables(nullptr, nullptr, false, LONG_TIMEOUT);
}

bool preinit_for_restart(THD *thd) {
  assert(!SchemaManager::initialized());

  if (!thd) {
    LogVSQL(ERROR_LEVEL, "No THD context available for pre-initialization");
    return true;
  }

  // Since this is called from a bootstrap thread on restart, we need to have it
  // handle errors the same way. If we don't this, some corner case scenarios
  // that cause get_villagesql_version to fail will not set the error status
  // properly in the DA.
  dd::upgrade::Bootstrap_error_handler error_handler;

  // Get current VillageSQL version from villagesql.properties table
  Semver current_villagesql_version;
  if (SchemaManagerStatus::read_villagesql_version(
          thd, &current_villagesql_version)) {
    return true;  // Error reading version
  }

  SchemaManagerStatus::set_version(current_villagesql_version);
  clear_table_caches(thd);

  return false;
}

}  // anonymous namespace

bool SchemaManager::preinit(dd::enum_dd_init_type dd_init) {
  assert(!SchemaManagerStatus::is_initialized.load() &&
         !SchemaManagerStatus::is_initializing.load());

  if (dd_init != dd::enum_dd_init_type::DD_INITIALIZE) {
    return ::bootstrap::run_bootstrap_thread(
        nullptr, nullptr, &preinit_for_restart, SYSTEM_THREAD_DD_INITIALIZE);
  }

  return false;
}

bool SchemaManager::bootstrap(THD *thd) {
  if (!thd) {
    LogVSQL(ERROR_LEVEL, "Missing THD context available for initialization");
    return true;
  }

  // Get current VillageSQL version from villagesql.properties table
  Semver current_villagesql_version;
  if (SchemaManagerStatus::read_villagesql_version(
          thd, &current_villagesql_version)) {
    return true;  // Error reading version
  }

  if (current_villagesql_version.is_valid()) {
    // Record the actual version
    SchemaManagerStatus::set_version(current_villagesql_version);
  }

  return init(thd);
}

bool SchemaManager::init(THD *thd) {
  if (!thd) {
    LogVSQL(ERROR_LEVEL, "Missing THD context available for initialization");
    return true;
  }

  // Check if already initialized to prevent double initialization
  if (SchemaManagerStatus::is_initialized.load() ||
      SchemaManagerStatus::is_initializing.exchange(true)) {
    LogVSQL(ERROR_LEVEL, "SchemaManager initialized twice!");
    return true;
  }

  LogVSQL(SYSTEM_LEVEL, "Validating system schema...");

  // Validate all VillageSQL system tables
  if (validate_villagesql_tables(thd)) {
    LogVSQL(ERROR_LEVEL, "Failed to validate system schema");
    return true;
  }

  if (mark_tables_as_hidden(thd)) {
    return true;
  }

  // Initialize VictionaryClient
  LogVSQL(INFORMATION_LEVEL, "Initializing VictionaryClient");
  if (VictionaryClient::instance().init(thd)) {
    LogVSQL(ERROR_LEVEL, "Failed to initialize/load VictionaryClient");
    return true;
  }

  SchemaManagerStatus::is_initialized.store(true);
  SchemaManagerStatus::is_initializing.store(false);

  LogVSQL(SYSTEM_LEVEL, "System schema verification completed successfully");
  return false;
}

bool SchemaManager::initialized() {
  return SchemaManagerStatus::is_initialized.load();
}

namespace {

bool install_villagesql_schema(THD *thd) {
  assert(!SchemaManagerStatus::get_version().is_valid());

  // Incomplete installation or first time running VillageSQL with a restart -
  // if this is the first time installing villagesql with --initialize, then
  // the system tables will be created towards the end of
  // --initialize, when sql_initialize.cc processes all of the schema files.
  LogVSQL(SYSTEM_LEVEL,
          "First time run (or incomplete system tables) detected, applying "
          "schema");

  // Run schema creation first (safe with IF NOT EXISTS and ON DUPLICATE KEY
  // UPDATE)
  const char **query_ptr;
  for (query_ptr = &::villagesql_schema[0]; *query_ptr != nullptr;
       query_ptr++) {
    if (execute_statement(thd, *query_ptr)) {
      LogVSQL(ERROR_LEVEL, "Failed to execute villagesql schema command: %s",
              *query_ptr);
      return true;
    }
  }

  // Reread the version from the database now that the schema has been
  // installed.
  Semver installed_villagesql_version;
  if (SchemaManagerStatus::read_villagesql_version(
          thd, &installed_villagesql_version)) {
    LogVSQL(ERROR_LEVEL, "Error reading the current version after install");
    return true;
  }

  if (!installed_villagesql_version.is_valid()) {
    LogVSQL(ERROR_LEVEL, "No valid version found after install");
    return true;
  }
  SchemaManagerStatus::set_version(installed_villagesql_version);

  LogVSQL(SYSTEM_LEVEL, "System table schema application completed");

  clear_table_caches(thd);

  return false;
}

// Bootstrap-thread handler with the guards upgrade_villagesql_schema
// normally inherits from dd::upgrade::upgrade_system_schemas.
bool upgrade_villagesql_schema_in_bootstrap(THD *thd) {
  const Disable_autocommit_guard autocommit_guard(thd);
  dd::upgrade::Bootstrap_error_handler error_handler;
  const Disable_binlog_guard disable_binlog(thd);
  const Disable_sql_log_bin_guard disable_sql_log_bin(thd);

  const bool err = SchemaManager::upgrade_villagesql_schema(thd);
  clear_table_caches(thd);
  return dd::end_transaction(thd, err);
}

}  // namespace

bool SchemaManager::maybe_install_villagesql_schema_on_first_run(
    long upgrade_mode) {
  // If this is the first time we are running, or if the previous installation
  // was partially successful, we should get an invalid version and reinstall
  // the villagesql scheam as if it were the first run.
  if (get_version().is_valid()) {
    // This is not the first time we are running.
    return false;
  }

  if (upgrade_mode == UPGRADE_NONE) {
    // Error: we require that first time runs create our system tables, and so
    // NONE is incompatible with this.
    LogVSQL(
        ERROR_LEVEL,
        "This is a first time run, and so cannot be run with --upgrade=NONE");
    return true;
  }

  init_optimizer_cost_module(true);
  if (bootstrap::run_bootstrap_thread(nullptr, nullptr,
                                      &install_villagesql_schema,
                                      SYSTEM_THREAD_SERVER_UPGRADE)) {
    LogVSQL(ERROR_LEVEL, "Failed to install VillageSQL schema on first run");
    return true;
  }
  delete_optimizer_cost_module();

  return false;
}

bool SchemaManager::run_villagesql_upgrades_standalone() {
  init_optimizer_cost_module(true);
  const bool err = bootstrap::run_bootstrap_thread(
      nullptr, nullptr, &upgrade_villagesql_schema_in_bootstrap,
      SYSTEM_THREAD_SERVER_UPGRADE);
  delete_optimizer_cost_module();
  return err;
}

bool SchemaManager::upgrade_villagesql_schema(THD *thd) {
  // Get current VillageSQL version from villagesql.properties table
  const Semver current_villagesql_version = get_version();
  const Semver target_villagesql_version = GetBuildVersion();

  // A code base change is not a numeric upgrade, so Semver deliberately leaves
  // versions from different code bases unordered and the comparison below can
  // never see one. Refuse to start rather than fall through it: otherwise the
  // version-specific upgrades are skipped, the stored version keeps naming the
  // old code base, and is_villagesql_upgrade_needed() stays true so every
  // later restart re-runs the whole server upgrade path.
  //
  // An invalid stored version means the schema is missing or incomplete, not
  // that it came from another code base: an unparsable stored version already
  // failed startup back in read_villagesql_version(). Installing is the job of
  // maybe_install_villagesql_schema_on_first_run(), which runs after this, so
  // there is nothing to check or upgrade here.
  //
  // TODO(villagesql-production): support upgrading a data directory across code
  // bases.
  if (current_villagesql_version.is_valid() &&
      current_villagesql_version.code_base() !=
          target_villagesql_version.code_base()) {
    LogVSQL(ERROR_LEVEL,
            "Cannot upgrade the VillageSQL schema across code bases: this data "
            "directory was created by a %s build, but this server is a %s "
            "build. Use the original server binary for this data directory.",
            current_villagesql_version.to_string().c_str(),
            target_villagesql_version.to_string().c_str());
    return true;
  }

  if (current_villagesql_version < target_villagesql_version) {
    if (opt_upgrade_mode == UPGRADE_NONE) {
      LogVSQL(ERROR_LEVEL,
              "An upgrade is required, but --upgrade=NONE is specified");
      return true;
    }

    if (current_villagesql_version.has_prerelease()) {
      if (!opt_villagesql_allow_unsafe_dev_upgrade) {
        LogVSQL(ERROR_LEVEL,
                "Upgrading from a development version (%s) is not allowed. "
                "Use --villagesql-allow-unsafe-dev-upgrade to permit this.",
                current_villagesql_version.to_string().c_str());
        return true;
      }
      LogVSQL(WARNING_LEVEL,
              "Upgrading from a development version (%s); "
              "--villagesql-allow-unsafe-dev-upgrade was specified.",
              current_villagesql_version.to_string().c_str());
    } else if (opt_villagesql_allow_unsafe_dev_upgrade) {
      LogVSQL(ERROR_LEVEL,
              "--villagesql-allow-unsafe-dev-upgrade specified, but current "
              "version (%s) is not a development version.",
              current_villagesql_version.to_string().c_str());
      return true;
    }

    LogVSQL(SYSTEM_LEVEL, "Schema upgrade started from version %s to %s",
            current_villagesql_version.to_string().c_str(),
            target_villagesql_version.to_string().c_str());

    // Run version-specific upgrades first
    if (run_villagesql_version_upgrades(thd, current_villagesql_version)) {
      return true;
    }

    Semver updated_villagesql_version;
    if (SchemaManagerStatus::read_villagesql_version(
            thd, &updated_villagesql_version)) {
      LogVSQL(ERROR_LEVEL, "Failed to read updated schema version");
      return true;
    }

    if (!updated_villagesql_version.is_valid()) {
      LogVSQL(ERROR_LEVEL, "Updated schema version is not valid");
      return true;
    }

    SchemaManagerStatus::set_version(updated_villagesql_version);
    if (updated_villagesql_version != target_villagesql_version) {
      LogVSQL(ERROR_LEVEL,
              "Schema updates did not reach desired version: %s vs. %s",
              updated_villagesql_version.to_string().c_str(),
              target_villagesql_version.to_string().c_str());
      return true;
    }

    LogVSQL(SYSTEM_LEVEL, "Schema upgrade to version %s completed",
            target_villagesql_version.to_string().c_str());
  } else if (opt_villagesql_allow_unsafe_dev_upgrade) {
    LogVSQL(ERROR_LEVEL,
            "--villagesql-allow-unsafe-dev-upgrade specified, but no upgrade "
            "is being performed.");
    return true;
  }

  return false;
}

bool SchemaManager::is_villagesql_upgrade_needed() {
  return SchemaManagerStatus::get_upgrade_needed();
}

Semver SchemaManager::get_version() {
  return SchemaManagerStatus::get_version();
}

void SchemaManager::deinit() { SchemaManagerStatus::deinit(); }

void SchemaManagerStatus::set_version(const Semver &ver) {
  assert(!is_initializing.load() && !is_initialized.load());

  LogVSQL(INFORMATION_LEVEL, "Setting Schema Version to %s",
          ver.to_string().c_str());

  // Safe because we only update during single-threaded initialization/upgrade
  delete version;
  version = new Semver(ver);

  if (!upgrade_needed) {
    // Treat as an upgrade when the stored version is invalid, when its code
    // base differs from the build (including legacy stored versions that
    // predate code bases and were assigned the legacy code base), or when it
    // is numerically older than the build.
    const Semver build_version = GetBuildVersion();
    upgrade_needed = new bool(!ver.is_valid() ||
                              ver.code_base() != build_version.code_base() ||
                              ver < build_version);
  }
}

bool SchemaManagerStatus::read_villagesql_version(THD *thd, Semver *version) {
  // Initialize outputs
  *version = Semver();

  // Check if villagesql schema exists at all, and get its id
  char query[512];
  snprintf(query, sizeof(query),
           "SELECT id FROM mysql.schemata WHERE name = '%s'",
           SchemaManager::VILLAGESQL_SCHEMA_NAME);

  std::string schema_id_str;
  if (execute_and_extract_single_value(thd, query, &schema_id_str)) {
    LogVSQL(ERROR_LEVEL, "Failed to read from mysql.schemata table");
    return true;
  }

  if (schema_id_str.empty()) {
    // First-time installation - schema doesn't exist yet
    // This is not an error, just means we need to create everything from
    // scratch
    LogVSQL(INFORMATION_LEVEL,
            "Missing %s schema - probably an initial installation",
            SchemaManager::VILLAGESQL_SCHEMA_NAME);
    return false;  // Success, but exists=false will trigger full initialization
  }

  // The value is not text, it is an integer, so we need to read the raw binary..
  if (schema_id_str.length() != sizeof(uint64_t)) {
    LogVSQL(ERROR_LEVEL, "Read unexpected sized value");
    return true;
  }

  const uint64_t schema_id = uint8korr(schema_id_str.data());

  // Schema exists, check if properties table exists, which should be the last
  // table created in the schema file
  snprintf(query, sizeof(query),
           "SELECT name FROM mysql.tables "
           "WHERE schema_id = %" PRIu64 " AND name = '%s'",
           schema_id, SchemaManager::PROPERTIES_TABLE_NAME);

  bool properties_table_exists = query_has_rows(thd, query);
  if (!properties_table_exists) {
    // Schema exists but properties table doesn't - partial installation
    // This can happen if schema creation was interrupted
    LogVSQL(INFORMATION_LEVEL, "Missing %s.%s table",
            SchemaManager::VILLAGESQL_SCHEMA_NAME,
            SchemaManager::PROPERTIES_TABLE_NAME);
    return false;  // Success, but exists=false will trigger schema completion
  }

  // Both schema and table exist, try to read version
  snprintf(query, sizeof(query),
           "SELECT value FROM %s.%s WHERE name = 'version'",
           SchemaManager::VILLAGESQL_SCHEMA_NAME,
           SchemaManager::PROPERTIES_TABLE_NAME);

  std::string version_str;
  if (execute_and_extract_single_value(thd, query, &version_str)) {
    LogVSQL(ERROR_LEVEL, "Failed to read from %s.%s table",
            SchemaManager::VILLAGESQL_SCHEMA_NAME,
            SchemaManager::PROPERTIES_TABLE_NAME);
    return true;
  }

  if (version_str.empty()) {
    // No version row found - this is not an error for initial installation
    LogVSQL(INFORMATION_LEVEL, "Missing version in %s.%s",
            SchemaManager::VILLAGESQL_SCHEMA_NAME,
            SchemaManager::PROPERTIES_TABLE_NAME);
    return false;  // Success, but exists=false will trigger initialization
  }

  std::string error_msg;
  if (!version->parse_schema_version(version_str, &error_msg)) {
    LogVSQL(ERROR_LEVEL, "Failed to parse schema version string \"%s\": %s",
            version_str.c_str(), error_msg.c_str());
    return true;  // It is an error to have an unparsable value
  }

  return false;  // Success
}

bool SchemaManagerStatus::write_villagesql_version(THD *thd,
                                                   const Semver &version) {
  // Insert or update version in villagesql.properties table
  std::string version_str = version.to_string();
  char query[512];
  snprintf(query, sizeof(query),
           "INSERT INTO %s.%s (name, value, description) VALUES "
           "('version', '%s', 'VillageSQL system schema version') "
           "ON DUPLICATE KEY UPDATE value = '%s'",
           SchemaManager::VILLAGESQL_SCHEMA_NAME,
           SchemaManager::PROPERTIES_TABLE_NAME, version_str.c_str(),
           version_str.c_str());

  // Execute the query - return true on failure, false on success
  if (execute_statement(thd, query)) {
    LogVSQL(ERROR_LEVEL, "Failed to update schema version to \"%s\"",
            version_str.c_str());
    return true;
  }

  // Update memory
  set_version(version);
  return false;
}

}  // namespace villagesql
