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

#ifndef VILLAGESQL_SCHEMA_SCHEMA_MANAGER_H_
#define VILLAGESQL_SCHEMA_SCHEMA_MANAGER_H_

#include <atomic>
#include <cassert>
#include "my_inttypes.h"
#include "sql/dd/dd.h"
#include "villagesql/include/semver.h"

class THD;

namespace villagesql {

// Manages VillageSQL system schema creation, upgrades, versioning and general
// extension initialization.
class SchemaManager {
 public:
  // Schema-related constants
  static const char *VILLAGESQL_SCHEMA_NAME;
  static const char *PROPERTIES_TABLE_NAME;
  static const char *COLUMNS_TABLE_NAME;
  static const char *SP_PARAMS_TABLE_NAME;
  static const char *EXTENSIONS_TABLE_NAME;
  static const char *INDEXES_TABLE_NAME;
  static const char *INDEX_COLUMNS_TABLE_NAME;

  // Maximum length of a name (extension name, type name, etc.) as stored in
  // the schema (matches the VARCHAR(64) columns in the system tables).
  static constexpr size_t kMaxNameLen = 64;
  // Maximum length of a qualified type name "extension_name.type_name".
  static constexpr size_t kMaxQualifiedTypeNameLen = (kMaxNameLen * 2) + 1;

  /**
   * Before init is called, set the internal bootstrap version for the
   * villagesql system tables so that upgrade necessity is evaluated
   * properly. This must be called only once from dd::init() with the init type.
   *
   * @return Status of performed operation
   * @retval false success
   * @retval true failure
   */
  static bool preinit(dd::enum_dd_init_type dd_init);

  /**
   * When running with --initialize and --init-file flags set this method is
   * responsible for setting the version and inititializing in memory state
   * after the VillageSQL initial installation but before any user
   * initializaiton has been run.
   *
   * @param thd Current thread execution context
   * @return Status of performed operation
   * @retval false success
   * @retval true failure
   */
  static bool bootstrap(THD *thd);

  /**
   * Initialize VillageSQL infrastructure by verifying schema and loading
   * extensions. Similar to mysql_persistent_dynamic_loader_imp::init()
   *
   * @param thd Current thread execution context
   * @return Status of performed operation
   * @retval false success
   * @retval true failure
   */
  static bool init(THD *thd);

  /**
   * Check if SchemaManager has been initialized
   * @return true if initialized, false otherwise
   */
  static bool initialized();

  /**
   * Maybe installs VillageSQL system schema to the current version. This must
   * be run on startup, before the upgrade path, but after preinit.
   *
   * This function is only used for a first time installation that is not using
   * --initialize.
   *
   * @param[in]  upgrade_mode  The upgrade mode, i.e. NONE, MINIMAL, AUTO,
   * FORCE.
   * @retval false  ON SUCCESS
   * @retval true   ON FAILURE
   */
  static bool maybe_install_villagesql_schema_on_first_run(long upgrade_mode);

  /**
   * Upgrades VillageSQL system schema to the current version.
   *
   * This function is called from MySQL's upgrade system whenever:
   * 1. MySQL server upgrade runs (version change)
   * 2. --upgrade=FORCE is used
   *
   * The function checks if VillageSQL schema upgrade is needed by comparing
   * the stored version in villagesql.properties against the VILLAGESQL_VERSION
   * constant. If an upgrade is needed, it runs version-specific migration
   * functions and updates the stored version.
   *
   * @param[in]  thd   Thread handle.
   *
   * @retval false  ON SUCCESS
   * @retval true   ON FAILURE
   */
  static bool upgrade_villagesql_schema(THD *thd);

  /**
   * Run upgrade_villagesql_schema() in its own bootstrap thread.
   *
   * Used with --upgrade=MINIMAL, which skips upgrade_system_schemas() where
   * the upgrades normally run. VillageSQL system tables are dictionary
   * state, so they are upgraded even in MINIMAL mode.
   *
   * @retval false  ON SUCCESS
   * @retval true   ON FAILURE
   */
  static bool run_villagesql_upgrades_standalone();

  /**
   * Returns true iff the version of the schema stored in the database
   * is less than the version of the binary currently running.
   *
   * Safe to call at any point during startup. Returns false when startup never
   * established an answer, which happens on the dd::init() failure path and
   * under --initialize; see the definition for why, and the mtr test
   * villagesql/percona_merge.dd_init_failure_graceful_exit for the case that
   * made it matter.
   */
  static bool is_villagesql_upgrade_needed();

  /**
   * Read the version of the schema manager from memory, it is updated:
   * during startup when read from the database
   * immediately after any schema updates run during server startup
   * after the schema is written during --initialize
   */
  static Semver get_version();

  /**
   * Free resources allocated by SchemaManager. Called during server shutdown.
   */
  static void deinit();
};

}  // namespace villagesql

#endif  // VILLAGESQL_SCHEMA_SCHEMA_MANAGER_H_
