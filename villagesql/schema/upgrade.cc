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

#include "villagesql/schema/upgrade.h"

#include <string>

#include "mysqld_error.h"
#include "sql/sql_class.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/schema_manager.h"
#include "villagesql/schema/systable/helpers.h"

namespace villagesql {
namespace upgrade {

bool upgrade_villagesql_from_0_0_1_to_0_0_3(THD *thd) {
  // Add type_parameters column to custom_columns table
  LogVSQL(INFORMATION_LEVEL,
          "Upgrading custom_columns: adding type_parameters column");
  return execute_statement_ignore_errors(
      thd,
      "ALTER TABLE villagesql.custom_columns "
      "ADD COLUMN type_parameters JSON NOT NULL "
      "COMMENT 'Type instantiation parameters as JSON'",
      {ER_DUP_FIELDNAME});
}

bool upgrade_villagesql_from_0_0_4_to_0_0_5(THD *thd) {
  // Add pending_action column to extensions table. Stores a JSON blob
  // describing a deferred action (today: version_update via
  // ALTER EXTENSION ... AT RESTART). NULL when no action is pending.
  LogVSQL(INFORMATION_LEVEL,
          "Upgrading extensions: adding pending_action column");
  return execute_statement_ignore_errors(
      thd,
      "ALTER TABLE villagesql.extensions "
      "ADD COLUMN pending_action JSON NULL "
      "COMMENT 'Pending deferred action (e.g. version update). "
      "NULL when no action is pending.'",
      {ER_DUP_FIELDNAME});
}

bool upgrade_villagesql_from_0_0_5_to_0_0_6(THD *thd) {
  // Disable persistent InnoDB statistics on the system tables, matching the
  // mysql.* tables. With persistent stats, InnoDB queues a table for background
  // recalculation once 10% of its rows change, and these tables are small
  // enough that a single write crosses that line. The background stats thread
  // then takes MDL on the table, which can hang startup: the pending
  // extension-update apply writes these tables just before
  // dd::reset_tables_and_tablespaces() takes MDL_EXCLUSIVE on every cached
  // table with a one-year lock wait.
  const char *const tables[] = {SchemaManager::EXTENSIONS_TABLE_NAME,
                                SchemaManager::COLUMNS_TABLE_NAME,
                                SchemaManager::SP_PARAMS_TABLE_NAME,
                                SchemaManager::INDEXES_TABLE_NAME,
                                SchemaManager::INDEX_COLUMNS_TABLE_NAME,
                                SchemaManager::PROPERTIES_TABLE_NAME};

  LogVSQL(
      INFORMATION_LEVEL,
      "Upgrading villagesql system tables: disabling persistent statistics");
  for (const char *table : tables) {
    const std::string query = std::string("ALTER TABLE ") +
                              SchemaManager::VILLAGESQL_SCHEMA_NAME + "." +
                              table + " STATS_PERSISTENT=0";
    if (execute_statement(thd, query.c_str())) return true;
  }
  return false;
}

}  // namespace upgrade
}  // namespace villagesql
