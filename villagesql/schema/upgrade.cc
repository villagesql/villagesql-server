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
  // Upgrade all villagesql system tables to utf8mb4_bin collation. This is
  // to standardize the collation across all system tables, and to avoid issues
  // with comparisons between villagesql system tables and the data dictionary.
  //
  // Disable persistent InnoDB statistics on the system tables, matching the
  // mysql.* tables. With persistent stats, InnoDB queues a table for background
  // recalculation once 10% of its rows change, and these tables are small
  // enough that a single write crosses that line. The background stats thread
  // then takes MDL on the table, which can hang startup: the pending
  // extension-update apply writes these tables just before
  // dd::reset_tables_and_tablespaces() takes MDL_EXCLUSIVE on every cached
  // table with a one-year lock wait.
  //
  // CONVERT TO is table-wide and also stamps the table collation onto JSON
  // columns, which CREATE TABLE leaves at binary. Restate each JSON column so
  // it is re-parsed as JSON and keeps the binary collation an install produces;
  // otherwise an upgraded datadir carries different column metadata than a
  // fresh one. Keep these definitions in sync with
  // villagesql/schema/villagesql_schema.sql.in.
  LogVSQL(INFORMATION_LEVEL,
          "Upgrading villagesql system tables to utf8mb4_bin and adding "
          "STATS_PERSISTENT=0");
  static const char *statements[] = {
      "ALTER TABLE villagesql.extensions "
      "CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, "
      "MODIFY pending_action JSON NULL "
      "COMMENT 'Pending deferred action (e.g. version update). "
      "NULL when no action is pending.', "
      "STATS_PERSISTENT=0",
      "ALTER TABLE villagesql.custom_columns "
      "CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, "
      "MODIFY type_parameters JSON NOT NULL "
      "COMMENT 'Type instantiation parameters as JSON', "
      "STATS_PERSISTENT=0",
      "ALTER TABLE villagesql.custom_sp_params "
      "CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, "
      "MODIFY type_parameters JSON NOT NULL "
      "COMMENT 'Type instantiation parameters as JSON', "
      "STATS_PERSISTENT=0",
      "ALTER TABLE villagesql.custom_indexes "
      "CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, "
      "MODIFY index_type_parameters JSON NOT NULL "
      "COMMENT 'Index type instantiation parameters as JSON', "
      "STATS_PERSISTENT=0",
      "ALTER TABLE villagesql.custom_index_columns "
      "CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, "
      "STATS_PERSISTENT=0",
      "ALTER TABLE villagesql.properties "
      "CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, "
      "STATS_PERSISTENT=0",
  };
  for (const char *statement : statements) {
    if (execute_statement(thd, statement)) return true;
  }
  return false;
}

}  // namespace upgrade
}  // namespace villagesql
