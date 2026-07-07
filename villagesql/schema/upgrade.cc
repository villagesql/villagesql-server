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

#include "mysqld_error.h"
#include "sql/sql_class.h"
#include "villagesql/include/error.h"
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

}  // namespace upgrade
}  // namespace villagesql
