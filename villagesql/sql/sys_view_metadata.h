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

#ifndef VILLAGESQL_SQL_SYS_VIEW_METADATA_H_
#define VILLAGESQL_SQL_SYS_VIEW_METADATA_H_

class THD;

namespace villagesql {

// Restores the stored column metadata of the sys views that installing the
// VillageSQL INFORMATION_SCHEMA overrides re-derives.
//
// When INFORMATION_SCHEMA is first created - it's created in strict sql_mode.
// When INFORMATION_SCHEMA (NON_DD_BASED) are created with CREATE OR REPLACE
// VIEW, they are created with sql_mode = 0. This makes columns defined with
// COLLATE and CONCAT to be marked as NOT NULL, which is not the case when they
// are created in strict sql_mode. This function replays the CREATE OR REPLACE
// VIEW statements for these views under strict sql_mode to restore the original
// column metadata.
//
// Two requirements on the caller:
//
//   - Run it on a bootstrap thread. run_bootstrap_thread() installs the server
//     default sql_mode, and the strict mode in it is precisely what makes the
//     replay record the vanilla values. Under a relaxed sql_mode this
//     re-records the rewritten form and achieves nothing.
//
//   - Have the optimizer cost model initialized. Some of the sys views are
//     ALGORITHM = TEMPTABLE, and create_tmp_table() reaches into it. Startup
//     tears the cost model down between bootstrap DDL phases, so the caller
//     generally needs an init_optimizer_cost_module() / delete pair around it.
//
// Returns true on failure. Callers during startup should treat that as fatal:
// it runs single-threaded with no concurrent DDL, so this replay should not
// failed.
bool refresh_sys_view_metadata(THD *thd);

}  // namespace villagesql

#endif  // VILLAGESQL_SQL_SYS_VIEW_METADATA_H_
