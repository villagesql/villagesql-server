// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, see <https://www.gnu.org/licenses/>.

#ifndef VILLAGESQL_SERVICES_PREVIEW_TABLE_STORAGE_H_
#define VILLAGESQL_SERVICES_PREVIEW_TABLE_STORAGE_H_

#include <string>
#include <string_view>

#include "villagesql/sdk/include/villagesql/abi/preview/table_storage.h"

class THD;

namespace villagesql::services {

vef_preview_table_storage_t *preview_table_storage_vtable();

// Schema that holds the physical tables backing hidden-table descriptors.
extern const char *const kTableStorageSchema;

// Derive the physical table name (within kTableStorageSchema) from a
// hidden-table logical_name. Sanitizes characters that aren't legal in
// table identifiers and clips to MySQL's identifier length limit.
std::string physical_table_storage_name(std::string_view logical_name);

// Materialize the physical table backing a table_storage_def via
// mysql_create_table with IF NOT EXISTS semantics. Must NOT be called
// from inside the custom-index runtime's locked region (g_runtime_mu)
// — invokes mysql_create_table, which fires transaction-commit hooks
// that re-enter the runtime lock and deadlock. Safe from DDL paths
// (Sql_cmd_create_table::execute, Metadata_modifier::process_create /
// process_alter) where no runtime lock is held.
bool materialize_physical_table_storage(THD *thd,
                                        const vef_table_storage_def_t *def,
                                        char *error_msg,
                                        uint32_t error_msg_len);

}  // namespace villagesql::services

#endif  // VILLAGESQL_SERVICES_PREVIEW_TABLE_STORAGE_H_
