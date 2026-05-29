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

#ifndef VILLAGESQL_SERVICES_PREVIEW_CUSTOM_INDEX_TABLE_STORAGE_BACKEND_H_
#define VILLAGESQL_SERVICES_PREVIEW_CUSTOM_INDEX_TABLE_STORAGE_BACKEND_H_

namespace villagesql::services {

// Build (lazily) the hidden-table-backed custom-index backend and register
// it with the runtime. Called once during server startup from
// register_builtin_capabilities() after the table_storage capability itself
// is registered.
void register_table_storage_custom_index_backend();

}  // namespace villagesql::services

#endif  // VILLAGESQL_SERVICES_PREVIEW_CUSTOM_INDEX_TABLE_STORAGE_BACKEND_H_
