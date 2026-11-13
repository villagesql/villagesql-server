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

#ifndef VILLAGESQL_SERVICES_PREVIEW_SQL_QUERY_H
#define VILLAGESQL_SERVICES_PREVIEW_SQL_QUERY_H

#include <string>

#include "mysql/strings/m_ctype.h"
#include "villagesql/sdk/include/villagesql/abi/preview/sql_query.h"

namespace villagesql::services {

enum class QueryTextStatus { kOk, kInvalid, kOutOfMemory };

// Convert a command-service cell to UTF-8. Binary and internal ASCII values
// (charset == nullptr) pass through. On failure, out is unchanged.
QueryTextStatus copy_query_result_text(const char *value, size_t length,
                                       const CHARSET_INFO *charset,
                                       std::string *out);

// Returns the server-side vtable for the "vsql::preview::sql_query" capability.
vef_preview_sql_query_t *preview_sql_query_vtable();

}  // namespace villagesql::services

#endif  // VILLAGESQL_SERVICES_PREVIEW_SQL_QUERY_H
