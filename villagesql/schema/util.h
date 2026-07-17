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

#ifndef VILLAGESQL_SCHEMA_UTIL_H
#define VILLAGESQL_SCHEMA_UTIL_H

#include "mysql/strings/m_ctype.h"
#include "sql/mysqld_cs.h"
#include "villagesql/schema/schema_manager.h"

struct TABLE;
class Table_ref;

namespace villagesql {

// Case-insensitive comparison for system table/schema identifiers, using
// the system charset. This is how MySQL compares system object names.
inline bool system_name_eq(const char *a, const char *b) {
  return my_strcasecmp(system_charset_info, a, b) == 0;
}

// Check if a database name is the 'villagesql' schema. db_name must not be
// nullptr.
inline bool is_villagesql_schema(const char *db_name) {
  return system_name_eq(SchemaManager::VILLAGESQL_SCHEMA_NAME, db_name);
}

inline bool is_mysql_schema(const char *db_name) {
  return system_name_eq("mysql", db_name);
}

// Like the above but also considered true if this is in a mysql system schema.
inline bool is_system_schema(const char *db_name) {
  return (is_mysql_schema(db_name) || system_name_eq("sys", db_name) ||
          is_villagesql_schema(db_name));
}

// Check if a schema and table name match a VillageSQL system table.
inline bool is_villagesql_system_table(const char *schema_name,
                                       const char *table_name,
                                       const char *expected_table_name) {
  return is_villagesql_schema(schema_name) &&
         system_name_eq(table_name, expected_table_name);
}

// Check if a TABLE is in the 'villagesql' schema.
// All tables in the villagesql schema are system tables that can be accessed
// with no_read_locking from INFORMATION_SCHEMA views.
// This allows INFORMATION_SCHEMA views to query villagesql tables
// without triggering InnoDB locking assertions.
bool is_villagesql_system_table(const TABLE *table);

// Number of tables reachable through the next_global chain from `first`
// (0 if `first` is nullptr).
uint count_global_tables(const Table_ref *first);

}  // namespace villagesql

#endif  // VILLAGESQL_SCHEMA_UTIL_H
