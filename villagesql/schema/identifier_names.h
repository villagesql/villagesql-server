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

#ifndef VILLAGESQL_SCHEMA_IDENTIFIER_NAMES_H_
#define VILLAGESQL_SCHEMA_IDENTIFIER_NAMES_H_

#include <string>

struct CHARSET_INFO;

namespace villagesql {

// Single home for identifier collation rules. System tables store names
// as entered; canonical forms are for in-memory keys and comparisons only.
// Identifiers never contain characters outside utf8mb3 (the parser converts
// them to the system charset), so utf8mb3 case folding always applies.

// Collation for database/table names; delegates to the DD's
// Object_table_definition_impl::fs_name_collation().
const CHARSET_INFO *fs_name_collation();

// Database names: follow lower_case_table_names (like MySQL DD)
std::string canonical_database_name(const std::string &name);

// Table names: follow lower_case_table_names (like MySQL DD)
std::string canonical_table_name(const std::string &name);

// Column names: always case-insensitive (like MySQL DD)
std::string canonical_column_name(const std::string &name);

// Extension names: always case-insensitive (like plugin names)
std::string canonical_extension_name(const std::string &name);

// Type names: always case-insensitive (like SQL type names)
std::string canonical_type_name(const std::string &name);

// Index names: always case-insensitive (like MySQL DD)
std::string canonical_index_name(const std::string &name);

// Equality per MySQL identifier rules. Use these instead of ad-hoc
// my_strcasecmp calls.
bool database_names_equal(const std::string &a, const std::string &b);
bool table_names_equal(const std::string &a, const std::string &b);
bool column_names_equal(const std::string &a, const std::string &b);
bool index_names_equal(const std::string &a, const std::string &b);
bool type_names_equal(const std::string &a, const std::string &b);
bool extension_names_equal(const std::string &a, const std::string &b);

// Collation canonicalizing custom-type parameter strings. Parameters are not
// MySQL identifiers; the canonical form is embedded in stored
// type_parameters JSON, so this must stay stable.
const CHARSET_INFO *type_parameter_collation();

// Test-only access to the lower_case_table_names global.
void test_set_lower_case_table_names(int value);
int test_get_lower_case_table_names();

}  // namespace villagesql

#endif  // VILLAGESQL_SCHEMA_IDENTIFIER_NAMES_H_
