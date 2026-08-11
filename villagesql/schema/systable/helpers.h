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

#ifndef VILLAGESQL_SCHEMA_SYSTABLE_HELPERS_H_
#define VILLAGESQL_SCHEMA_SYSTABLE_HELPERS_H_

#include <initializer_list>
#include <string>
#include <string_view>

struct CHARSET_INFO;
class Field;
class THD;

namespace villagesql {

// For use with asserts.
const bool VILLAGESQL_NOT_IMPLEMENTED = false;

// ===== Identifier normalization utilities =====
// Get the appropriate charset for identifier normalization
// Uses fs_name_collation() logic from MySQL DD
const CHARSET_INFO *get_identifier_charset();

// Normalize different identifier types according to MySQL rules
// Database names: Follow lower_case_table_names setting (like MySQL DD)
std::string normalize_database_name(const std::string &name);

// Table names: Follow lower_case_table_names setting (like MySQL DD)
std::string normalize_table_name(const std::string &name);

// Column names: Always case-insensitive (per MySQL standard)
std::string normalize_column_name(const std::string &name);

// Extension names: Always case-insensitive (like plugin names)
std::string normalize_extension_name(const std::string &name);

// Type names: Always case-insensitive (like SQL type names)
std::string normalize_type_name(const std::string &name);

// Index names: Follow lower_case_table_names setting (same as table names)
std::string normalize_index_name(const std::string &name);

// Build a qualified base name string "extension_name.type_name".
inline std::string make_qualified_base_name(std::string_view extension_name,
                                            std::string_view type_name) {
  std::string result;
  result.reserve(extension_name.size() + 1 + type_name.size());
  result.append(extension_name);
  result.push_back('.');
  result.append(type_name);
  return result;
}

// Returns true if name is a qualified custom type name (contains a dot),
// e.g. "vsql_complex.COMPLEX". The dot is the canonical separator defined
// by make_qualified_base_name().
inline bool is_qualified_name(std::string_view name) {
  return name.find('.') != std::string_view::npos;
}

struct ParsedQualifiedName {
  std::string extension_name;  // empty if unqualified
  std::string bare_name;
};

// Splits a potentially qualified name into {extension_name, bare_name}.
// "ext.name" -> {"ext", "name"}
// "name"     -> {"",    "name"}
inline ParsedQualifiedName parse_qualified_name(std::string_view name) {
  auto dot = name.find('.');
  if (dot == std::string_view::npos) return {"", std::string(name)};
  return {std::string(name.substr(0, dot)), std::string(name.substr(dot + 1))};
}

// Helper functions for reading a value from a Field.
void read_string_field(Field *f, std::string &out);
void read_unsigned_field(Field *f, unsigned int &out);
void read_bigint_field(Field *f, long long &out);
void read_tinyint_unsigned_field(Field *f, unsigned char &out);

// Execute a statement; log failures at ERROR_LEVEL. Return true on failure.
bool execute_statement(THD *thd, const char *query);

// Execute a statement; silently suppress errors whose code appears in
// ignored_errors (logs at INFORMATION_LEVEL and returns false for those).
// Any other failure is logged at ERROR_LEVEL and returns true.
bool execute_statement_ignore_errors(
    THD *thd, const char *query,
    std::initializer_list<unsigned int> ignored_errors);

// Helper function to execute a query and check if it returns any rows
bool query_has_rows(THD *thd, const char *query);

// Helper function to execute a query and extract a single column string value.
// Returns true on query execution failure and logs an error.
// Returns false on success; if no row/column is returned, sets value to empty.
// On success with a row, copies the string value into the provided std::string.
bool execute_and_extract_single_value(THD *thd, const char *query,
                                      std::string *value);

// ===== Test utilities =====
// Direct access to lower_case_table_names for testing
// This allows unit tests to modify the same variable the normalization
// functions use
void test_set_lower_case_table_names(int value);
int test_get_lower_case_table_names();

}  // namespace villagesql

#endif  // VILLAGESQL_SCHEMA_SYSTABLE_HELPERS_H_
