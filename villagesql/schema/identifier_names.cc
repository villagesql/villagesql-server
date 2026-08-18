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

#include "villagesql/schema/identifier_names.h"

#include "mysql/strings/m_ctype.h"
#include "sql/dd/impl/types/object_table_definition_impl.h"
#include "sql/mysqld.h"
#include "sql/mysqld_cs.h"
#include "sql/strfunc.h"

namespace villagesql {

const CHARSET_INFO *fs_name_collation() {
  return dd::Object_table_definition_impl::fs_name_collation();
}

std::string canonical_database_name(const std::string &name) {
  if (::lower_case_table_names == 0) {
    return name;
  }
  return casedn(fs_name_collation(), name);
}

std::string canonical_table_name(const std::string &name) {
  if (::lower_case_table_names == 0) {
    return name;
  }
  return casedn(fs_name_collation(), name);
}

std::string canonical_column_name(const std::string &name) {
  // utf8mb3 lowering, not utf8mb4_0900 folding: they differ for characters
  // like U+1E9E, and the DD uses utf8mb3 (dd::tables::Columns).
  return casedn(&my_charset_utf8mb3_tolower_ci, name);
}

std::string canonical_extension_name(const std::string &name) {
  return casedn(system_charset_info, name);
}

std::string canonical_type_name(const std::string &name) {
  return casedn(&my_charset_utf8mb3_tolower_ci, name);
}

std::string canonical_index_name(const std::string &name) {
  // Index names ignore lower_case_table_names (see dd::tables::Indexes).
  return casedn(&my_charset_utf8mb3_tolower_ci, name);
}

bool database_names_equal(const std::string &a, const std::string &b) {
  return canonical_database_name(a) == canonical_database_name(b);
}

bool table_names_equal(const std::string &a, const std::string &b) {
  return canonical_table_name(a) == canonical_table_name(b);
}

bool column_names_equal(const std::string &a, const std::string &b) {
  return canonical_column_name(a) == canonical_column_name(b);
}

bool index_names_equal(const std::string &a, const std::string &b) {
  return canonical_index_name(a) == canonical_index_name(b);
}

bool type_names_equal(const std::string &a, const std::string &b) {
  return canonical_type_name(a) == canonical_type_name(b);
}

bool extension_names_equal(const std::string &a, const std::string &b) {
  return canonical_extension_name(a) == canonical_extension_name(b);
}

namespace {

// Lower both sides under utf8mb4_bin, which uses the same case tables as
// the DD's utf8mb3_tolower_ci.
std::string folded_match_sql(const std::string &vsql_name,
                             const std::string &dd_name) {
  return "LOWER(" + vsql_name + ")=LOWER(CONVERT(" + dd_name +
         " USING utf8mb4) COLLATE utf8mb4_bin)";
}

// Exact-bytes comparison
std::string binary_match_sql(const std::string &vsql_name,
                             const std::string &dd_name) {
  return vsql_name + "=CONVERT(" + dd_name +
         " USING utf8mb4) COLLATE utf8mb4_bin";
}

}  // namespace

std::string database_name_match_sql(const std::string &vsql_name,
                                    const std::string &dd_name) {
  if (::lower_case_table_names == 0) {
    return binary_match_sql(vsql_name, dd_name);
  }
  return folded_match_sql(vsql_name, dd_name);
}

std::string table_name_match_sql(const std::string &vsql_name,
                                 const std::string &dd_name) {
  if (::lower_case_table_names == 0) {
    return binary_match_sql(vsql_name, dd_name);
  }
  return folded_match_sql(vsql_name, dd_name);
}

std::string column_name_match_sql(const std::string &vsql_name,
                                  const std::string &dd_name) {
  return folded_match_sql(vsql_name, dd_name);
}

const CHARSET_INFO *type_parameter_collation() {
  return &my_charset_utf8mb4_0900_ai_ci;
}

}  // namespace villagesql
