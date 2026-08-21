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

#include "villagesql/schema/systable/helpers.h"

#include <algorithm>
#include <cctype>
#include <initializer_list>
#include <string>
#include "lex_string.h"
#include "mysql/strings/m_ctype.h"
#include "sql/field.h"
#include "sql/mysqld.h"
#include "sql/sql_class.h"
#include "sql/statement/ed_connection.h"
#include "villagesql/include/error.h"

namespace villagesql {

void read_string_field(Field *f, std::string &out) {
  if (f && !f->is_null()) {
    char buffer[4096];
    String str(buffer, sizeof(buffer), &my_charset_utf8mb4_bin);
    f->val_str(&str);
    out = std::string(str.ptr(), str.length());
  } else {
    out.clear();
  }
}

void read_unsigned_field(Field *f, unsigned int &out) {
  if (f && !f->is_null()) {
    out = static_cast<unsigned int>(f->val_int());
  } else {
    out = 0;
  }
}

void read_bigint_field(Field *f, long long &out) {
  if (f && !f->is_null()) {
    out = f->val_int();
  } else {
    out = 0;
  }
}

void read_tinyint_unsigned_field(Field *f, unsigned char &out) {
  if (f && !f->is_null()) {
    out = static_cast<unsigned char>(f->val_int());
  } else {
    out = 0;
  }
}

bool execute_statement(THD *thd, const char *query) {
  Ed_connection con(thd);
  LEX_STRING str;
  str.str = const_cast<char *>(query);
  str.length = strlen(query);

  if (con.execute_direct(str)) {
    LogVSQL(ERROR_LEVEL, "Statement failed: %s — %s", query,
            con.get_last_error());
    return true;
  }
  return false;
}

bool execute_statement_ignore_errors(
    THD *thd, const char *query,
    std::initializer_list<unsigned int> ignored_errors) {
  Ed_connection con(thd);
  LEX_STRING str;
  str.str = const_cast<char *>(query);
  str.length = strlen(query);

  if (con.execute_direct(str)) {
    unsigned int err = con.get_last_errno();
    for (unsigned int code : ignored_errors) {
      if (err == code) {
        LogVSQL(INFORMATION_LEVEL, "Statement error ignored (errno=%u): %s",
                err, query);
        return false;
      }
    }
    LogVSQL(ERROR_LEVEL, "Statement failed: %s — %s", query,
            con.get_last_error());
    return true;
  }
  return false;
}

bool query_has_rows(THD *thd, const char *query) {
  Ed_connection con(thd);
  LEX_STRING str;
  str.str = const_cast<char *>(query);
  str.length = strlen(query);

  // Execute the query
  if (con.execute_direct(str)) {
    LogVSQL(ERROR_LEVEL, "Failed to run query %s", query);
    return false;  // Query failed, assume no rows
  }

  // Check if we got any result rows
  Ed_result_set *result_set = con.get_result_sets();
  return (result_set != nullptr && result_set->size() > 0);
}

bool execute_and_extract_single_value(THD *thd, const char *query,
                                      std::string *value) {
  Ed_connection con(thd);
  value->clear();

  LEX_STRING str;
  str.str = const_cast<char *>(query);
  str.length = strlen(query);

  if (con.execute_direct(str)) {
    LogVSQL(ERROR_LEVEL, "Failed to run query %s: %s", query,
            con.get_last_error());
    return true;  // Query execution failed
  }

  Ed_result_set *result_set = con.get_result_sets();
  if (result_set == nullptr || result_set->size() == 0) {
    return false;  // No rows - value is empty
  }

  List_iterator<Ed_row> row_iter(*result_set);
  Ed_row *first_row = row_iter++;
  if (first_row == nullptr) {
    return false;  // No first row - value is empty
  }

  const Ed_column *result = first_row->get_column(0);
  if (result == nullptr || result->str == nullptr) {
    return false;  // No column or null value - value is empty
  }

  // Copy the string while the Ed_connection is still alive
  value->assign(result->str, result->length);
  return false;  // Success
}

// ===== Test utilities =====
void test_set_lower_case_table_names(int value) {
  ::lower_case_table_names = value;
}

int test_get_lower_case_table_names() { return ::lower_case_table_names; }

}  // namespace villagesql
