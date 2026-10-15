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
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "lex_string.h"
#include "my_base.h"
#include "mysql/strings/m_ctype.h"
#include "sql-common/json_dom.h"
#include "sql/field.h"
#include "sql/handler.h"
#include "sql/mysqld.h"
#include "sql/sql_class.h"
#include "sql/statement/ed_connection.h"
#include "sql/table.h"
#include "sql_string.h"
#include "template_utils.h"
#include "villagesql/include/error.h"

namespace villagesql {

int read_row_for_update(TABLE &table, const uchar *key_buf) {
  int error = table.file->ha_index_read_map(table.record[0], key_buf,
                                            HA_WHOLE_KEY, HA_READ_KEY_EXACT);
  if (error) return error;

  store_record(&table, record[1]);
  return 0;
}

std::string json_object_from_pairs(const StringPairs &pairs) {
  // Json_key_comparator gives the member order a Json_object would, and
  // assignment drops all but the last value for a key, as add_alias() does.
  std::map<std::string, std::string, Json_key_comparator> members;
  for (const auto &[key, value] : pairs) members[key] = value;

  // Assembled here rather than via Json_wrapper::to_string(), which reads
  // current_thd to bound the result against max_allowed_packet, which is a
  // limit these parameters cannot reach, and a dependency that makes the call
  // unusable without a session.
  StringBuffer<256> buf;
  buf.set_charset(&my_charset_utf8mb4_bin);
  if (buf.append('{')) return "{}";
  bool first = true;
  for (const auto &[key, value] : members) {
    if (!first && buf.append(", ", 2)) return "{}";
    first = false;
    if (double_quote(key.data(), key.length(), &buf) || buf.append(": ", 2) ||
        double_quote(value.data(), value.length(), &buf))
      return "{}";
  }
  if (buf.append('}')) return "{}";
  return std::string(buf.ptr(), buf.length());
}

bool json_object_to_pairs(const std::string &json, StringPairs *out) {
  if (json.empty()) {
    out->clear();
    return false;
  }

  // The caller decides what a parse failure means, so report it through the
  // return value rather than raising here.
  Json_dom_ptr dom = Json_dom::parse(
      json.data(), json.length(), [](const char *, size_t) {}, [] {});
  if (dom == nullptr || dom->json_type() != enum_json_type::J_OBJECT)
    return true;

  const Json_object *obj = down_cast<const Json_object *>(dom.get());
  StringPairs pairs;
  pairs.reserve(obj->cardinality());
  for (const auto &member : *obj) {
    if (member.second->json_type() != enum_json_type::J_STRING) return true;
    const Json_string *str =
        down_cast<const Json_string *>(member.second.get());
    pairs.emplace_back(member.first, str->value());
  }

  *out = std::move(pairs);
  return false;
}

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
