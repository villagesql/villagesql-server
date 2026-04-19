// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is designed to work with certain software (including
// but not limited to OpenSSL) that is licensed under separate terms,
// as designated in a particular file or component or in included license
// documentation.  The authors of MySQL hereby grant you an additional
// permission to link the program and your derivative works with the
// separately licensed software that they have either included with
// the program or referenced in the documentation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

// VillageSQL extension for testing the run_query service.
//
// Exposes two VDFs:
//
//   run_query_count(sql VARCHAR) RETURNS INT
//     Executes the given SQL and returns the number of rows returned.
//     Returns NULL if sql is NULL or on SQL error (sets a warning on error).
//
//   run_query_first_col(sql VARCHAR) RETURNS VARCHAR
//     Executes the given SQL and returns the first column of the first row
//     as a string. Returns NULL if sql is NULL, the result set is empty,
//     the first value is SQL NULL, or on SQL error (sets a warning on error).

#include <villagesql/vsql.h>

using namespace vsql;

// run_query_count(sql) - returns the number of rows produced by sql.
void run_query_count(StringArg sql, IntResult out) {
  if (sql.is_null()) {
    out.set_null();
    return;
  }

  long long count = 0;
  char error_msg[VEF_MAX_ERROR_LEN] = {};

  vef_run_query_result_t rc = villagesql::run_query(
      sql.value(), nullptr,
      [&](const std::vector<std::string_view> & /*vals*/) { ++count; },
      error_msg);

  if (rc == VEF_QUERY_ERROR) {
    out.warning(error_msg);
    return;
  }

  out.set(count);
}

// run_query_first_col(sql) - returns the first column of the first row.
void run_query_first_col(StringArg sql, StringResult out) {
  if (sql.is_null()) {
    out.set_null();
    return;
  }

  bool got_row = false;
  std::string first_value;
  bool first_is_null = false;
  char error_msg[VEF_MAX_ERROR_LEN] = {};

  vef_run_query_result_t rc = villagesql::run_query(
      sql.value(), nullptr,
      [&](const std::vector<std::string_view> &vals) {
        if (got_row) return;  // only keep the first row
        got_row = true;
        if (vals.empty() || vals[0].data() == nullptr) {
          first_is_null = true;
        } else {
          first_value = std::string(vals[0]);
        }
      },
      error_msg);

  if (rc == VEF_QUERY_ERROR) {
    out.warning(error_msg);
    return;
  }

  if (!got_row || first_is_null) {
    out.set_null();
    return;
  }

  auto buf = out.buffer();
  size_t len = std::min(first_value.size(), buf.size());
  memcpy(buf.data(), first_value.data(), len);
  out.set_length(len);
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension("vsql_run_query_test", "0.0.1")
        .func(make_func<&run_query_count>("run_query_count")
                  .returns(INT)
                  .param(STRING)
                  .build())
        .func(make_func<&run_query_first_col>("run_query_first_col")
                  .returns(STRING)
                  .param(STRING)
                  .build()))
