// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

#ifndef VILLAGESQL_VSQL_RUN_QUERY_H
#define VILLAGESQL_VSQL_RUN_QUERY_H

// run_query — execute SQL from an extension background thread
//
// run_query requires a vef_thread_t* handle, which the server passes to
// background thread entry points registered by the extension. This design
// enforces at compile time that run_query cannot be called from inside a VDF,
// since VDF callbacks do not receive a vef_thread_t handle.
//
// Usage (from a background thread entry point):
//
//   #include <villagesql/vsql/run_query.h>
//
//   void my_thread(vef_thread_t *thread) {
//     // Collect SHOW GLOBAL STATUS into a vector of {name, value} pairs.
//     std::vector<std::pair<std::string, std::string>> rows;
//     auto result = villagesql::run_query(
//         thread,
//         "SHOW GLOBAL STATUS",
//         [&](const std::vector<std::string_view> &cols) { /* optional */ },
//         [&](const std::vector<std::string_view> &vals) {
//             rows.push_back({std::string(vals[0]), std::string(vals[1])});
//         });
//     if (result != VEF_QUERY_OK) { /* handle error */ }
//
//     // The simpler overload with no callbacks is useful for SET / DDL:
//     villagesql::run_query(thread, "SET SESSION sql_mode = ''");
//   }

#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include <villagesql/abi/types.h>

namespace villagesql {

// Extension-local storage for the run_query function pointer, set during
// vef_register() by vef_register_impl() in extension_builder.h.
inline vef_run_query_fn g_run_query = nullptr;

// Meta callback type: called once with column names before the first row.
using RunQueryMetaCb =
    std::function<void(const std::vector<std::string_view> &col_names)>;

// Row callback type: called once per result row with column values as strings.
// SQL NULL is represented as a default-constructed string_view (data() ==
// nullptr). An empty (non-NULL) string has data() != nullptr and size() == 0.
// To distinguish NULL from empty string, check data() == nullptr.
using RunQueryRowCb =
    std::function<void(const std::vector<std::string_view> &values)>;

// Execute a SQL statement. Returns VEF_QUERY_OK, VEF_QUERY_ERROR, or
// VEF_QUERY_ABORTED. On error, error_msg (if non-null, must point to a buffer
// of at least VEF_MAX_ERROR_LEN bytes) receives a description.
// inline: header-only implementation to avoid ODR violations across TUs.
inline vef_run_query_result_t run_query(vef_thread_t *thread,
                                        std::string_view sql,
                                        RunQueryMetaCb meta_cb,
                                        RunQueryRowCb row_cb,
                                        char *error_msg = nullptr) {
  if (g_run_query == nullptr) {
    if (error_msg)
      snprintf(error_msg, VEF_MAX_ERROR_LEN, "run_query service unavailable");
    return VEF_QUERY_ERROR;
  }

  // Wrap C++ lambdas in a struct to pass through the C callbacks.
  struct Ctx {
    RunQueryMetaCb meta_cb;
    RunQueryRowCb row_cb;
    unsigned int col_count{0};
    std::vector<std::string_view> col_names_view;
    std::vector<std::string_view> row_view;
  };
  Ctx c{std::move(meta_cb), std::move(row_cb)};

  auto c_meta = [](const char *const *col_names, unsigned int col_count,
                   void *ctx) -> int {
    auto *c = static_cast<Ctx *>(ctx);
    c->col_count = col_count;
    c->col_names_view.resize(col_count);
    c->row_view.resize(col_count);
    for (unsigned int i = 0; i < col_count; ++i)
      c->col_names_view[i] = col_names[i] ? col_names[i] : "";
    if (c->meta_cb) c->meta_cb(c->col_names_view);
    return 0;
  };

  auto c_row = [](const vef_col_value_t *values, unsigned int col_count,
                  void *ctx) -> int {
    auto *c = static_cast<Ctx *>(ctx);
    if (!c->row_cb) return 0;
    c->row_view.resize(col_count);
    for (unsigned int i = 0; i < col_count; ++i) {
      if (values[i].is_null)
        c->row_view[i] = {};
      else
        c->row_view[i] = {values[i].str, values[i].str_len};
    }
    c->row_cb(c->row_view);
    return 0;
  };

  return g_run_query(thread, sql.data(), sql.size(), c_meta, c_row, &c,
                     error_msg);
}

// Convenience overload: execute a statement with no result processing.
inline vef_run_query_result_t run_query(vef_thread_t *thread,
                                        std::string_view sql,
                                        char *error_msg = nullptr) {
  return run_query(thread, sql, nullptr, nullptr, error_msg);
}

}  // namespace villagesql

#endif  // VILLAGESQL_VSQL_RUN_QUERY_H
