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

#include "villagesql/services/run_query.h"

#include <cassert>
#include <cstring>
#include <string>
#include <vector>

#include "mysql/components/my_service.h"
#include "mysql/components/services/mysql_admin_session.h"
#include "mysql/service_command.h"
#include "mysql/service_plugin_registry.h"
#include "mysql/service_srv_session.h"
#include "sql/sql_class.h"

namespace villagesql {
namespace services {

namespace {

// Internal context threaded through all command-service callbacks.
struct RunQueryCtx {
  vef_column_meta_fn meta_cb;
  vef_row_fn row_cb;
  void *user_ctx;

  // Column names collected during metadata phase.
  std::vector<std::string> col_names;
  // Pointers into col_names[i].c_str(), rebuilt before meta_cb call.
  std::vector<const char *> col_name_ptrs;

  // Per-row accumulation. col_idx tracks which column we are filling.
  unsigned int col_idx{0};
  std::vector<vef_col_value_t> row_values;
  // Storage backing the str pointers in row_values.
  std::vector<std::string> row_strings;

  vef_run_query_result_t result{VEF_QUERY_OK};
  std::string error_str;
  bool aborted{false};

  unsigned int col_count() const {
    return static_cast<unsigned int>(col_names.size());
  }

  // Write the current column value and advance col_idx.
  void set_col_str(std::string value) {
    if (col_idx >= row_values.size()) return;
    row_strings[col_idx] = std::move(value);
    row_values[col_idx].is_null = false;
    row_values[col_idx].str = row_strings[col_idx].c_str();
    row_values[col_idx].str_len = row_strings[col_idx].size();
    ++col_idx;
  }

  void set_col_null() {
    if (col_idx >= row_values.size()) return;
    row_strings[col_idx].clear();
    row_values[col_idx].is_null = true;
    row_values[col_idx].str = nullptr;
    row_values[col_idx].str_len = 0;
    ++col_idx;
  }
};

// ---- metadata callbacks ----

static int cb_start_result_metadata(void *ctx, uint num_cols, uint /*flags*/,
                                    const CHARSET_INFO * /*resultcs*/) {
  auto *qctx = static_cast<RunQueryCtx *>(ctx);
  qctx->col_names.clear();
  qctx->col_name_ptrs.clear();
  qctx->col_names.reserve(num_cols);
  qctx->col_name_ptrs.reserve(num_cols);
  qctx->row_values.resize(num_cols);
  qctx->row_strings.resize(num_cols);
  return 0;
}

static int cb_field_metadata(void *ctx, struct st_send_field *field,
                             const CHARSET_INFO * /*charset*/) {
  auto *qctx = static_cast<RunQueryCtx *>(ctx);
  qctx->col_names.emplace_back(field->col_name ? field->col_name : "");
  return 0;
}

static int cb_end_result_metadata(void *ctx, uint /*server_status*/,
                                  uint /*warn_count*/) {
  auto *qctx = static_cast<RunQueryCtx *>(ctx);
  if (qctx->meta_cb == nullptr) return 0;

  for (const auto &name : qctx->col_names)
    qctx->col_name_ptrs.push_back(name.c_str());

  int rc = qctx->meta_cb(qctx->col_name_ptrs.data(), qctx->col_count(),
                         qctx->user_ctx);
  if (rc != 0) {
    qctx->aborted = true;
    return 1;
  }
  return 0;
}

// ---- row callbacks ----

static int cb_start_row(void *ctx) {
  auto *qctx = static_cast<RunQueryCtx *>(ctx);
  if (qctx->aborted) return 1;
  qctx->col_idx = 0;
  unsigned int n = qctx->col_count();
  qctx->row_values.resize(n);
  qctx->row_strings.resize(n);
  return 0;
}

static int cb_end_row(void *ctx) {
  auto *qctx = static_cast<RunQueryCtx *>(ctx);
  if (qctx->aborted || qctx->row_cb == nullptr) return 0;

  int rc =
      qctx->row_cb(qctx->row_values.data(), qctx->col_count(), qctx->user_ctx);
  if (rc != 0) {
    qctx->aborted = true;
    return 1;
  }
  return 0;
}

static void cb_abort_row(void *ctx) {
  static_cast<RunQueryCtx *>(ctx)->aborted = true;
}

// ---- per-column value callbacks ----

static int cb_get_null(void *ctx) {
  static_cast<RunQueryCtx *>(ctx)->set_col_null();
  return 0;
}

// With CS_TEXT_REPRESENTATION, string-typed and most other columns arrive here.
static int cb_get_string(void *ctx, const char *value, size_t length,
                         const CHARSET_INFO * /*valuecs*/) {
  static_cast<RunQueryCtx *>(ctx)->set_col_str(
      std::string(value ? value : "", length));
  return 0;
}

// Integer columns (BIGINT etc.) may arrive via get_longlong even in text mode.
static int cb_get_longlong(void *ctx, longlong value, uint is_unsigned) {
  char buf[32];
  if (is_unsigned)
    snprintf(buf, sizeof(buf), "%llu", static_cast<unsigned long long>(value));
  else
    snprintf(buf, sizeof(buf), "%lld", static_cast<long long>(value));
  static_cast<RunQueryCtx *>(ctx)->set_col_str(buf);
  return 0;
}

static int cb_get_double(void *ctx, double value, uint32 /*decimals*/) {
  char buf[64];
  snprintf(buf, sizeof(buf), "%.17g", value);
  static_cast<RunQueryCtx *>(ctx)->set_col_str(buf);
  return 0;
}

static int cb_get_integer(void *ctx, longlong value) {
  return cb_get_longlong(ctx, value, 0);
}

// Decimal/date/time types: fall back to treating their value as a string.
// In CS_TEXT_REPRESENTATION these typically come via get_string instead.
static int cb_get_decimal(void * /*ctx*/, const decimal_t * /*value*/) {
  return 0;
}
static int cb_get_date(void * /*ctx*/, const MYSQL_TIME * /*value*/) {
  return 0;
}
static int cb_get_time(void * /*ctx*/, const MYSQL_TIME * /*value*/,
                       uint /*decimals*/) {
  return 0;
}
static int cb_get_datetime(void * /*ctx*/, const MYSQL_TIME * /*value*/,
                           uint /*decimals*/) {
  return 0;
}

// ---- status callbacks ----

static void cb_handle_ok(void * /*ctx*/, uint /*server_status*/,
                         uint /*warn_count*/, ulonglong /*affected_rows*/,
                         ulonglong /*last_insert_id*/,
                         const char * /*message*/) {}

static void cb_handle_error(void *ctx, uint sql_errno, const char *err_msg,
                            const char * /*sqlstate*/) {
  auto *qctx = static_cast<RunQueryCtx *>(ctx);
  qctx->result = VEF_QUERY_ERROR;
  char buf[VEF_MAX_ERROR_LEN];
  snprintf(buf, sizeof(buf), "MySQL error %u: %s", sql_errno,
           err_msg ? err_msg : "(unknown)");
  qctx->error_str = buf;
}

static void cb_shutdown(void * /*ctx*/, int /*server_shutdown*/) {}

static bool cb_connection_alive(void * /*ctx*/) { return true; }

static ulong cb_get_client_capabilities(void * /*ctx*/) { return 0; }

static const struct st_command_service_cbs kRunQueryCallbacks = {
    cb_start_result_metadata,
    cb_field_metadata,
    cb_end_result_metadata,
    cb_start_row,
    cb_end_row,
    cb_abort_row,
    cb_get_client_capabilities,
    cb_get_null,
    cb_get_integer,
    cb_get_longlong,
    cb_get_decimal,
    cb_get_double,
    cb_get_date,
    cb_get_time,
    cb_get_datetime,
    cb_get_string,
    cb_handle_ok,
    cb_handle_error,
    cb_shutdown,
    cb_connection_alive,
};

}  // namespace

vef_run_query_result_t run_query(const char *sql, size_t sql_len,
                                 vef_column_meta_fn meta_cb, vef_row_fn row_cb,
                                 void *ctx, char *error_msg) {
  assert(sql != nullptr);

  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry == nullptr) {
    if (error_msg)
      snprintf(error_msg, VEF_MAX_ERROR_LEN,
               "run_query: failed to acquire plugin registry");
    return VEF_QUERY_ERROR;
  }

  my_service<SERVICE_TYPE(mysql_admin_session)> admin_session_svc(
      "mysql_admin_session.mysql_server", registry);
  if (!admin_session_svc.is_valid()) {
    mysql_plugin_registry_release(registry);
    if (error_msg)
      snprintf(error_msg, VEF_MAX_ERROR_LEN,
               "run_query: mysql_admin_session service unavailable");
    return VEF_QUERY_ERROR;
  }

  MYSQL_SESSION session = admin_session_svc->open(nullptr, nullptr);
  if (session == nullptr) {
    mysql_plugin_registry_release(registry);
    if (error_msg)
      snprintf(error_msg, VEF_MAX_ERROR_LEN,
               "run_query: failed to open admin session");
    return VEF_QUERY_ERROR;
  }

  // Propagate the caller's current database into the admin session.
  if (current_thd != nullptr) {
    LEX_CSTRING caller_db = current_thd->db();
    if (caller_db.str != nullptr && caller_db.length > 0) {
      RunQueryCtx init_ctx;
      COM_DATA init_cmd;
      memset(&init_cmd, 0, sizeof(init_cmd));
      init_cmd.com_init_db.db_name = caller_db.str;
      init_cmd.com_init_db.length =
          static_cast<unsigned long>(caller_db.length);
      command_service_run_command(
          session, COM_INIT_DB, &init_cmd, &my_charset_utf8mb4_general_ci,
          &kRunQueryCallbacks, CS_TEXT_REPRESENTATION, &init_ctx);
    }
  }

  RunQueryCtx qctx;
  qctx.meta_cb = meta_cb;
  qctx.row_cb = row_cb;
  qctx.user_ctx = ctx;

  COM_DATA cmd;
  memset(&cmd, 0, sizeof(cmd));
  cmd.com_query.query = sql;
  cmd.com_query.length = static_cast<unsigned int>(sql_len);

  command_service_run_command(
      session, COM_QUERY, &cmd, &my_charset_utf8mb4_general_ci,
      &kRunQueryCallbacks, CS_TEXT_REPRESENTATION, &qctx);

  srv_session_close(session);
  mysql_plugin_registry_release(registry);

  if (qctx.aborted) return VEF_QUERY_ABORTED;
  if (qctx.result == VEF_QUERY_ERROR) {
    if (error_msg)
      snprintf(error_msg, VEF_MAX_ERROR_LEN, "%s", qctx.error_str.c_str());
    return VEF_QUERY_ERROR;
  }
  return VEF_QUERY_OK;
}

}  // namespace services
}  // namespace villagesql
