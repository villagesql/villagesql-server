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

#include "villagesql/services/preview/sql_query.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "mysql/service_command.h"
#include "mysql/service_srv_session.h"
#include "mysql/strings/m_ctype.h"
#include "sql/sql_class.h"
#include "sql/sql_error.h"
#include "sql/srv_session.h"
#include "villagesql/include/error.h"
#include "villagesql/services/preview/thread_worker.h"

// Full definitions of the opaque ABI types, in the global namespace to match
// their forward declarations in abi/preview/sql_query.h.

// Stores the thread handle so that sql_execute can create a fresh Srv_session
// per query. The borrowed-THD Srv_session transitions through
// ASSOCIATE → ASSOCIATED → DISASSOCIATED on each execute and cannot be reused.
struct vef_sql_session_t {
  vef_thread_handle_t *handle{nullptr};
};

struct vef_sql_result_t {
  unsigned int num_columns{0};

  struct Cell {
    std::string value;
    bool is_null{false};
  };
  std::vector<std::vector<Cell>> rows;
  size_t current_row{0};

  std::vector<const char *> row_ptrs;
  std::vector<unsigned long> row_lengths;

  struct Diag {
    uint32_t errno_{0};
    vef_sql_diag_severity_t severity{VEF_SQL_DIAG_ERROR};
    char sqlstate[6]{"00000"};
    std::string message;
  };
  bool has_error{false};
  Diag error;
  std::vector<Diag> warnings;
};

// Common base for ExecCtx and ForEachCtx so the shared cb_handle_error
// callback can recover the result handle from either ctx type via a single
// static_cast from void*.
struct CtxBase {
  vef_sql_result_t *result{nullptr};
};

// Context for the streaming for_each_row path.
struct ForEachCtx : CtxBase {
  unsigned int num_columns{0};
  unsigned int col_idx{0};
  vef_sql_row_cb cb{nullptr};
  void *user_ctx{nullptr};
  bool stop{false};

  // Per-row storage reused each row (avoids per-row allocation).
  std::vector<std::string> values;
  std::vector<bool> is_null;
  std::vector<const char *> row_ptrs;
  std::vector<unsigned long> row_lengths;
};

namespace villagesql::services {

namespace {

struct ExecCtx : CtxBase {
  unsigned int col_idx{0};
};

static int cb_start_result_metadata(void *ctx, uint num_cols, uint,
                                    const CHARSET_INFO *) {
  static_cast<ExecCtx *>(ctx)->result->num_columns = num_cols;
  return 0;
}

static int cb_field_metadata(void *, struct st_send_field *,
                             const CHARSET_INFO *) {
  return 0;
}

static int cb_end_result_metadata(void *, uint, uint) { return 0; }

static int cb_start_row(void *ctx) {
  auto *c = static_cast<ExecCtx *>(ctx);
  c->result->rows.emplace_back(c->result->num_columns);
  c->col_idx = 0;
  return 0;
}

static int cb_end_row(void *ctx) {
  auto *c = static_cast<ExecCtx *>(ctx);
  auto &row = c->result->rows.back();
  while (c->col_idx < row.size()) {
    row[c->col_idx].is_null = true;
    ++c->col_idx;
  }
  return 0;
}

static void cb_abort_row(void *ctx) {
  auto *c = static_cast<ExecCtx *>(ctx);
  if (!c->result->rows.empty()) c->result->rows.pop_back();
}

static ulong cb_get_client_capabilities(void *) { return 0; }

static int cb_get_null(void *ctx) {
  auto *c = static_cast<ExecCtx *>(ctx);
  auto &row = c->result->rows.back();
  if (c->col_idx < row.size()) {
    row[c->col_idx].is_null = true;
    ++c->col_idx;
  }
  return 0;
}

static int cb_get_string(void *ctx, const char *value, size_t length,
                         const CHARSET_INFO *) {
  auto *c = static_cast<ExecCtx *>(ctx);
  auto &row = c->result->rows.back();
  if (c->col_idx < row.size()) {
    row[c->col_idx].value.assign(value, length);
    row[c->col_idx].is_null = false;
    ++c->col_idx;
  }
  return 0;
}

static int cb_get_longlong(void *ctx, longlong value, uint is_unsigned) {
  char buf[32];
  if (is_unsigned)
    snprintf(buf, sizeof(buf), "%llu", static_cast<unsigned long long>(value));
  else
    snprintf(buf, sizeof(buf), "%lld", value);
  return cb_get_string(ctx, buf, strlen(buf), nullptr);
}

static int cb_get_integer(void *ctx, longlong value) {
  return cb_get_longlong(ctx, value, 0);
}

static int cb_get_double(void *ctx, double value, uint32) {
  char buf[32];
  snprintf(buf, sizeof(buf), "%.17g", value);
  return cb_get_string(ctx, buf, strlen(buf), nullptr);
}

static int cb_get_decimal(void *, const decimal_t *) { return 0; }
static int cb_get_date(void *, const MYSQL_TIME *) { return 0; }
static int cb_get_time(void *, const MYSQL_TIME *, uint) { return 0; }
static int cb_get_datetime(void *, const MYSQL_TIME *, uint) { return 0; }

static void cb_handle_ok(void *, uint, uint, ulonglong, ulonglong,
                         const char *) {}

static vef_sql_result_t *result_from_ctx(void *ctx) {
  return static_cast<CtxBase *>(ctx)->result;
}

static void cb_handle_error(void *ctx, uint sql_errno, const char *err_msg,
                            const char *sqlstate) {
  vef_sql_result_t *r = result_from_ctx(ctx);
  if (should_assert_if_null(r)) {
    LogVSQL(ERROR_LEVEL, "sql_query: query error %u: %s", sql_errno,
            err_msg ? err_msg : "(unknown)");
    return;
  }
  r->has_error = true;
  r->error.errno_ = sql_errno;
  r->error.severity = VEF_SQL_DIAG_ERROR;
  if (sqlstate != nullptr) {
    std::strncpy(r->error.sqlstate, sqlstate, sizeof(r->error.sqlstate) - 1);
    r->error.sqlstate[sizeof(r->error.sqlstate) - 1] = '\0';
  }
  if (err_msg != nullptr) r->error.message.assign(err_msg);
}

// Iterate the THD's Diagnostics_area and copy warning/note conditions into
// the result. Errors are already captured via cb_handle_error.
static void collect_warnings(THD *thd, vef_sql_result_t *result) {
  if (thd == nullptr || result == nullptr) return;
  Diagnostics_area *da = thd->get_stmt_da();
  if (da == nullptr) return;
  Diagnostics_area::Sql_condition_iterator it = da->sql_conditions();
  const Sql_condition *cond;
  while ((cond = it++) != nullptr) {
    Sql_condition::enum_severity_level lvl = cond->severity();
    vef_sql_diag_severity_t sev;
    switch (lvl) {
      case Sql_condition::SL_NOTE:
        sev = VEF_SQL_DIAG_NOTE;
        break;
      case Sql_condition::SL_WARNING:
        sev = VEF_SQL_DIAG_WARNING;
        break;
      default:
        // SL_ERROR conditions are already surfaced via cb_handle_error;
        // skip them here so they aren't reported twice.
        continue;
    }
    vef_sql_result_t::Diag d;
    d.errno_ = cond->mysql_errno();
    d.severity = sev;
    const char *ss = cond->returned_sqlstate();
    if (ss != nullptr) {
      // Not strncpy(): GCC's -Wstringop-truncation fires on the (safe) case
      // where the source is exactly as long as the destination minus its
      // terminator, which is the normal case for a 5-character SQLSTATE.
      const size_t ss_len = std::min(std::strlen(ss), sizeof(d.sqlstate) - 1);
      std::memcpy(d.sqlstate, ss, ss_len);
      d.sqlstate[ss_len] = '\0';
    }
    const char *msg = cond->message_text();
    if (msg != nullptr) d.message.assign(msg, cond->message_octet_length());
    result->warnings.push_back(std::move(d));
  }
}

static void cb_shutdown(void *, int) {}
static bool cb_connection_alive(void *) { return true; }

// Streaming callbacks for for_each_row.

static int fe_start_result_metadata(void *ctx, uint num_cols, uint,
                                    const CHARSET_INFO *) {
  auto *c = static_cast<ForEachCtx *>(ctx);
  c->num_columns = num_cols;
  c->result->num_columns = num_cols;
  c->values.assign(num_cols, std::string{});
  c->is_null.assign(num_cols, false);
  c->row_ptrs.resize(num_cols);
  c->row_lengths.resize(num_cols);
  return 0;
}

static int fe_field_metadata(void *, struct st_send_field *,
                             const CHARSET_INFO *) {
  return 0;
}

static int fe_end_result_metadata(void *, uint, uint) { return 0; }

static int fe_start_row(void *ctx) {
  auto *c = static_cast<ForEachCtx *>(ctx);
  c->col_idx = 0;
  for (unsigned int i = 0; i < c->num_columns; ++i) {
    c->values[i].clear();
    c->is_null[i] = false;
  }
  return 0;
}

static int fe_end_row(void *ctx) {
  auto *c = static_cast<ForEachCtx *>(ctx);
  // MySQL provides no way to abort a running query mid-result; set stop=true
  // and discard remaining rows until command_service_run_command returns.
  if (c->stop) return 0;
  for (unsigned int i = 0; i < c->num_columns; ++i) {
    if (c->is_null[i]) {
      c->row_ptrs[i] = nullptr;
      c->row_lengths[i] = 0;
    } else {
      c->row_ptrs[i] = c->values[i].c_str();
      c->row_lengths[i] = static_cast<unsigned long>(c->values[i].size());
    }
  }
  if (!c->cb(c->row_ptrs.data(), c->row_lengths.data(), c->num_columns,
             c->user_ctx))
    c->stop = true;
  return 0;
}

static void fe_abort_row(void *) {}

static int fe_get_null(void *ctx) {
  auto *c = static_cast<ForEachCtx *>(ctx);
  if (c->col_idx < c->num_columns) {
    c->is_null[c->col_idx] = true;
    ++c->col_idx;
  }
  return 0;
}

static int fe_get_string(void *ctx, const char *value, size_t length,
                         const CHARSET_INFO *) {
  auto *c = static_cast<ForEachCtx *>(ctx);
  if (c->col_idx < c->num_columns) {
    c->values[c->col_idx].assign(value, length);
    c->is_null[c->col_idx] = false;
    ++c->col_idx;
  }
  return 0;
}

static int fe_get_longlong(void *ctx, longlong value, uint is_unsigned) {
  char buf[32];
  if (is_unsigned)
    snprintf(buf, sizeof(buf), "%llu", static_cast<unsigned long long>(value));
  else
    snprintf(buf, sizeof(buf), "%lld", value);
  return fe_get_string(ctx, buf, strlen(buf), nullptr);
}

static int fe_get_integer(void *ctx, longlong value) {
  return fe_get_longlong(ctx, value, 0);
}

static int fe_get_double(void *ctx, double value, uint32) {
  char buf[32];
  snprintf(buf, sizeof(buf), "%.17g", value);
  return fe_get_string(ctx, buf, strlen(buf), nullptr);
}

static const struct st_command_service_cbs kForEachCallbacks = {
    fe_start_result_metadata,
    fe_field_metadata,
    fe_end_result_metadata,
    fe_start_row,
    fe_end_row,
    fe_abort_row,
    cb_get_client_capabilities,
    fe_get_null,
    fe_get_integer,
    fe_get_longlong,
    cb_get_decimal,
    fe_get_double,
    cb_get_date,
    cb_get_time,
    cb_get_datetime,
    fe_get_string,
    cb_handle_ok,
    cb_handle_error,
    cb_shutdown,
    cb_connection_alive,
};

static const struct st_command_service_cbs kCallbacks = {
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

static vef_sql_session_t *sql_open_session(vef_thread_handle_t *handle) {
  if (handle == nullptr || handle->thd == nullptr) return nullptr;
  auto *session = new (std::nothrow) vef_sql_session_t;
  if (session == nullptr) return nullptr;
  session->handle = handle;
  return session;
}

static void sql_close_session(vef_sql_session_t *session) { delete session; }

static vef_sql_result_t *sql_execute(vef_sql_session_t *session,
                                     const char *sql, uint64_t sql_len) {
  if (session == nullptr || session->handle == nullptr ||
      session->handle->thd == nullptr)
    return nullptr;

  // Create a fresh Srv_session per execute. The borrowed-THD Srv_session
  // transitions ASSOCIATE → ASSOCIATED → DISASSOCIATED on each use and cannot
  // be reused across calls. Creating it here preserves the worker THD's
  // skip_grants() security context for each query.
  static auto noop_cb = [](void *, unsigned int, const char *) noexcept {};
  std::unique_ptr<Srv_session> srv(
      new (std::nothrow) Srv_session(static_cast<srv_session_error_cb>(noop_cb),
                                     nullptr, session->handle->thd));
  if (srv == nullptr) {
    LogVSQL(ERROR_LEVEL, "sql_query: failed to create Srv_session");
    return nullptr;
  }

  std::unique_ptr<vef_sql_result_t> result(new (std::nothrow) vef_sql_result_t);
  if (result == nullptr) {
    return nullptr;
  }

  ExecCtx ctx;
  ctx.result = result.get();

  COM_DATA cmd;
  memset(&cmd, 0, sizeof(cmd));
  cmd.com_query.query = sql;
  cmd.com_query.length = static_cast<unsigned int>(sql_len);

  command_service_run_command(srv.get(), COM_QUERY, &cmd,
                              &my_charset_utf8mb4_general_ci, &kCallbacks,
                              CS_TEXT_REPRESENTATION, &ctx);

  collect_warnings(session->handle->thd, result.get());

  return result.release();
}

static vef_sql_result_t *sql_for_each_row(vef_sql_session_t *session,
                                          const char *sql, uint64_t sql_len,
                                          vef_sql_row_cb cb, void *ctx) {
  if (session == nullptr || session->handle == nullptr ||
      session->handle->thd == nullptr || cb == nullptr)
    return nullptr;

  static auto noop_cb = [](void *, unsigned int, const char *) noexcept {};
  std::unique_ptr<Srv_session> srv(
      new (std::nothrow) Srv_session(static_cast<srv_session_error_cb>(noop_cb),
                                     nullptr, session->handle->thd));
  if (srv == nullptr) {
    LogVSQL(ERROR_LEVEL, "sql_query: failed to create Srv_session");
    return nullptr;
  }

  std::unique_ptr<vef_sql_result_t> result(new (std::nothrow) vef_sql_result_t);
  if (result == nullptr) {
    return nullptr;
  }

  ForEachCtx fe_ctx;
  fe_ctx.result = result.get();
  fe_ctx.cb = cb;
  fe_ctx.user_ctx = ctx;

  COM_DATA cmd;
  memset(&cmd, 0, sizeof(cmd));
  cmd.com_query.query = sql;
  cmd.com_query.length = static_cast<unsigned int>(sql_len);

  command_service_run_command(
      srv.get(), COM_QUERY, &cmd, &my_charset_utf8mb4_general_ci,
      &kForEachCallbacks, CS_TEXT_REPRESENTATION, &fe_ctx);

  collect_warnings(session->handle->thd, result.get());

  return result.release();
}

static bool sql_fetch_row(vef_sql_result_t *result, const char ***row_out,
                          const unsigned long **lengths_out) {
  if (result == nullptr || result->current_row >= result->rows.size())
    return false;

  const auto &row = result->rows[result->current_row];
  unsigned int n = static_cast<unsigned int>(row.size());

  result->row_ptrs.resize(n);
  result->row_lengths.resize(n);

  for (unsigned int i = 0; i < n; ++i) {
    if (row[i].is_null) {
      result->row_ptrs[i] = nullptr;
      result->row_lengths[i] = 0;
    } else {
      result->row_ptrs[i] = row[i].value.c_str();
      result->row_lengths[i] = static_cast<unsigned long>(row[i].value.size());
    }
  }

  *row_out = result->row_ptrs.data();
  *lengths_out = result->row_lengths.data();
  ++result->current_row;
  return true;
}

static unsigned int sql_num_columns(vef_sql_result_t *result) {
  if (result == nullptr) return 0;
  return result->num_columns;
}

static void sql_close_result(vef_sql_result_t *result) { delete result; }

static void fill_diag(const vef_sql_result_t::Diag &src, vef_sql_diag_t *out) {
  out->errno_ = src.errno_;
  out->severity = src.severity;
  out->sqlstate = src.sqlstate;
  out->message = src.message.c_str();
  out->message_len = src.message.size();
}

static bool sql_has_error(const vef_sql_result_t *result) {
  return result != nullptr && result->has_error;
}

static bool sql_get_error(const vef_sql_result_t *result, vef_sql_diag_t *out) {
  if (result == nullptr || !result->has_error || out == nullptr) return false;
  fill_diag(result->error, out);
  return true;
}

static unsigned int sql_warning_count(const vef_sql_result_t *result) {
  if (result == nullptr) return 0;
  return static_cast<unsigned int>(result->warnings.size());
}

static bool sql_get_warning(const vef_sql_result_t *result, unsigned int i,
                            vef_sql_diag_t *out) {
  if (result == nullptr || out == nullptr || i >= result->warnings.size())
    return false;
  fill_diag(result->warnings[i], out);
  return true;
}

namespace {
vef_preview_sql_query_t g_sql_query_vtable{VEF_PREVIEW_SQL_QUERY_ABI_VERSION,
                                           &sql_open_session,
                                           &sql_close_session,
                                           &sql_execute,
                                           &sql_fetch_row,
                                           &sql_num_columns,
                                           &sql_close_result,
                                           &sql_for_each_row,
                                           &sql_has_error,
                                           &sql_get_error,
                                           &sql_warning_count,
                                           &sql_get_warning};
}  // namespace

vef_preview_sql_query_t *preview_sql_query_vtable() {
  return &g_sql_query_vtable;
}

}  // namespace villagesql::services
