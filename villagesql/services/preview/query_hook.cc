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

#include "villagesql/services/preview/query_hook.h"

#include <algorithm>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "my_sys.h"
#include "my_systime.h"
#include "sql/auth/sql_security_ctx.h"
#include "sql/command_mapping.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "villagesql/include/error.h"

namespace villagesql::services {

namespace {

struct RegisteredHook {
  std::string extension_name;
  const vef_query_hook_cc_t *cc;
};

using HookList = std::vector<RegisteredHook>;

// Dispatch list. Writers (on_populate/on_depopulate) hold g_mu, copy the
// list, mutate the copy, and swap. Readers (on_query_status_end) take g_mu
// briefly to bump the shared_ptr refcount, then iterate without the lock
// so hook callbacks cannot block INSTALL EXTENSION.
std::mutex g_mu;
std::shared_ptr<HookList> g_hooks{std::make_shared<HookList>()};

std::shared_ptr<HookList> snapshot() {
  std::lock_guard<std::mutex> lock(g_mu);
  return g_hooks;
}

vef_preview_query_hook_t g_query_hook_vtable{
    VEF_PREVIEW_QUERY_HOOK_ABI_VERSION};

}  // namespace

vef_preview_query_hook_t *preview_query_hook_vtable() {
  return &g_query_hook_vtable;
}

bool on_populate_query_hook(const PopulateContext &ctx,
                            std::string &error_message) {
  if (ctx.capability_config == nullptr) return false;
  const auto *cc =
      static_cast<const vef_query_hook_cc_t *>(ctx.capability_config);
  if (cc->hook == nullptr) {
    error_message = "query_hook: capability_config has NULL hook function";
    return true;
  }
  // Reserved phases are declared in the ABI but not yet dispatched. Reject
  // at install time so extensions don't silently observe nothing.
  if (cc->phase != VEF_QUERY_HOOK_POSTEXECUTE) {
    error_message = "query_hook: phase " +
                    std::to_string(static_cast<int>(cc->phase)) +
                    " is reserved but not yet implemented";
    return true;
  }

  std::lock_guard<std::mutex> lock(g_mu);
  auto new_list = std::make_shared<HookList>(*g_hooks);
  new_list->push_back({std::string(ctx.extension_name), cc});
  g_hooks = std::move(new_list);

  LogVSQL(
      INFORMATION_LEVEL, "Registered query hook phase=%d from extension '%.*s'",
      static_cast<int>(cc->phase), static_cast<int>(ctx.extension_name.size()),
      ctx.extension_name.data());
  return false;
}

void on_depopulate_query_hook(const DepopulateContext &ctx) {
  if (ctx.capability_config == nullptr) return;
  const auto *cc =
      static_cast<const vef_query_hook_cc_t *>(ctx.capability_config);

  std::lock_guard<std::mutex> lock(g_mu);
  auto new_list = std::make_shared<HookList>(*g_hooks);
  new_list->erase(
      std::remove_if(new_list->begin(), new_list->end(),
                     [&](const RegisteredHook &h) { return h.cc == cc; }),
      new_list->end());
  g_hooks = std::move(new_list);
}

void on_query_status_end(THD *thd) {
  if (thd == nullptr) return;

  auto hooks = snapshot();
  if (hooks->empty()) return;

  vef_query_hook_args_t args{};
  args.phase = VEF_QUERY_HOOK_POSTEXECUTE;

  const LEX_CSTRING query = thd->query();
  args.query = query.str;
  args.query_len = query.length;

  const Security_context *sctx = thd->security_context();
  args.user = sctx->priv_user().str;
  args.host = sctx->ip().str;
  args.connection_id = thd->thread_id();
  args.port = thd->peer_port;
  args.in_transaction = thd->in_active_multi_stmt_transaction();
  args.sql_command = get_sql_command_string(thd->lex->sql_command);

  const Diagnostics_area *da = thd->get_stmt_da();
  if (da != nullptr && da->is_error()) {
    args.status = static_cast<int>(da->mysql_errno());
    args.sqlstate = da->returned_sqlstate();
    args.error_message = da->message_text();
  }

  args.query_start_utime = thd->start_utime;
  if (thd->start_utime != 0) {
    const ulonglong elapsed_us = my_micro_time() - thd->start_utime;
    args.query_time_secs = static_cast<double>(elapsed_us) / 1000000.0;
  }
  args.lock_time_secs = static_cast<double>(thd->get_lock_usec()) / 1000000.0;
  args.rows_sent = thd->get_sent_row_count();
  args.rows_examined = thd->get_examined_row_count();
  args.rows_affected = (da != nullptr && da->is_ok()) ? da->affected_rows() : 0;
  args.bytes_sent = thd->status_var.bytes_sent;
  args.bytes_received = thd->status_var.bytes_received;

  args.schema = (thd->db().str != nullptr && thd->db().length > 0)
                    ? thd->db().str
                    : nullptr;

  for (const auto &h : *hooks) {
    if (h.cc->phase != VEF_QUERY_HOOK_POSTEXECUTE) continue;
    char error_buf[VEF_MAX_ERROR_LEN]{};
    vef_query_hook_result_t result{};
    result.error_msg = error_buf;
    h.cc->hook(&args, &result);
    // Defensive: force the last byte to NUL so the %s log below cannot walk
    // off the end if an extension writes the buffer without null-terminating.
    error_buf[VEF_MAX_ERROR_LEN - 1] = '\0';
    if (error_buf[0] != '\0') {
      LogVSQL(WARNING_LEVEL, "Query hook error in extension '%s': %s",
              h.extension_name.c_str(), error_buf);
    }
  }
}

}  // namespace villagesql::services
