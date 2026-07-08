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

#include "villagesql/services/preview/statement_event.h"

#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "my_sys.h"
#include "my_systime.h"
#include "sql/auth/sql_security_ctx.h"
#include "sql/command_mapping.h"
#include "sql/sql_class.h"
#include "sql/sql_digest.h"
#include "sql/sql_lex.h"
#include "villagesql/include/error.h"

namespace villagesql::services {

namespace {

struct RegisteredHook {
  std::string extension_name;
  const vef_statement_event_cc_t *cc;
};

using HookList = std::vector<RegisteredHook>;

// Dispatch list. Writers (on_populate/on_depopulate) hold g_mu, copy the
// list, mutate the copy, and swap. Readers (on_statement_postexecute) take g_mu
// briefly to bump the shared_ptr refcount, then iterate without the lock
// so handlers cannot block INSTALL EXTENSION.
std::mutex g_mu;
std::shared_ptr<HookList> g_hooks{std::make_shared<HookList>()};

// Fast-path: number of registered hooks. on_statement_postexecute loads this
// with acquire and returns immediately when zero, avoiding the mutex and
// shared_ptr bump on every query when no extensions are installed.
std::atomic<size_t> g_hook_count{0};

// Drain counter: incremented (seq_cst) before snapshot(), decremented
// (release) after the dispatch loop. on_depopulate spins on this with acquire
// after removing the handler from g_hooks, ensuring no extension code runs
// after on_depopulate returns and dlclose becomes safe.
std::atomic<size_t> g_inflight{0};

struct ScopedInflightGuard {
  ScopedInflightGuard() { g_inflight.fetch_add(1, std::memory_order_seq_cst); }
  ~ScopedInflightGuard() { g_inflight.fetch_sub(1, std::memory_order_release); }
};

std::shared_ptr<HookList> snapshot() {
  std::lock_guard<std::mutex> lock(g_mu);
  return g_hooks;
}

vef_preview_statement_event_t g_statement_event_vtable{
    VEF_PREVIEW_STATEMENT_EVENT_ABI_VERSION};

}  // namespace

vef_preview_statement_event_t *preview_statement_event_vtable() {
  return &g_statement_event_vtable;
}

bool on_populate_statement_event(const PopulateContext &ctx,
                                 std::string &error_message) {
  if (ctx.capability_config == nullptr) return false;
  const auto *cc =
      static_cast<const vef_statement_event_cc_t *>(ctx.capability_config);
  if (cc->handler == nullptr) {
    error_message =
        "statement_event: capability_config has NULL handler function";
    return true;
  }
  // Reserved phases are declared in the ABI but not yet dispatched. Reject
  // at install time so extensions don't silently observe nothing.
  if (cc->phase != VEF_STATEMENT_EVENT_POSTEXECUTE) {
    error_message = "statement_event: phase " +
                    std::to_string(static_cast<int>(cc->phase)) +
                    " is reserved but not yet implemented";
    return true;
  }

  {
    std::lock_guard<std::mutex> lock(g_mu);
    auto new_list = std::make_shared<HookList>(*g_hooks);
    new_list->push_back({std::string(ctx.extension_name), cc});
    g_hooks = std::move(new_list);
  }
  g_hook_count.fetch_add(1, std::memory_order_release);

  LogVSQL(INFORMATION_LEVEL,
          "Registered statement event phase=%d from extension '%.*s'",
          static_cast<int>(cc->phase),
          static_cast<int>(ctx.extension_name.size()),
          ctx.extension_name.data());
  return false;
}

void on_depopulate_statement_event(const DepopulateContext &ctx) {
  if (ctx.capability_config == nullptr) return;
  const auto *cc =
      static_cast<const vef_statement_event_cc_t *>(ctx.capability_config);

  {
    std::lock_guard<std::mutex> lock(g_mu);
    auto new_list = std::make_shared<HookList>(*g_hooks);
    new_list->erase(
        std::remove_if(new_list->begin(), new_list->end(),
                       [&](const RegisteredHook &h) { return h.cc == cc; }),
        new_list->end());
    g_hooks = std::move(new_list);
  }
  g_hook_count.fetch_sub(1, std::memory_order_release);
  while (g_inflight.load(std::memory_order_acquire) > 0)
    std::this_thread::yield();
}

void on_statement_postexecute(THD *thd) {
  if (thd == nullptr) return;
  if (g_hook_count.load(std::memory_order_acquire) == 0) return;

  ScopedInflightGuard inflight_guard;
  auto hooks = snapshot();
  if (hooks->empty()) return;

  vef_statement_event_args_t args{};
  args.phase = VEF_STATEMENT_EVENT_POSTEXECUTE;

  // Use the server's rewritten (redacted) query when one exists, else the raw
  // text -- the same rule the general/slow/binary logs follow
  // (sql/sql_class.h: "If rewritten_query is non-empty, the rewritten query it
  // contains should be used in logs"). Password obfuscation is the rewriting
  // this guards: SET PASSWORD, CREATE/ALTER USER ... IDENTIFIED BY, replication
  // SOURCE_PASSWORD, CREATE SERVER OPTIONS(PASSWORD ...), etc., whose raw text
  // carries a cleartext secret. Without this the secret would reach every
  // statement_event sink. The hook fires on the owning connection thread
  // post-execute, so reading rewritten_query() needs no LOCK_thd_query.
  if (thd->rewritten_query().length() != 0) {
    args.query = thd->rewritten_query().ptr();
    args.query_len = thd->rewritten_query().length();
  } else {
    const LEX_CSTRING query = thd->query();
    args.query = query.str;
    args.query_len = query.length;
  }

  const Security_context *sctx = thd->security_context();
  args.user = sctx->priv_user().str;
  args.client_ip = sctx->ip().str;
  args.connection_id = thd->thread_id();
  args.port = thd->peer_port;
  args.in_transaction = thd->in_active_multi_stmt_transaction();
  args.sql_command =
      thd->lex ? get_sql_command_string(thd->lex->sql_command) : nullptr;

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

  // Per-statement deltas. copy_status_var_ptr holds a snapshot of status_var
  // taken at statement start; subtracting it gives the per-statement
  // contribution. When null (e.g. internal or optimizer-only statements that
  // skip the slow-query-log snapshot path), the raw cumulative session total
  // is returned instead.
  const System_status_var *snap = thd->copy_status_var_ptr;
  auto stat_delta = [&](ulonglong System_status_var::*field) -> uint64_t {
    return snap ? thd->status_var.*field - snap->*field
                : thd->status_var.*field;
  };

  args.bytes_sent = stat_delta(&System_status_var::bytes_sent);
  args.bytes_received = stat_delta(&System_status_var::bytes_received);

  args.schema = (thd->db().str != nullptr && thd->db().length > 0)
                    ? thd->db().str
                    : nullptr;

  // Warning count from the diagnostics area.
  args.warning_count = da != nullptr ? da->last_statement_cond_count() : 0;

  // Digest text — computed into a stack buffer; pointer is valid for the
  // duration of on_statement_postexecute.
  String digest_str;
  if (thd->m_digest != nullptr) {
    compute_digest_text(&thd->m_digest->m_digest_storage, &digest_str);
    args.digest_text =
        digest_str.length() > 0 ? digest_str.c_ptr_safe() : nullptr;
  }

  args.select_full_join =
      stat_delta(&System_status_var::select_full_join_count);
  args.select_full_range_join =
      stat_delta(&System_status_var::select_full_range_join_count);
  args.select_range = stat_delta(&System_status_var::select_range_count);
  args.select_range_check =
      stat_delta(&System_status_var::select_range_check_count);
  args.select_scan = stat_delta(&System_status_var::select_scan_count);
  args.sort_merge_passes =
      stat_delta(&System_status_var::filesort_merge_passes);
  args.sort_range = stat_delta(&System_status_var::filesort_range_count);
  args.sort_rows = stat_delta(&System_status_var::filesort_rows);
  args.sort_scan = stat_delta(&System_status_var::filesort_scan_count);
  args.created_tmp_tables = stat_delta(&System_status_var::created_tmp_tables);
  args.created_tmp_disk_tables =
      stat_delta(&System_status_var::created_tmp_disk_tables);

  args.no_index_used =
      (thd->server_status & SERVER_QUERY_NO_INDEX_USED) ? 1 : 0;
  args.no_good_index_used =
      (thd->server_status & SERVER_QUERY_NO_GOOD_INDEX_USED) ? 1 : 0;

  for (const auto &h : *hooks) {
    if (h.cc->phase != VEF_STATEMENT_EVENT_POSTEXECUTE) continue;
    char error_buf[VEF_MAX_ERROR_LEN]{};
    vef_statement_event_result_t result{};
    result.error_msg = error_buf;
    h.cc->handler(&args, &result);
    // Defensive: force the last byte to NUL so the %s log below cannot walk
    // off the end if an extension writes the buffer without null-terminating.
    error_buf[VEF_MAX_ERROR_LEN - 1] = '\0';
    if (error_buf[0] != '\0') {
      LogVSQL(WARNING_LEVEL, "Statement event error in extension '%s': %s",
              h.extension_name.c_str(), error_buf);
    }
  }
}

}  // namespace villagesql::services
