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
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, see <https://www.gnu.org/licenses/>.

// =============================================================================
// PREVIEW CAPABILITY — UNSTABLE API
// =============================================================================
// This header is part of the VEF preview surface. Its API and ABI may change
// or be removed without notice. See villagesql/preview/README.md for details.
// =============================================================================

#ifndef VILLAGESQL_PREVIEW_QUERY_HOOK_H
#define VILLAGESQL_PREVIEW_QUERY_HOOK_H

#include <cstdarg>
#include <cstddef>
#include <cstdio>
#include <string_view>

#include <villagesql/abi/preview/query_hook.h>
#include <villagesql/detail/capability_base.h>
#include <villagesql/detail/capability_traits.h>

namespace vsql::preview_query_hook {

// Typed read-only view of vef_query_hook_args_t. Use the accessors below
// rather than poking at the C struct directly.
class QueryHookArgs {
 public:
  explicit QueryHookArgs(const vef_query_hook_args_t *a) : a_(a) {}

  vef_query_hook_phase_t phase() const { return a_->phase; }

  std::string_view query() const {
    return a_->query ? std::string_view(a_->query, a_->query_len)
                     : std::string_view{};
  }

  const char *user() const { return a_->user; }
  const char *host() const { return a_->host; }
  unsigned long connection_id() const { return a_->connection_id; }
  uint16_t port() const { return a_->port; }
  bool in_transaction() const { return a_->in_transaction; }
  const char *sql_command() const { return a_->sql_command; }
  const char *schema() const { return a_->schema; }

  int status() const { return a_->status; }
  const char *sqlstate() const { return a_->sqlstate; }
  const char *error_message() const { return a_->error_message; }

  uint64_t query_start_utime() const { return a_->query_start_utime; }
  double query_time_secs() const { return a_->query_time_secs; }
  double lock_time_secs() const { return a_->lock_time_secs; }
  uint64_t rows_sent() const { return a_->rows_sent; }
  uint64_t rows_examined() const { return a_->rows_examined; }
  uint64_t rows_affected() const { return a_->rows_affected; }
  uint64_t bytes_sent() const { return a_->bytes_sent; }
  uint64_t bytes_received() const { return a_->bytes_received; }

 private:
  const vef_query_hook_args_t *a_;
};

// Writable result for a hook invocation. For POSTEXECUTE error_msg is
// advisory: the server logs it but does not propagate it to the client.
//
// The result struct owns a fixed-size buffer (VEF_MAX_ERROR_LEN); the
// printf-style API formats directly into it so there are no lifetime
// concerns about caller-owned strings outliving the hook return.
class QueryHookResult {
 public:
  explicit QueryHookResult(vef_query_hook_result_t *r) : r_(r) {}

  // Write a null-terminated, printf-formatted error message into the
  // result buffer. Output is truncated at VEF_MAX_ERROR_LEN - 1 bytes.
  __attribute__((format(printf, 2, 3))) void error_msg(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(r_->error_msg, VEF_MAX_ERROR_LEN, fmt, ap);
    va_end(ap);
  }

 private:
  vef_query_hook_result_t *r_;
};

// QueryHookCapability<Phase, Fn> declares a single hook that fires at Phase.
// Each unique (Phase, Fn) instantiation gets its own static capability_config.
//
// Usage:
//   static void slow_query_hook(const vsql::preview_query_hook::QueryHookArgs&,
//                               vsql::preview_query_hook::QueryHookResult&);
//
//   static vsql::preview_query_hook::QueryHookCapability<
//       VEF_QUERY_HOOK_POSTEXECUTE, &slow_query_hook> g_cap;
//
//   VEF_GENERATE_ENTRY_POINTS(make_extension().with(g_cap))
template <vef_query_hook_phase_t Phase,
          void (*Fn)(const QueryHookArgs &, QueryHookResult &)>
class QueryHookCapability
    : public ::vsql::detail::CapabilityBase<QueryHookCapability<Phase, Fn>> {
 public:
  QueryHookCapability() noexcept;

  // One static capability_config per (Phase, Fn) instantiation. Populated by
  // the constructor; CapabilityTraits::capability_config() returns its address
  // so the wire format carries a pointer to it.
  static inline vef_query_hook_cc_t cc{};

 private:
  template <typename Capability>
  friend struct ::vsql::detail::CapabilityTraits;

  const vef_preview_query_hook_t *abi_ = nullptr;
};

}  // namespace vsql::preview_query_hook

#include <villagesql/preview/detail/query_hook_register.h>
#include <villagesql/preview/query_hook_impl.h>

#endif  // VILLAGESQL_PREVIEW_QUERY_HOOK_H
