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

#ifndef VILLAGESQL_PREVIEW_STATEMENT_EVENT_H
#define VILLAGESQL_PREVIEW_STATEMENT_EVENT_H

#include <cstdarg>
#include <cstddef>
#include <cstdio>
#include <string_view>

#include <villagesql/abi/preview/statement_event.h>
#include <villagesql/detail/capability_base.h>
#include <villagesql/detail/capability_traits.h>

namespace vsql::preview_statement_event {

// Typed read-only view of vef_statement_event_args_t. Use the accessors below
// rather than poking at the C struct directly.
class StatementEventArgs {
 public:
  explicit StatementEventArgs(const vef_statement_event_args_t *a) : a_(a) {}

  vef_statement_event_phase_t phase() const { return a_->phase; }

  std::string_view query() const {
    return a_->query ? std::string_view(a_->query, a_->query_len)
                     : std::string_view{};
  }

  const char *user() const { return a_->user; }
  const char *client_ip() const { return a_->client_ip; }
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

  uint64_t warning_count() const { return a_->warning_count; }
  const char *digest_text() const { return a_->digest_text; }
  const char *digest_hash() const { return a_->digest_hash; }

  uint64_t select_full_join() const { return a_->select_full_join; }
  uint64_t select_full_range_join() const { return a_->select_full_range_join; }
  uint64_t select_range() const { return a_->select_range; }
  uint64_t select_range_check() const { return a_->select_range_check; }
  uint64_t select_scan() const { return a_->select_scan; }

  uint64_t sort_merge_passes() const { return a_->sort_merge_passes; }
  uint64_t sort_range() const { return a_->sort_range; }
  uint64_t sort_rows() const { return a_->sort_rows; }
  uint64_t sort_scan() const { return a_->sort_scan; }

  uint64_t created_tmp_tables() const { return a_->created_tmp_tables; }
  uint64_t created_tmp_disk_tables() const {
    return a_->created_tmp_disk_tables;
  }

  bool no_index_used() const { return a_->no_index_used != 0; }
  bool no_good_index_used() const { return a_->no_good_index_used != 0; }

  uint64_t read_first() const { return a_->read_first; }
  uint64_t read_last() const { return a_->read_last; }
  uint64_t read_key() const { return a_->read_key; }
  uint64_t read_next() const { return a_->read_next; }
  uint64_t read_prev() const { return a_->read_prev; }
  uint64_t read_rnd() const { return a_->read_rnd; }
  uint64_t read_rnd_next() const { return a_->read_rnd_next; }

 private:
  const vef_statement_event_args_t *a_;
};

// Writable result for a handler invocation. For POSTEXECUTE error_msg is
// advisory: the server logs it but does not propagate it to the client.
//
// The result struct owns a fixed-size buffer (VEF_MAX_ERROR_LEN); the
// printf-style API formats directly into it so there are no lifetime
// concerns about caller-owned strings outliving the handler return.
class StatementEventResult {
 public:
  explicit StatementEventResult(vef_statement_event_result_t *r) : r_(r) {}

  // Write a null-terminated, printf-formatted error message into the
  // result buffer. Output is truncated at VEF_MAX_ERROR_LEN - 1 bytes.
  __attribute__((format(printf, 2, 3))) void error_msg(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(r_->error_msg, VEF_MAX_ERROR_LEN, fmt, ap);
    va_end(ap);
  }

 private:
  vef_statement_event_result_t *r_;
};

// StatementEventCapability<Phase, Fn> declares a single handler that fires at
// Phase. Each unique (Phase, Fn) instantiation gets its own static
// capability_config.
//
// Usage:
//   static void slow_query_hook(const
//   vsql::preview_statement_event::StatementEventArgs&,
//                               vsql::preview_statement_event::StatementEventResult&);
//
//   static vsql::preview_statement_event::StatementEventCapability<
//       VEF_STATEMENT_EVENT_POSTEXECUTE, &slow_query_hook> g_cap;
//
//   VEF_GENERATE_ENTRY_POINTS(make_extension().with(g_cap))
template <vef_statement_event_phase_t Phase,
          void (*Fn)(const StatementEventArgs &, StatementEventResult &)>
class StatementEventCapability : public ::vsql::detail::CapabilityBase<
                                     StatementEventCapability<Phase, Fn>> {
 public:
  StatementEventCapability() noexcept;

  // One static capability_config per (Phase, Fn) instantiation. Populated by
  // the constructor; CapabilityTraits::capability_config() returns its address
  // so the wire format carries a pointer to it.
  static inline vef_statement_event_cc_t cc{};

 private:
  template <typename Capability>
  friend struct ::vsql::detail::CapabilityTraits;

  const vef_preview_statement_event_t *abi_ = nullptr;
};

}  // namespace vsql::preview_statement_event

#include <villagesql/preview/detail/statement_event_register.h>
#include <villagesql/preview/statement_event_impl.h>

#endif  // VILLAGESQL_PREVIEW_STATEMENT_EVENT_H
