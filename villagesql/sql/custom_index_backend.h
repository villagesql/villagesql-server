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

#ifndef VILLAGESQL_SQL_CUSTOM_INDEX_BACKEND_H
#define VILLAGESQL_SQL_CUSTOM_INDEX_BACKEND_H

#include <cstdint>

#include "villagesql/sdk/include/villagesql/abi/preview/index.h"

class THD;
struct TABLE;

namespace villagesql {

struct LoadedIndex;

// Server-internal abstraction for the storage strategy that backs a custom
// index. custom_index_runtime owns the common pieces (victionary lookup,
// key extraction, dirty flagging, drop scheduling, KNN plumbing); each
// backend owns the strategy-specific lifecycle and per-call hand-off.
//
// This interface is deliberately ignorant of specific backends. Concrete
// backends live next to their dependencies (e.g. the hidden-table backend
// lives in villagesql/services/preview/ alongside the table_storage
// capability) and register themselves with the runtime at startup via
// register_custom_index_backend(). The runtime selects a backend for a
// loaded index by walking the registry and calling matches() on each.
class CustomIndexBackend {
 public:
  CustomIndexBackend() = default;
  virtual ~CustomIndexBackend() = default;
  CustomIndexBackend(const CustomIndexBackend &) = delete;
  CustomIndexBackend &operator=(const CustomIndexBackend &) = delete;

  // True if this backend handles indexes declared with `intf`. The runtime
  // uses the first matching backend; backends must arrange their matchers
  // so at most one matches any given intf.
  virtual bool matches(const vef_type_index_intf_t &intf) const = 0;

  // Pre-DDL hook: materialize the persistent storage that backs this
  // index. Called by the runtime at the top of a CREATE TABLE / CREATE
  // INDEX / ALTER ADD INDEX statement, BEFORE the outer DDL machinery
  // has started. Runs as if it were a separate top-level operation
  // (acquires its own MDLs, runs its own DD update, commits as part of
  // the outer statement's transaction).
  // If this returns failure, the outer DDL is aborted.
  // Default: no-op for backends that don't need persistent storage.
  virtual bool pre_create_storage(THD *thd, const vef_type_index_intf_t &intf,
                                  char *error_msg, uint32_t error_msg_len) {
    (void)thd;
    (void)intf;
    (void)error_msg;
    (void)error_msg_len;
    return false;
  }

  // Cleanup-on-failure: drop the storage that pre_create_storage
  // created, in the case that the outer DDL later fails. Best-effort —
  // failures are logged but not propagated, since the outer error is
  // already going to surface to the user.
  // Default: no-op.
  virtual void cleanup_failed_create(THD *thd,
                                     const vef_type_index_intf_t &intf) {
    (void)thd;
    (void)intf;
  }

  // Called once when a LoadedIndex is constructed. Runs under the
  // runtime's lock; must not block. The backend may allocate per-index
  // state and stash it internally (keyed off `loaded`).
  virtual bool on_load(LoadedIndex *loaded, char *error_msg,
                       uint32_t error_msg_len) = 0;

  // Called when a LoadedIndex is being destroyed. Runs under the
  // runtime's lock; releases per-index state.
  virtual void on_drop(LoadedIndex *loaded) = 0;

  // Open per-statement resources required for write maintenance of this
  // index. Called from custom_index_prepare_table_writes, outside the
  // runtime's lock; may block on MDL or other I/O.
  virtual bool prepare_table_writes(THD *thd, TABLE *table, LoadedIndex *loaded,
                                    char *error_msg,
                                    uint32_t error_msg_len) = 0;

  // Tear down per-statement resources for this index. Called when the
  // base table is unlocked or the statement ends.
  virtual void finish_table_writes(THD *thd, TABLE *table,
                                   LoadedIndex *loaded) = 0;

  // Populate ABI ctx fields owned by this backend before each DML
  // callback; clear them after. Runs under the runtime's lock.
  virtual void before_callback(THD *thd, TABLE *table, LoadedIndex *loaded) = 0;
  virtual void after_callback(THD *thd, TABLE *table, LoadedIndex *loaded) = 0;

  // Safety-net teardown of any per-thd state that survived past statement
  // boundaries. The runtime calls this from commit/rollback and from
  // commit_stmt/rollback_stmt after an error path may have skipped
  // finish_table_writes. Idempotent: must do nothing when there is no
  // per-thd state left to release.
  virtual void on_statement_end(THD *thd) = 0;
};

// Register a backend with the runtime. Pointer must remain valid for the
// process lifetime (typically a function-local static singleton). Order of
// registration determines lookup order; no duplicate-detection beyond
// matchers being mutually exclusive.
void register_custom_index_backend(CustomIndexBackend *backend);

// Look up a backend whose matches() returns true for `intf`. Returns
// nullptr if no backend matches; the runtime treats that as a no-op
// backend (no per-statement open/lock, no ctx population).
CustomIndexBackend *find_custom_index_backend(
    const vef_type_index_intf_t &intf);

// Invoke `fn` on every registered backend, in registration order. Used by
// the runtime for cross-backend lifecycle calls (e.g. on_statement_end).
// The callback runs while the registry mutex is held; it must not call
// register_custom_index_backend or find_custom_index_backend reentrantly.
void for_each_custom_index_backend(void (*fn)(CustomIndexBackend *, void *),
                                   void *user);

}  // namespace villagesql

#endif  // VILLAGESQL_SQL_CUSTOM_INDEX_BACKEND_H
