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

#ifndef VILLAGESQL_SQL_CUSTOM_INDEX_RUNTIME_H_
#define VILLAGESQL_SQL_CUSTOM_INDEX_RUNTIME_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "villagesql/sdk/include/villagesql/abi/preview/index.h"

class Alter_info;
class Table_ref;
class THD;
struct TABLE;

namespace villagesql {

class CustomIndexBackend;
struct CustomIndexKnnScan;
struct RuntimeArena;

// A custom index instance loaded into the server. Owned by the runtime;
// passed by pointer to backends and to extension callbacks. Backends may
// stash per-index state via their own internal maps keyed off this
// pointer; they must release it in CustomIndexBackend::on_drop.
struct LoadedIndex {
  std::string name;
  vef_index_ctx_t ctx{};
  // The index type's callback vtable, stored by value. The intf is a
  // small POD of function pointers — the C ABI side hands it to us via
  // a pointer (IndexTypeDescriptor::intf()), but inside the runtime we
  // treat it as a value: copied at load time, accessed with dot syntax
  // (`loaded->intf.insert(...)`), passed by const reference across
  // internal interfaces.
  vef_type_index_intf_t intf{};
  std::unique_ptr<RuntimeArena> arena;
  vef_storage_ctx_t *storage{nullptr};
  CustomIndexBackend *backend{nullptr};
  bool dirty{false};
};

// Pre-DDL hook: for each custom index declared in `alter_info`, materialize
// its backing storage by calling the appropriate backend's
// pre_create_storage. Called at the top of CREATE TABLE / CREATE INDEX /
// ALTER ADD INDEX before the outer DDL machinery starts. Returns true on
// failure; the caller should abort the outer DDL.
//
// On caller-side failure (outer DDL fails after pre_create succeeded), the
// caller must call custom_index_rollback_pre_create to drop the storage
// we just created.
bool custom_index_pre_create_storage(THD *thd, const Alter_info *alter_info);

// True if `alter_info` adds a custom index (USING EXTENDED(...)). DDL
// callers use this to force the copy/rebuild ALTER algorithm so the
// engine's row-by-row copy fires our per-row maintenance hooks for the
// new index. Without this override the inplace path would skip
// the row scan.
//
// TODO(villagesql-indexing): adopt a dual row-identity model. Each
// custom index has a mode chosen at CREATE INDEX:
//
//   PK mode  — identity = base table's PK columns (what this code does
//              today). Identity is extracted directly from the record,
//              no position() call needed. Requires the base table to
//              have a user PK. Force-copy ONLY when adding a custom
//              index; other ALTERs stay inplace.
//
//   REF mode — identity = handler::ref (engine row reference, opaque
//              VARBINARY(ref_length)). Captured via position() after
//              the write. Works on tables without a user PK (InnoDB
//              synthesizes DB_ROW_ID and uses it as the clustered-index
//              key). Refs may shift on any inplace ALTER that rebuilds
//              the clustered index, so REF mode requires MariaDB's
//              broader force-copy policy (table->s->hlindexes() in
//              their sql_table.cc):
//                - adding a custom index,
//                - renaming a custom index,
//                - adding/dropping ANY index on a table that already
//                  has a custom index.
//
// Lookup cost is the same in both modes: InnoDB's clustered index IS
// the PK (or DB_ROW_ID), so both ha_index_read_map(pk_cols) and
// ha_rnd_pos(ref) ultimately do the same B+tree walk. The real
// trade-off is force-copy policy, not lookup speed: PK mode lets
// normal ALTERs run inplace; REF mode forces copy more often.
//
// Default at CREATE INDEX: PK mode if the base table has a user PK,
// REF mode otherwise. Allow an explicit override (e.g. USING
// EXTENDED(...) WITH ROW_REF) for callers who want REF mode on PK
// tables (e.g. to keep the hidden-table identity column small when
// the PK is wide).
//
// Hidden-table schema follows the mode: PK mode uses the base PK
// columns as the hidden table's PK; REF mode uses a single
// __row_ref VARBINARY(ref_length) NOT NULL column. KNN scan iterator
// dispatches on the mode (ha_index_read_map vs ha_rnd_pos).
//
// Mode is per-index, stored in victionary. If any custom index on a
// given table is in REF mode, the broader force-copy policy applies
// to that table for simplicity.
//
// Contract: the server picks the mode; the extension is mode-agnostic.
// Index algorithms (bloom, vector, etc.) treat the identity bytes as
// opaque bookkeeping — they never introspect them. So intf->insert /
// mark_delete keep their existing `pkey_columns` opaque-bytes
// signature; the server passes either packed PK columns or a single
// ref blob and the extension stores/returns whatever it got. If we
// ever surface stability semantics to extensions (e.g. "identity is
// stable across ALTER rebuilds"), it'd be as a server-supplied hint
// flag, not an extension-chosen mode.
bool alter_info_adds_custom_index(const Alter_info *alter_info);

// Symmetric cleanup: drop any storage that custom_index_pre_create_storage
// created. Called when the outer DDL fails. Best-effort; failures are
// logged.
void custom_index_rollback_pre_create(THD *thd, const Alter_info *alter_info);

// DROP TABLE hook: for each table in `tables` (a chain through
// next_local), look up its custom indexes in victionary and drop their
// backing storage. Must be called AFTER MySQL's DROP TABLE has
// successfully removed the base tables (since this also removes the
// victionary entries that we no longer need to consult). Best-effort;
// failures are logged but don't propagate.
//
// To support this ordering, the caller should snapshot the set of
// custom-index intfs BEFORE the drop via
// custom_index_snapshot_for_drop, then pass the snapshot to
// custom_index_drop_snapshotted_storage AFTER the base drop succeeds.
struct CustomIndexDropSnapshot {
  std::vector<vef_type_index_intf_t> intfs;
};
std::unique_ptr<CustomIndexDropSnapshot> custom_index_snapshot_for_drop(
    THD *thd, Table_ref *tables);
void custom_index_drop_snapshotted_storage(
    THD *thd, const CustomIndexDropSnapshot *snapshot);

// Open and write-lock the per-statement resources needed to maintain this
// base table's custom indexes. Called by handler::ha_external_lock when
// the engine acquires a write lock on the table, before the first row is
// written. Dispatches to each backend's prepare_table_writes.
//
// Returns 0 on success or a non-zero HA_ERR_* code on failure; in the
// failure case any partial state has been rolled back so the caller can
// abort the lock.
int custom_index_prepare_table_writes(THD *thd, TABLE *table);

// Tear down the per-statement resources opened by
// custom_index_prepare_table_writes. Called by handler::ha_external_lock
// on the unlock path before the engine's external_lock(F_UNLCK).
void custom_index_finish_table_writes(THD *thd, TABLE *table);

int custom_index_after_write_row(THD *thd, TABLE *table,
                                 const unsigned char *record);
int custom_index_after_update_row(THD *thd, TABLE *table,
                                  const unsigned char *old_record,
                                  const unsigned char *new_record);
int custom_index_after_delete_row(THD *thd, TABLE *table,
                                  const unsigned char *record);

void custom_index_commit_stmt(THD *thd);
void custom_index_commit(THD *thd);
void custom_index_rollback_stmt(THD *thd);
void custom_index_rollback(THD *thd);

void custom_index_schedule_drop(THD *thd, const char *db, const char *table,
                                const char *index_name);

bool custom_index_knn_scan_begin(TABLE *table, const char *index_name,
                                 const unsigned char *query_key,
                                 uint32_t query_key_len, uint32_t limit,
                                 CustomIndexKnnScan **scan, char *error_msg,
                                 uint32_t error_msg_len);
bool custom_index_knn_scan_next(CustomIndexKnnScan *scan,
                                const unsigned char **pkey_data,
                                uint32_t *pkey_len, bool *eof, char *error_msg,
                                uint32_t error_msg_len);
void custom_index_knn_scan_end(CustomIndexKnnScan **scan);

}  // namespace villagesql

#endif  // VILLAGESQL_SQL_CUSTOM_INDEX_RUNTIME_H_
