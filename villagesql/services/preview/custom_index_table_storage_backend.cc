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

#include "villagesql/services/preview/custom_index_table_storage_backend.h"

#include <cstdio>
#include <map>
#include <mutex>
#include <string>
#include <string_view>

#include "sql/create_field.h"
#include "sql/dd/types/column.h"
#include "sql/handler.h"
#include "sql/item.h"
#include "sql/key_spec.h"
#include "sql/sql_alter.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_list.h"
#include "sql/sql_table.h"
#include "sql/table.h"
#include "villagesql/include/error.h"
#include "villagesql/sdk/include/villagesql/abi/preview/index.h"
#include "villagesql/sdk/include/villagesql/abi/preview/table_storage.h"
#include "villagesql/services/preview/table_storage.h"
#include "villagesql/sql/custom_index_backend.h"
#include "villagesql/sql/custom_index_runtime.h"

namespace villagesql::services {

namespace {

constexpr size_t kErrorMessageLength = 512;

struct PerIndexState {
  vef_table_storage_t *table = nullptr;
  const vef_preview_table_storage_t *abi = nullptr;
};

struct OpenHandle {
  vef_table_storage_handle_t *handle = nullptr;
  const vef_preview_table_storage_t *abi = nullptr;
};

class TableStorageCustomIndexBackend : public villagesql::CustomIndexBackend {
 public:
  bool matches(const vef_type_index_intf_t &intf) const override {
    return intf.table_storage_def != nullptr;
  }

  bool pre_create_storage(THD *thd, const vef_type_index_intf_t &intf,
                          char *error_msg, uint32_t error_msg_len) override {
    vef_table_storage_def_t def{};
    vef_index_ctx_t ctx{};
    if (intf.table_storage_def(&ctx, &def, error_msg, error_msg_len)) {
      return true;
    }
    // Materialize the physical hidden table now. Safe here because
    // process_create / process_alter haven't taken the runtime lock; if
    // we called abi->create from inside on_load instead, we'd self-
    // deadlock on g_runtime_mu during mysql_create_table's commit hooks.
    return materialize_physical_table_storage(thd, &def, error_msg,
                                              error_msg_len);
  }

  void cleanup_failed_create(THD *thd,
                             const vef_type_index_intf_t &intf) override {
    vef_table_storage_def_t def{};
    vef_index_ctx_t ctx{};
    char err[kErrorMessageLength]{};
    if (intf.table_storage_def(&ctx, &def, err, sizeof(err))) {
      LogVSQL(ERROR_LEVEL, "cleanup_failed_create: table_storage_def: %s", err);
      return;
    }
    drop_via_mysql_rm_table(thd, &def);
  }

  bool on_load(villagesql::LoadedIndex *loaded, char *error_msg,
               uint32_t error_msg_len) override {
    vef_table_storage_def_t def{};
    if (loaded->intf.table_storage_def(&loaded->ctx, &def, error_msg,
                                       error_msg_len)) {
      return true;
    }

    PerIndexState state;
    state.abi = preview_table_storage_vtable();
    if (state.abi == nullptr) {
      snprintf(error_msg, error_msg_len,
               "table_storage capability is unavailable");
      return true;
    }
    if (state.abi->create(&def, &state.table, error_msg, error_msg_len)) {
      return true;
    }

    std::lock_guard<std::mutex> guard(state_mu_);
    per_index_state_.emplace(loaded, state);
    return false;
  }

  void on_drop(villagesql::LoadedIndex *loaded) override {
    PerIndexState state;
    {
      std::lock_guard<std::mutex> guard(state_mu_);
      auto it = per_index_state_.find(loaded);
      if (it == per_index_state_.end()) return;
      state = it->second;
      per_index_state_.erase(it);
    }
    if (state.abi != nullptr && state.table != nullptr) {
      char err[kErrorMessageLength]{};
      if (state.abi->drop(state.table, err, sizeof(err))) {
        LogVSQL(ERROR_LEVEL, "Custom index '%s' hidden-table drop failed: %s",
                loaded->name.c_str(), err);
      }
    }
  }

  // Open & write-lock the backing hidden table for this index for the
  // duration of the current statement. Called from the server's write-lock
  // path (handler::ha_external_lock), outside the runtime's lock, so MDL
  // acquisition may block safely. Idempotent: if a handle is already open
  // for this (thd, loaded), do nothing.
  bool prepare_table_writes(THD *thd, TABLE * /*table*/,
                            villagesql::LoadedIndex *loaded, char *error_msg,
                            uint32_t error_msg_len) override {
    PerIndexState state;
    {
      std::lock_guard<std::mutex> guard(state_mu_);
      auto it = per_index_state_.find(loaded);
      if (it == per_index_state_.end()) {
        snprintf(error_msg, error_msg_len,
                 "hidden-table backend has no state for index '%s'",
                 loaded->name.c_str());
        return true;
      }
      state = it->second;
    }

    // Check (and reserve) the slot under the handle lock first so we never
    // do an open() with the slot already populated; release the lock for
    // the open itself.
    {
      std::lock_guard<std::mutex> guard(handle_mu_);
      auto &thd_handles = thd_handles_[thd];
      if (thd_handles.count(loaded->name) != 0) return false;
    }

    vef_table_storage_handle_t *handle = nullptr;
    if (state.abi->open(state.table, VEF_TABLE_STORAGE_LOCK_WRITE, &handle,
                        error_msg, error_msg_len)) {
      return true;
    }

    std::lock_guard<std::mutex> guard(handle_mu_);
    auto &thd_handles = thd_handles_[thd];
    // Re-check the slot in case a concurrent prepare on the same thd
    // raced us. Each thd has at most one statement in flight, so this is
    // defensive only; close the redundant handle if the race actually
    // happened.
    auto inserted =
        thd_handles.emplace(loaded->name, OpenHandle{handle, state.abi});
    if (!inserted.second) {
      state.abi->close(handle);
    }
    return false;
  }

  void finish_table_writes(THD *thd, TABLE * /*table*/,
                           villagesql::LoadedIndex *loaded) override {
    OpenHandle to_close;
    {
      std::lock_guard<std::mutex> guard(handle_mu_);
      auto thd_it = thd_handles_.find(thd);
      if (thd_it == thd_handles_.end()) return;
      auto handle_it = thd_it->second.find(loaded->name);
      if (handle_it == thd_it->second.end()) return;
      to_close = handle_it->second;
      thd_it->second.erase(handle_it);
      if (thd_it->second.empty()) thd_handles_.erase(thd_it);
    }
    to_close.abi->close(to_close.handle);
  }

  void before_callback(THD *thd, TABLE * /*table*/,
                       villagesql::LoadedIndex *loaded) override {
    std::lock_guard<std::mutex> guard(handle_mu_);
    auto thd_it = thd_handles_.find(thd);
    if (thd_it == thd_handles_.end()) {
      loaded->ctx.table_storage_handle = nullptr;
      return;
    }
    auto handle_it = thd_it->second.find(loaded->name);
    loaded->ctx.table_storage_handle =
        handle_it == thd_it->second.end() ? nullptr : handle_it->second.handle;
  }

  void after_callback(THD * /*thd*/, TABLE * /*table*/,
                      villagesql::LoadedIndex *loaded) override {
    loaded->ctx.table_storage_handle = nullptr;
  }

  void on_statement_end(THD *thd) override {
    // Safety net: finish_table_writes should have run for every prepared
    // table at unlock time. Any remaining handles indicate a path that
    // skipped F_UNLCK (e.g. an early error after F_WRLCK). Close them all.
    std::map<std::string, OpenHandle> to_close;
    {
      std::lock_guard<std::mutex> guard(handle_mu_);
      auto it = thd_handles_.find(thd);
      if (it == thd_handles_.end()) return;
      to_close.swap(it->second);
      thd_handles_.erase(it);
    }
    for (auto &kv : to_close) kv.second.abi->close(kv.second.handle);
  }

 private:
  // Build an HA_CREATE_INFO + Alter_info for the def and call
  // mysql_create_table as a separate top-level operation. The caller
  // (custom_index_pre_create_storage) runs us BEFORE the outer DDL has
  // taken any MDL or set up its own DD update context, so this looks
  // like a normal top-level CREATE TABLE to MySQL.
  bool create_via_mysql_create_table(THD *thd,
                                     const vef_table_storage_def_t *def,
                                     char *error_msg, uint32_t error_msg_len) {
    const std::string physical_name =
        physical_table_storage_name(std::string_view(def->logical_name));

    Alter_info alter_info(thd->mem_root);
    alter_info.flags =
        Alter_info::ALTER_ADD_COLUMN | Alter_info::ALTER_ADD_INDEX;

    char length_bufs[16][16]{};
    for (uint32_t i = 0; i < def->column_count; i++) {
      if (i >= sizeof(length_bufs) / sizeof(length_bufs[0])) {
        snprintf(error_msg, error_msg_len, "too many hidden table columns");
        return true;
      }
      enum_field_types type{};
      const char *length_str = nullptr;
      uint type_modifier = NOT_NULL_FLAG;
      switch (def->columns[i].type) {
        case VEF_TABLE_STORAGE_COL_BYTES:
          type = MYSQL_TYPE_VARCHAR;
          snprintf(length_bufs[i], sizeof(length_bufs[i]), "%u",
                   def->columns[i].max_length > 255U ? 4096U : 255U);
          length_str = length_bufs[i];
          break;
        case VEF_TABLE_STORAGE_COL_UINT64:
          type = MYSQL_TYPE_LONGLONG;
          type_modifier |= UNSIGNED_FLAG;
          break;
        case VEF_TABLE_STORAGE_COL_INT64:
          type = MYSQL_TYPE_LONGLONG;
          break;
      }

      auto *cr_field = new (thd->mem_root) Create_field();
      if (cr_field == nullptr) {
        snprintf(error_msg, error_msg_len, "out of memory");
        return true;
      }
      if (cr_field->init(thd, def->columns[i].name, type, length_str,
                         /*decimals=*/nullptr, type_modifier,
                         /*default=*/nullptr,
                         /*on_update=*/nullptr, &EMPTY_CSTR, /*change=*/nullptr,
                         /*interval_list=*/nullptr,
                         def->columns[i].type == VEF_TABLE_STORAGE_COL_BYTES
                             ? &my_charset_bin
                             : nullptr,
                         /*has_explicit_collation=*/false, /*geom_type=*/0,
                         /*gcol_info=*/nullptr, /*default_val_expr=*/nullptr,
                         /*srid=*/{}, dd::Column::enum_hidden_type::HT_VISIBLE,
                         /*is_array=*/false)) {
        snprintf(error_msg, error_msg_len, "failed to init column");
        return true;
      }
      if (alter_info.create_list.push_back(cr_field)) {
        snprintf(error_msg, error_msg_len, "failed to add column");
        return true;
      }
    }

    List<Key_part_spec> pk_parts;
    for (uint32_t i = 0; i < def->primary_key_column_count; i++) {
      const char *col_name = def->columns[def->primary_key_columns[i]].name;
      auto *part = new (thd->mem_root) Key_part_spec(
          {col_name, strlen(col_name)}, /*prefix_length=*/0, ORDER_ASC);
      if (part == nullptr || pk_parts.push_back(part)) {
        snprintf(error_msg, error_msg_len, "failed to add PK part");
        return true;
      }
    }
    auto *pk = new (thd->mem_root) Key_spec(
        thd->mem_root, KEYTYPE_PRIMARY, NULL_CSTR, &default_key_create_info,
        /*generated=*/false, /*check_for_duplicate_indexes=*/true, pk_parts);
    if (pk == nullptr || alter_info.key_list.push_back(pk)) {
      snprintf(error_msg, error_msg_len, "failed to add PK");
      return true;
    }

    HA_CREATE_INFO create_info;
    create_info.db_type = ha_resolve_by_legacy_type(thd, DB_TYPE_INNODB);
    if (create_info.db_type == nullptr) {
      snprintf(error_msg, error_msg_len, "InnoDB handlerton unavailable");
      return true;
    }
    create_info.row_type = ROW_TYPE_DEFAULT;
    create_info.default_table_charset = default_charset_info;
    create_info.table_charset = default_charset_info;
    create_info.alias = physical_name.c_str();
    create_info.options |= HA_LEX_CREATE_IF_NOT_EXISTS;

    Table_ref tref(kTableStorageSchema, std::strlen(kTableStorageSchema),
                   physical_name.c_str(), physical_name.length(),
                   physical_name.c_str(), TL_WRITE);
    tref.mdl_request.set_type(MDL_EXCLUSIVE);
    tref.open_strategy = Table_ref::OPEN_FOR_CREATE;

    // Swap thd->lex->query_tables to point at our single Table_ref;
    // mysql_create_table reads it for MDL/open. Restore afterward.
    Query_tables_list backup;
    thd->lex->reset_n_backup_query_tables_list(&backup);
    tref.next_global = nullptr;
    tref.next_local = nullptr;
    thd->lex->query_tables = &tref;
    thd->lex->query_tables_last = &tref.next_global;

    Diagnostics_area private_da(false);
    thd->push_diagnostics_area(&private_da);
    const bool result =
        mysql_create_table(thd, &tref, &create_info, &alter_info);
    // Capture the inner error before popping the DA.
    const char *inner_err =
        (result && private_da.is_error()) ? private_da.message_text() : "";
    const uint inner_errno =
        (result && private_da.is_error()) ? private_da.mysql_errno() : 0;
    thd->pop_diagnostics_area();

    thd->lex->restore_backup_query_tables_list(&backup);

    if (result) {
      snprintf(error_msg, error_msg_len,
               "mysql_create_table failed for hidden table '%s.%s' "
               "(errno=%u, msg=%s)",
               kTableStorageSchema, physical_name.c_str(), inner_errno,
               inner_err);
      return true;
    }
    return false;
  }

  void drop_via_mysql_rm_table(THD *thd, const vef_table_storage_def_t *def) {
    const std::string physical_name =
        physical_table_storage_name(std::string_view(def->logical_name));
    Table_ref tref(kTableStorageSchema, std::strlen(kTableStorageSchema),
                   physical_name.c_str(), physical_name.length(),
                   physical_name.c_str(), TL_WRITE);
    tref.mdl_request.set_type(MDL_EXCLUSIVE);

    Query_tables_list backup;
    thd->lex->reset_n_backup_query_tables_list(&backup);
    tref.next_global = nullptr;
    tref.next_local = nullptr;
    thd->lex->query_tables = &tref;
    thd->lex->query_tables_last = &tref.next_global;

    // mysql_rm_table calls my_ok on success. We're inside a top-level DDL
    // whose Diagnostics_area is already set (the user's DROP TABLE
    // already emitted its OK). Push a private DA so the nested
    // mysql_rm_table can set its own status without asserting.
    Diagnostics_area private_da(false);
    thd->push_diagnostics_area(&private_da);
    (void)mysql_rm_table(thd, &tref, /*if_exists=*/true,
                         /*drop_temporary=*/false);
    thd->pop_diagnostics_area();

    thd->lex->restore_backup_query_tables_list(&backup);
  }

  std::mutex state_mu_;
  std::map<villagesql::LoadedIndex *, PerIndexState> per_index_state_;

  std::mutex handle_mu_;
  std::map<THD *, std::map<std::string, OpenHandle>> thd_handles_;
};

}  // namespace

void register_table_storage_custom_index_backend() {
  static TableStorageCustomIndexBackend instance;
  villagesql::register_custom_index_backend(&instance);
}

}  // namespace villagesql::services
