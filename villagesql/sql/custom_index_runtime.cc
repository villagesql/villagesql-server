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

#include "villagesql/sql/custom_index_runtime.h"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "my_base.h"
#include "mysql/strings/m_ctype.h"
#include "sql/field.h"
#include "sql/handler.h"
#include "sql/key.h"
#include "sql/key_spec.h"
#include "sql/sql_alter.h"
#include "sql/sql_class.h"
#include "sql/table.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/descriptor/index_type_descriptor.h"
#include "villagesql/schema/schema_manager.h"
#include "villagesql/schema/systable/custom_index_columns.h"
#include "villagesql/schema/systable/custom_indexes.h"
#include "villagesql/schema/victionary_client.h"
#include "villagesql/sql/custom_index_backend.h"

namespace villagesql {

constexpr uint32_t kIndexContextVersion = 1;
constexpr size_t kErrorMessageLength = 512;

// RuntimeArena is forward-declared in the header so that LoadedIndex can
// hold a unique_ptr<RuntimeArena> without exposing the layout. The full
// definition lives here.
struct RuntimeArena {
  std::vector<std::unique_ptr<unsigned char[]>> blocks;
};

struct CustomIndexKnnScan {
  LoadedIndex *loaded{nullptr};
  vef_index_cursor_ref_t cursor{0};
  bool eof{true};
  std::vector<vef_storage_col_data_t> key_columns;
  std::vector<vef_storage_col_data_t> pkey_columns;
  std::vector<unsigned char> pkey_buffer;
};

namespace {

std::mutex g_runtime_mu;
std::map<std::string, std::unique_ptr<LoadedIndex>> g_loaded_indexes;
std::map<THD *, std::set<std::string>> g_thd_statement_touched;
std::map<THD *, std::set<std::string>> g_thd_transaction_touched;
std::map<THD *, std::set<std::string>> g_thd_statement_drops;
std::map<THD *, std::set<std::string>> g_thd_transaction_drops;

// True if `table` is an ALTER TABLE rebuild shadow (#sql-xxx).
// MEMORY.md notes that types/util.cc:is_alter_rebuild_table requires
// INTERNAL_TMP_TABLE, but that's evaluated at open_table_from_share
// time. By the time ha_write_row fires during the copy, the engine
// has already promoted the share to TRANSACTIONAL_TMP_TABLE — so we
// accept either tmp_table category alongside the #sql name prefix.
bool is_alter_rebuild_share(TABLE *table) {
  if (table == nullptr || table->s == nullptr) return false;
  if (table->s->tmp_table != INTERNAL_TMP_TABLE &&
      table->s->tmp_table != TRANSACTIONAL_TMP_TABLE) {
    return false;
  }
  if (table->s->table_name.str == nullptr) return false;
  return strncmp(table->s->table_name.str, tmp_file_prefix,
                 tmp_file_prefix_length) == 0;
}

// True if `table` should be maintained as the ALTER rebuild for the
// user-visible table stashed on THD. Caller passes thd; nullptr is
// treated as "no target set."
bool is_runtime_alter_rebuild(THD *thd, TABLE *table) {
  if (thd == nullptr) return false;
  if (thd->villagesql_alter_target_db.empty() ||
      thd->villagesql_alter_target_table.empty()) {
    return false;
  }
  return is_alter_rebuild_share(table);
}

bool is_runtime_internal_table(TABLE *table) {
  if (table == nullptr || table->s == nullptr ||
      table->s->tmp_table != NO_TMP_TABLE) {
    return true;
  }
  if (my_strcasecmp(system_charset_info, table->s->db.str,
                    SchemaManager::VILLAGESQL_SCHEMA_NAME) == 0) {
    return true;
  }
  return false;
}

void *runtime_arena_alloc(vef_storage_arena_t *arena_ctx, uint32_t size) {
  auto *arena = reinterpret_cast<RuntimeArena *>(arena_ctx);
  auto block = std::make_unique<unsigned char[]>(size);
  void *result = block.get();
  arena->blocks.push_back(std::move(block));
  return result;
}

void dummy_profile_fn(vef_index_ref_t, uint32_t, const void *const *, uint32_t,
                      void *) {}

uint32_t dummy_key_len_fn(vef_index_ref_t, uint32_t, bool) { return 0; }

std::string table_index_name(const char *db, const char *table,
                             const char *index_name) {
  return std::string(db) + "." + table + "." + index_name;
}

Field *find_field(TABLE *table, const std::string &field_name) {
  for (Field **field = table->field; *field != nullptr; ++field) {
    if (my_strcasecmp(system_charset_info, (*field)->field_name,
                      field_name.c_str()) == 0) {
      return *field;
    }
  }
  return nullptr;
}

vef_storage_col_data_t field_data_for_record(Field *field, const uchar *record,
                                             std::string *owned_buffer) {
  if (field == nullptr) return {nullptr, 0};
  if (field->is_null_in_record(record)) return {nullptr, 0};

  const ptrdiff_t diff = record - field->table->record[0];
  field->move_field_offset(diff);

  if (field->has_type_context()) {
    String buffer;
    String *value = field->val_str(&buffer);
    if (value == nullptr) {
      field->move_field_offset(-diff);
      return {nullptr, 0};
    }
    owned_buffer->assign(value->ptr(), value->length());
    field->move_field_offset(-diff);
    return {pointer_cast<const uchar *>(owned_buffer->data()),
            static_cast<uint32_t>(owned_buffer->size())};
  }

  const uchar *data = field->data_ptr();
  const uint32_t length = field->pack_length();
  field->move_field_offset(-diff);
  return {data, length};
}

// TODO(villagesql-indexing): final implementation must expose the engine
// row reference (handler::ref, populated by handler::position()) to
// index extensions instead of requiring a user-declared primary key.
// MariaDB hlindex stores (ref, key_columns) and uses ha_rnd_pos(ref) to
// fetch base rows; we should do the same so CREATE INDEX works on
// tables without an explicit PK (or with an engine-synthesized one).
// Until then, base tables without a usable PK are rejected here.
bool collect_primary_key_columns(TABLE *table, const uchar *record,
                                 std::vector<vef_storage_col_data_t> *out) {
  if (table->s->primary_key >= MAX_KEY) return true;

  const KEY &primary_key = table->s->key_info[table->s->primary_key];
  out->clear();
  out->reserve(primary_key.user_defined_key_parts);
  for (uint i = 0; i < primary_key.user_defined_key_parts; ++i) {
    const KEY_PART_INFO &key_part = primary_key.key_part[i];
    if (key_part.length == 0) return true;
    out->push_back(
        vef_storage_col_data_t{record + key_part.offset, key_part.length});
  }
  return false;
}

bool collect_key_columns(TABLE *table, const std::vector<std::string> &columns,
                         const uchar *record,
                         std::vector<vef_storage_col_data_t> *out,
                         std::vector<std::string> *owned_buffers) {
  out->clear();
  owned_buffers->clear();
  out->reserve(columns.size());
  owned_buffers->reserve(columns.size());
  for (const std::string &column : columns) {
    Field *field = find_field(table, column);
    if (field == nullptr) return true;
    owned_buffers->emplace_back();
    out->push_back(
        field_data_for_record(field, record, &owned_buffers->back()));
  }
  return false;
}

bool get_custom_index_columns(THD *thd, uint64_t index_id,
                              std::vector<std::string> *out) {
  VictionaryClient &vclient = VictionaryClient::instance();
  std::vector<const IndexColumnEntry *> columns =
      thd != nullptr ? vclient.custom_index_columns().get_prefix(
                           thd, IndexColumnKeyPrefix(index_id))
                     : vclient.GetColumnsForIndex(index_id);
  std::sort(columns.begin(), columns.end(),
            [](const IndexColumnEntry *a, const IndexColumnEntry *b) {
              return a->key_position() < b->key_position();
            });
  out->clear();
  out->reserve(columns.size());
  for (const IndexColumnEntry *column : columns)
    out->push_back(column->column_name);
  return false;
}

LoadedIndex *load_index_locked(const char *db, const char *table_name,
                               const IndexEntry &entry,
                               const IndexTypeDescriptor &descriptor,
                               uint32_t num_key_columns,
                               uint32_t num_primary_key_columns) {
  const std::string runtime_name =
      table_index_name(db, table_name, entry.index_name().c_str());
  auto existing = g_loaded_indexes.find(runtime_name);
  if (existing != g_loaded_indexes.end()) return existing->second.get();

  auto loaded = std::make_unique<LoadedIndex>();
  loaded->name = runtime_name;
  loaded->intf = descriptor.intf();
  loaded->arena = std::make_unique<RuntimeArena>();
  loaded->ctx.version = kIndexContextVersion;
  loaded->ctx.index_ref = const_cast<char *>(loaded->name.c_str());
  loaded->ctx.num_key_columns = num_key_columns;
  loaded->ctx.num_primary_key_columns = num_primary_key_columns;
  loaded->ctx.profile_fn = dummy_profile_fn;
  loaded->ctx.helper_fn = dummy_profile_fn;
  loaded->ctx.key_len_fn = dummy_key_len_fn;

  char error_msg[kErrorMessageLength]{};
  if (loaded->intf.load(
          &loaded->ctx, /*storage_ref=*/0,
          reinterpret_cast<vef_storage_arena_t *>(loaded->arena.get()),
          runtime_arena_alloc, &loaded->storage, error_msg,
          sizeof(error_msg))) {
    LogVSQL(ERROR_LEVEL, "Failed to load custom index '%s': %s",
            loaded->name.c_str(), error_msg);
    return nullptr;
  }

  loaded->backend = find_custom_index_backend(loaded->intf);
  if (loaded->backend != nullptr &&
      loaded->backend->on_load(loaded.get(), error_msg, sizeof(error_msg))) {
    LogVSQL(ERROR_LEVEL, "Custom index '%s' backend on_load failed: %s",
            loaded->name.c_str(), error_msg);
    return nullptr;
  }

  LoadedIndex *result = loaded.get();
  g_loaded_indexes.emplace(runtime_name, std::move(loaded));
  return result;
}

void mark_touched_locked(THD *thd, const std::string &runtime_name) {
  g_thd_statement_touched[thd].insert(runtime_name);
  g_thd_transaction_touched[thd].insert(runtime_name);
}

void mark_dirty_locked(const std::set<std::string> &names) {
  for (const std::string &name : names) {
    auto it = g_loaded_indexes.find(name);
    if (it != g_loaded_indexes.end()) it->second->dirty = true;
  }
}

// Notify every registered backend that the THD's statement is ending and
// any per-statement state should be torn down. Called from commit/rollback
// paths under g_runtime_mu; backends synchronize on their own internal
// mutexes.
void notify_backends_statement_end_locked(THD *thd) {
  for_each_custom_index_backend(
      [](CustomIndexBackend *backend, void *user) {
        backend->on_statement_end(static_cast<THD *>(user));
      },
      thd);
}

void drop_loaded_indexes_locked(const std::set<std::string> &names) {
  for (const std::string &name : names) {
    auto it = g_loaded_indexes.find(name);
    if (it == g_loaded_indexes.end()) continue;

    LoadedIndex *loaded = it->second.get();
    char error_msg[kErrorMessageLength]{};
    if (loaded->intf.drop != nullptr && loaded->storage != nullptr &&
        loaded->intf.drop(&loaded->ctx, loaded->storage, /*trx_ref=*/0,
                          error_msg, sizeof(error_msg))) {
      LogVSQL(ERROR_LEVEL, "Custom index '%s' drop callback failed: %s",
              loaded->name.c_str(), error_msg);
    }
    if (loaded->backend != nullptr) loaded->backend->on_drop(loaded);
    g_loaded_indexes.erase(it);
  }
}

int apply_to_custom_indexes(
    THD *thd, TABLE *table, const uchar *record,
    const std::function<bool(LoadedIndex *, vef_storage_col_data_t *,
                             vef_storage_col_data_t *, char *, uint32_t)>
        &callback) {
  // TODO(villagesql-indexing): Make internal system-table writes bypass
  // extension index maintenance through an explicit server-side context flag.
  // For now, avoid re-entering Victionary/custom-index code while persisting
  // VillageSQL metadata such as INSTALL/UNINSTALL EXTENSION changes.
  const bool is_rebuild = is_runtime_alter_rebuild(thd, table);
  if (is_runtime_internal_table(table) && !is_rebuild) return 0;

  const char *lookup_db =
      is_rebuild ? thd->villagesql_alter_target_db.c_str() : table->s->db.str;
  const char *lookup_table = is_rebuild
                                 ? thd->villagesql_alter_target_table.c_str()
                                 : table->s->table_name.str;
  VictionaryClient &vclient = VictionaryClient::instance();
  if (!vclient.is_initialized()) return 0;

  std::vector<const IndexEntry *> indexes;
  std::map<uint64_t, std::vector<std::string>> index_columns;
  std::map<uint64_t, const IndexTypeDescriptor *> descriptors;
  {
    auto read_lock = vclient.get_read_lock();
    if (is_rebuild) {
      // ALTER TABLE / CREATE INDEX has marked the new index entries on
      // the THD but the DDL has not committed yet — include uncommitted.
      indexes = vclient.custom_indexes().get_prefix(
          thd, IndexKeyPrefix(lookup_db, lookup_table));
    } else {
      indexes = vclient.GetCustomIndexesForTable(lookup_db, lookup_table);
    }
    if (indexes.empty()) return 0;

    for (const IndexEntry *entry : indexes) {
      const auto *descriptor = vclient.index_type_descriptors().get_committed(
          IndexTypeDescriptorKey(entry->index_type_name, entry->extension_name,
                                 entry->extension_version));
      if (descriptor == nullptr) {
        LogVSQL(ERROR_LEVEL,
                "Custom index type '%s.%s' not registered for index '%s'",
                entry->extension_name.c_str(), entry->index_type_name.c_str(),
                entry->index_name().c_str());
        return HA_ERR_INTERNAL_ERROR;
      }
      descriptors.emplace(entry->index_id, descriptor);

      std::vector<std::string> names;
      get_custom_index_columns(is_rebuild ? thd : nullptr, entry->index_id,
                               &names);
      index_columns.emplace(entry->index_id, std::move(names));
    }
  }

  std::vector<vef_storage_col_data_t> pkey_columns;
  if (collect_primary_key_columns(table, record, &pkey_columns)) {
    LogVSQL(ERROR_LEVEL, "Custom indexes require a primary key for '%s.%s'",
            table->s->db.str, table->s->table_name.str);
    return HA_ERR_INTERNAL_ERROR;
  }

  std::vector<vef_storage_col_data_t> key_columns;
  std::vector<std::string> key_column_buffers;
  for (const IndexEntry *entry : indexes) {
    const auto columns_it = index_columns.find(entry->index_id);
    if (columns_it == index_columns.end() ||
        collect_key_columns(table, columns_it->second, record, &key_columns,
                            &key_column_buffers)) {
      LogVSQL(ERROR_LEVEL, "Cannot resolve custom index columns for '%s'",
              entry->index_name().c_str());
      return HA_ERR_INTERNAL_ERROR;
    }

    std::lock_guard<std::mutex> guard(g_runtime_mu);
    LoadedIndex *loaded = load_index_locked(
        lookup_db, lookup_table, *entry, *descriptors[entry->index_id],
        static_cast<uint32_t>(key_columns.size()),
        static_cast<uint32_t>(pkey_columns.size()));
    if (loaded == nullptr) return HA_ERR_INTERNAL_ERROR;

    char error_msg[kErrorMessageLength]{};
    if (loaded->backend != nullptr) {
      loaded->backend->before_callback(thd, table, loaded);
    }
    const bool failed =
        callback(loaded, key_columns.data(), pkey_columns.data(), error_msg,
                 sizeof(error_msg));
    if (loaded->backend != nullptr) {
      loaded->backend->after_callback(thd, table, loaded);
    }
    if (failed) {
      // TODO(villagesql-indexing): the ABI conflates soft (e.g.
      // uniqueness violation) and hard (e.g. corrupted state) failures
      // into a single bool return. We log at WARNING and avoid
      // poisoning the index with dirty=true; revisit once the ABI
      // distinguishes the two so corruption can be surfaced more
      // loudly while expected rejections stay quiet. Tests that
      // exercise the soft-failure path need to mtr.add_suppression
      // this WARNING line.
      LogVSQL(WARNING_LEVEL, "Custom index '%s' maintenance failed: %s",
              loaded->name.c_str(), error_msg);
      return HA_ERR_INTERNAL_ERROR;
    }
    mark_touched_locked(thd, loaded->name);
  }

  return 0;
}

// Resolve the table's custom indexes from victionary into LoadedIndex
// pointers. The returned vector contains entries in registration order;
// each entry is non-null. On error, returns false and leaves `out` empty.
// Must be called under g_runtime_mu.
bool resolve_loaded_indexes_locked(THD *thd, TABLE *table,
                                   std::vector<LoadedIndex *> *out) {
  out->clear();
  // For ALTER rebuild tables, look up victionary entries under the
  // user-visible name that mysql_alter_table stashed on THD. For all
  // other tables, the share's own name is correct.
  const char *lookup_db = nullptr;
  const char *lookup_table = nullptr;
  if (is_runtime_alter_rebuild(thd, table)) {
    lookup_db = thd->villagesql_alter_target_db.c_str();
    lookup_table = thd->villagesql_alter_target_table.c_str();
  } else {
    if (is_runtime_internal_table(table)) return true;
    lookup_db = table->s->db.str;
    lookup_table = table->s->table_name.str;
  }
  if (table->s->primary_key >= MAX_KEY) return true;

  VictionaryClient &vclient = VictionaryClient::instance();
  if (!vclient.is_initialized()) return true;

  std::vector<const IndexEntry *> indexes;
  std::map<uint64_t, const IndexTypeDescriptor *> descriptors;
  std::map<uint64_t, uint32_t> index_column_counts;
  const bool is_rebuild = is_runtime_alter_rebuild(thd, table);
  {
    auto read_lock = vclient.get_read_lock();
    if (is_rebuild) {
      indexes = vclient.custom_indexes().get_prefix(
          thd, IndexKeyPrefix(lookup_db, lookup_table));
    } else {
      indexes = vclient.GetCustomIndexesForTable(lookup_db, lookup_table);
    }
    if (indexes.empty()) return true;

    for (const IndexEntry *entry : indexes) {
      const auto *descriptor = vclient.index_type_descriptors().get_committed(
          IndexTypeDescriptorKey(entry->index_type_name, entry->extension_name,
                                 entry->extension_version));
      if (descriptor == nullptr) {
        LogVSQL(ERROR_LEVEL,
                "Custom index type '%s.%s' not registered for index '%s'",
                entry->extension_name.c_str(), entry->index_type_name.c_str(),
                entry->index_name().c_str());
        out->clear();
        return false;
      }
      descriptors.emplace(entry->index_id, descriptor);
      const size_t col_count =
          is_rebuild
              ? vclient.custom_index_columns()
                    .get_prefix(thd, IndexColumnKeyPrefix(entry->index_id))
                    .size()
              : vclient.GetColumnsForIndex(entry->index_id).size();
      index_column_counts.emplace(entry->index_id,
                                  static_cast<uint32_t>(col_count));
    }
  }

  const uint32_t pk_column_count =
      table->s->key_info[table->s->primary_key].user_defined_key_parts;

  for (const IndexEntry *entry : indexes) {
    LoadedIndex *loaded = load_index_locked(
        lookup_db, lookup_table, *entry, *descriptors[entry->index_id],
        index_column_counts[entry->index_id], pk_column_count);
    if (loaded == nullptr) {
      out->clear();
      return false;
    }
    out->push_back(loaded);
  }
  return true;
}

}  // namespace

namespace {

// Resolve every custom-index key in `alter_info` to its registered intf.
// Returns true on lookup error (and reports it via villagesql_error or
// the THD error state); fills `out` with the intf for each custom key.
// out is parallel to alter_info->key_list (entries for non-custom keys
// are nullptr). On error, out is cleared.
bool resolve_custom_index_intfs(const Alter_info *alter_info,
                                std::vector<vef_type_index_intf_t> *out) {
  out->clear();
  if (alter_info == nullptr) return false;

  VictionaryClient &vclient = VictionaryClient::instance();
  if (!vclient.is_initialized()) return false;

  bool any_custom = false;
  auto read_lock = vclient.get_read_lock();
  for (size_t i = 0; i < alter_info->key_list.size(); ++i) {
    const Key_spec *key = alter_info->key_list[i];
    if (key == nullptr) continue;
    const KEY_CREATE_INFO &kci = key->key_create_info;
    if (kci.custom_index_type.length == 0) continue;

    const std::string type_name(kci.custom_index_type.str,
                                kci.custom_index_type.length);
    const std::string ext_name =
        kci.custom_index_extension.str
            ? std::string(kci.custom_index_extension.str,
                          kci.custom_index_extension.length)
            : "";

    const auto matches = vclient.index_type_descriptors().get_prefix_committed(
        IndexTypeDescriptorKeyPrefix(type_name, ext_name));
    if (matches.size() != 1) {
      out->clear();
      if (ext_name.empty()) {
        villagesql_error("Custom index type '%s' not found", MYF(0),
                         type_name.c_str());
      } else {
        villagesql_error("Custom index type '%s' not found in extension '%s'",
                         MYF(0), type_name.c_str(), ext_name.c_str());
      }
      return true;
    }
    out->push_back(matches[0]->intf());
    any_custom = true;
  }
  if (!any_custom) out->clear();
  return false;
}

}  // namespace

bool alter_info_adds_custom_index(const Alter_info *alter_info) {
  if (alter_info == nullptr) return false;
  if ((alter_info->flags & Alter_info::ALTER_ADD_INDEX) == 0) return false;
  // If the test-only proceed flag is set, the runtime won't materialize
  // or maintain anything for the custom-typed key, so don't force a
  // copy ALTER either — the rebuild's per-row maintenance would just
  // fail to resolve the (fictional) index type and crash.
  bool skip = false;
  DBUG_EXECUTE_IF("villagesql_custom_index_proceed", skip = true;);
  if (skip) return false;
  for (const Key_spec *key : alter_info->key_list) {
    if (key != nullptr && key->key_create_info.custom_index_type.length > 0) {
      return true;
    }
  }
  return false;
}

bool custom_index_pre_create_storage(THD *thd, const Alter_info *alter_info) {
  // Test-only escape hatch: some tests use unregistered index types to
  // poke at downstream metadata behavior. They set this debug flag to
  // skip the runtime's type-resolution / storage-materialization step
  // here, matching the matching skip in Metadata_modifier::add_indexes.
  bool skip = false;
  DBUG_EXECUTE_IF("villagesql_custom_index_proceed", skip = true;);
  if (skip) return false;

  std::vector<vef_type_index_intf_t> intfs;
  if (resolve_custom_index_intfs(alter_info, &intfs)) return true;
  if (intfs.empty()) return false;

  // Walk the custom-index intfs and ask each backend to materialize its
  // storage. Track which ones we successfully created so we can roll
  // them back if a later one fails.
  std::vector<vef_type_index_intf_t> created;
  for (const vef_type_index_intf_t &intf : intfs) {
    CustomIndexBackend *backend = find_custom_index_backend(intf);
    if (backend == nullptr) continue;
    char error_msg[kErrorMessageLength]{};
    if (backend->pre_create_storage(thd, intf, error_msg, sizeof(error_msg))) {
      // Surface an SQL-level error so dispatch_command finds the THD
      // in a consistent error state. Without this, returning true with
      // a clean DA trips an assertion in send_statement_status.
      if (!thd->is_error()) {
        villagesql_error("Failed to create custom index storage: %s", MYF(0),
                         error_msg);
      }
      // Roll back the ones we already created.
      for (const vef_type_index_intf_t &done : created) {
        CustomIndexBackend *b = find_custom_index_backend(done);
        if (b != nullptr) b->cleanup_failed_create(thd, done);
      }
      return true;
    }
    created.push_back(intf);
  }
  return false;
}

void custom_index_rollback_pre_create(THD *thd, const Alter_info *alter_info) {
  std::vector<vef_type_index_intf_t> intfs;
  if (resolve_custom_index_intfs(alter_info, &intfs)) return;
  for (const vef_type_index_intf_t &intf : intfs) {
    CustomIndexBackend *backend = find_custom_index_backend(intf);
    if (backend != nullptr) backend->cleanup_failed_create(thd, intf);
  }
}

std::unique_ptr<CustomIndexDropSnapshot> custom_index_snapshot_for_drop(
    THD * /*thd*/, Table_ref *tables) {
  auto snapshot = std::make_unique<CustomIndexDropSnapshot>();

  VictionaryClient &vclient = VictionaryClient::instance();
  if (!vclient.is_initialized()) return snapshot;

  auto read_lock = vclient.get_read_lock();
  for (Table_ref *t = tables; t != nullptr; t = t->next_local) {
    if (t->db == nullptr || t->table_name == nullptr) continue;
    if (my_strcasecmp(system_charset_info, t->db,
                      SchemaManager::VILLAGESQL_SCHEMA_NAME) == 0) {
      continue;
    }
    for (const IndexEntry *entry :
         vclient.GetCustomIndexesForTable(t->db, t->table_name)) {
      const auto *descriptor = vclient.index_type_descriptors().get_committed(
          IndexTypeDescriptorKey(entry->index_type_name, entry->extension_name,
                                 entry->extension_version));
      if (descriptor != nullptr) {
        snapshot->intfs.push_back(descriptor->intf());
      }
    }
  }
  return snapshot;
}

void custom_index_drop_snapshotted_storage(
    THD *thd, const CustomIndexDropSnapshot *snapshot) {
  if (snapshot == nullptr) return;
  for (const vef_type_index_intf_t &intf : snapshot->intfs) {
    CustomIndexBackend *backend = find_custom_index_backend(intf);
    if (backend != nullptr) backend->cleanup_failed_create(thd, intf);
  }
}

int custom_index_prepare_table_writes(THD *thd, TABLE *table) {
  if (is_runtime_internal_table(table) &&
      !is_runtime_alter_rebuild(thd, table)) {
    return 0;
  }

  // Phase 1 (under runtime lock): resolve which loaded indexes back this
  // table. load_index_locked may construct new LoadedIndex objects but
  // must not block — backends' on_load is also called under this lock.
  std::vector<LoadedIndex *> loaded_indexes;
  {
    std::lock_guard<std::mutex> guard(g_runtime_mu);
    if (!resolve_loaded_indexes_locked(thd, table, &loaded_indexes)) {
      return HA_ERR_INTERNAL_ERROR;
    }
  }
  if (loaded_indexes.empty()) return 0;

  // Phase 2 (no lock): dispatch to each backend's prepare_table_writes.
  // This is where MDL acquisition may block. On any failure, roll back
  // the indexes already prepared by calling finish_table_writes on them.
  std::vector<LoadedIndex *> prepared;
  prepared.reserve(loaded_indexes.size());
  for (LoadedIndex *loaded : loaded_indexes) {
    if (loaded->backend == nullptr) continue;
    char error_msg[kErrorMessageLength]{};
    if (loaded->backend->prepare_table_writes(thd, table, loaded, error_msg,
                                              sizeof(error_msg))) {
      LogVSQL(ERROR_LEVEL,
              "Custom index '%s' backend prepare_table_writes failed: %s",
              loaded->name.c_str(), error_msg);
      for (LoadedIndex *done : prepared) {
        done->backend->finish_table_writes(thd, table, done);
      }
      return HA_ERR_INTERNAL_ERROR;
    }
    prepared.push_back(loaded);
  }
  return 0;
}

void custom_index_finish_table_writes(THD *thd, TABLE *table) {
  if (is_runtime_internal_table(table) &&
      !is_runtime_alter_rebuild(thd, table)) {
    return;
  }

  std::vector<LoadedIndex *> loaded_indexes;
  {
    std::lock_guard<std::mutex> guard(g_runtime_mu);
    // Resolve without invoking load — finish should match prepare exactly,
    // but it's also called from F_UNLCK paths where the indexes are
    // already loaded. Reuse the same helper; if the index isn't loaded
    // anymore (e.g. concurrent DDL drop), the backend's finish is a no-op.
    resolve_loaded_indexes_locked(thd, table, &loaded_indexes);
  }
  for (LoadedIndex *loaded : loaded_indexes) {
    if (loaded->backend == nullptr) continue;
    loaded->backend->finish_table_writes(thd, table, loaded);
  }
}

int custom_index_after_write_row(THD *thd, TABLE *table, const uchar *record) {
  return apply_to_custom_indexes(
      thd, table, record,
      [](LoadedIndex *loaded, vef_storage_col_data_t *key_columns,
         vef_storage_col_data_t *pkey_columns, char *error_msg,
         uint32_t error_msg_len) {
        vef_storage_col_ref_t key_ref = VEF_STORAGE_EMPTY_COLUMN_REF;
        return loaded->intf.insert(&loaded->ctx, loaded->storage,
                                   /*trx_ref=*/0, key_columns, pkey_columns,
                                   &key_ref, error_msg, error_msg_len);
      });
}

int custom_index_after_delete_row(THD *thd, TABLE *table, const uchar *record) {
  return apply_to_custom_indexes(
      thd, table, record,
      [](LoadedIndex *loaded, vef_storage_col_data_t *key_columns,
         vef_storage_col_data_t *pkey_columns, char *error_msg,
         uint32_t error_msg_len) {
        vef_storage_col_ref_t key_ref = VEF_STORAGE_EMPTY_COLUMN_REF;
        return loaded->intf.mark_delete(
            &loaded->ctx, loaded->storage, /*trx_ref=*/0, &key_ref, key_columns,
            pkey_columns, /*delete_mark=*/true, error_msg, error_msg_len);
      });
}

int custom_index_after_update_row(THD *thd, TABLE *table,
                                  const uchar *old_record,
                                  const uchar *new_record) {
  int error = custom_index_after_delete_row(thd, table, old_record);
  if (error != 0) return error;
  return custom_index_after_write_row(thd, table, new_record);
}

void custom_index_commit_stmt(THD *thd) {
  std::lock_guard<std::mutex> guard(g_runtime_mu);
  notify_backends_statement_end_locked(thd);
  g_thd_statement_touched.erase(thd);
  g_thd_statement_drops.erase(thd);
}

void custom_index_commit(THD *thd) {
  std::lock_guard<std::mutex> guard(g_runtime_mu);
  notify_backends_statement_end_locked(thd);
  auto drop_it = g_thd_transaction_drops.find(thd);
  if (drop_it != g_thd_transaction_drops.end()) {
    drop_loaded_indexes_locked(drop_it->second);
    g_thd_transaction_drops.erase(drop_it);
  }
  g_thd_statement_touched.erase(thd);
  g_thd_transaction_touched.erase(thd);
  g_thd_statement_drops.erase(thd);
}

void custom_index_rollback_stmt(THD *thd) {
  std::lock_guard<std::mutex> guard(g_runtime_mu);
  notify_backends_statement_end_locked(thd);
  auto it = g_thd_statement_touched.find(thd);
  if (it != g_thd_statement_touched.end()) {
    mark_dirty_locked(it->second);
    g_thd_statement_touched.erase(it);
  }

  auto drop_it = g_thd_statement_drops.find(thd);
  if (drop_it != g_thd_statement_drops.end()) {
    auto trx_it = g_thd_transaction_drops.find(thd);
    if (trx_it != g_thd_transaction_drops.end()) {
      for (const std::string &name : drop_it->second) {
        trx_it->second.erase(name);
      }
      if (trx_it->second.empty()) g_thd_transaction_drops.erase(trx_it);
    }
    g_thd_statement_drops.erase(drop_it);
  }
}

void custom_index_rollback(THD *thd) {
  std::lock_guard<std::mutex> guard(g_runtime_mu);
  notify_backends_statement_end_locked(thd);
  auto stmt_it = g_thd_statement_touched.find(thd);
  if (stmt_it != g_thd_statement_touched.end()) {
    mark_dirty_locked(stmt_it->second);
    g_thd_statement_touched.erase(stmt_it);
  }
  auto trx_it = g_thd_transaction_touched.find(thd);
  if (trx_it != g_thd_transaction_touched.end()) {
    mark_dirty_locked(trx_it->second);
    g_thd_transaction_touched.erase(trx_it);
  }
  g_thd_statement_drops.erase(thd);
  g_thd_transaction_drops.erase(thd);
}

void custom_index_schedule_drop(THD *thd, const char *db, const char *table,
                                const char *index_name) {
  if (thd == nullptr || db == nullptr || table == nullptr ||
      index_name == nullptr) {
    return;
  }
  std::lock_guard<std::mutex> guard(g_runtime_mu);
  const std::string name = table_index_name(db, table, index_name);
  g_thd_statement_drops[thd].insert(name);
  g_thd_transaction_drops[thd].insert(name);
}

bool custom_index_knn_scan_begin(TABLE *table, const char *index_name,
                                 const unsigned char *query_key,
                                 uint32_t query_key_len, uint32_t limit,
                                 CustomIndexKnnScan **scan, char *error_msg,
                                 uint32_t error_msg_len) {
  if (scan == nullptr) return true;
  *scan = nullptr;
  if (table == nullptr || table->s == nullptr || index_name == nullptr ||
      query_key == nullptr || query_key_len == 0) {
    snprintf(error_msg, error_msg_len, "invalid custom index KNN scan");
    return true;
  }

  VictionaryClient &vclient = VictionaryClient::instance();
  if (!vclient.is_initialized()) {
    snprintf(error_msg, error_msg_len, "Victionary is not initialized");
    return true;
  }

  const IndexEntry *matched_entry = nullptr;
  const IndexTypeDescriptor *matched_descriptor = nullptr;
  std::vector<std::string> matched_columns;
  {
    auto read_lock = vclient.get_read_lock();
    std::vector<const IndexEntry *> indexes = vclient.GetCustomIndexesForTable(
        table->s->db.str, table->s->table_name.str);
    for (const IndexEntry *entry : indexes) {
      if (my_strcasecmp(system_charset_info, entry->index_name().c_str(),
                        index_name) != 0) {
        continue;
      }
      const auto *descriptor = vclient.index_type_descriptors().get_committed(
          IndexTypeDescriptorKey(entry->index_type_name, entry->extension_name,
                                 entry->extension_version));
      if (descriptor == nullptr ||
          !(descriptor->intf().capabilities & VEF_INDEX_CAP_KNN)) {
        snprintf(error_msg, error_msg_len,
                 "custom index '%s' does not support KNN", index_name);
        return true;
      }
      get_custom_index_columns(/*thd=*/nullptr, entry->index_id,
                               &matched_columns);
      matched_entry = entry;
      matched_descriptor = descriptor;
      break;
    }
  }

  if (matched_entry == nullptr || matched_descriptor == nullptr) {
    snprintf(error_msg, error_msg_len, "custom index '%s' not found",
             index_name);
    return true;
  }
  if (matched_columns.size() != 1) {
    snprintf(error_msg, error_msg_len,
             "custom KNN scans require a one-column index");
    return true;
  }
  if (table->s->primary_key >= MAX_KEY) {
    snprintf(error_msg, error_msg_len,
             "custom KNN scans require a primary key");
    return true;
  }
  const KEY &primary_key = table->s->key_info[table->s->primary_key];
  if (primary_key.user_defined_key_parts != 1) {
    snprintf(error_msg, error_msg_len,
             "custom KNN scans require a one-column primary key");
    return true;
  }

  std::lock_guard<std::mutex> guard(g_runtime_mu);
  LoadedIndex *loaded = load_index_locked(
      table->s->db.str, table->s->table_name.str, *matched_entry,
      *matched_descriptor, static_cast<uint32_t>(matched_columns.size()),
      primary_key.user_defined_key_parts);
  if (loaded == nullptr) {
    snprintf(error_msg, error_msg_len, "failed to load custom index '%s'",
             index_name);
    return true;
  }
  if (loaded->dirty) {
    snprintf(error_msg, error_msg_len,
             "custom index '%s' is dirty and must be rebuilt", index_name);
    return true;
  }

  auto result = std::make_unique<CustomIndexKnnScan>();
  result->loaded = loaded;
  result->key_columns.resize(loaded->ctx.num_key_columns);
  result->pkey_columns.resize(loaded->ctx.num_primary_key_columns);

  vef_storage_col_data_t query_column{
      .data = query_key,
      .length = query_key_len,
  };
  vef_index_scan_key_t scan_key{
      .version = 1,
      .type = VEF_INDEX_SCAN_KEY_TYPE_KNN_QUERY,
      .num_key_columns = 1,
      .key_columns = &query_column,
      .include_key = true,
  };
  vef_index_scan_desc_t scan_desc{
      .version = 1,
      .scan_type = VEF_INDEX_SCAN_TYPE_KNN,
      .reverse = false,
      .limit = limit,
      .num_keys = 1,
      .keys = &scan_key,
  };

  if (loaded->intf.scan_begin(&loaded->ctx, loaded->storage, /*mctx=*/0,
                              &scan_desc, &result->cursor, &result->eof,
                              error_msg, error_msg_len)) {
    return true;
  }

  *scan = result.release();
  return false;
}

bool custom_index_knn_scan_next(CustomIndexKnnScan *scan,
                                const unsigned char **pkey_data,
                                uint32_t *pkey_len, bool *eof, char *error_msg,
                                uint32_t error_msg_len) {
  if (scan == nullptr || pkey_data == nullptr || pkey_len == nullptr ||
      eof == nullptr) {
    snprintf(error_msg, error_msg_len, "invalid custom index KNN cursor");
    return true;
  }
  if (scan->eof) {
    *eof = true;
    return false;
  }

  vef_storage_col_ref_t key_ref = VEF_STORAGE_EMPTY_COLUMN_REF;
  if (scan->loaded->intf.scan_fetch(
          scan->cursor, &key_ref, scan->key_columns.data(),
          scan->pkey_columns.data(), error_msg, error_msg_len)) {
    return true;
  }

  const vef_storage_col_data_t &pkey = scan->pkey_columns[0];
  if (pkey.data == nullptr || pkey.length == 0) {
    snprintf(error_msg, error_msg_len,
             "custom index KNN cursor returned an empty primary key");
    return true;
  }
  scan->pkey_buffer.assign(pkey.data, pkey.data + pkey.length);
  *pkey_data = scan->pkey_buffer.data();
  *pkey_len = static_cast<uint32_t>(scan->pkey_buffer.size());
  *eof = false;

  bool next_eof = false;
  if (scan->loaded->intf.scan_position(scan->cursor, VEF_INDEX_CURSOR_OP_NEXT,
                                       &next_eof, error_msg, error_msg_len)) {
    return true;
  }
  scan->eof = next_eof;
  return false;
}

void custom_index_knn_scan_end(CustomIndexKnnScan **scan) {
  if (scan == nullptr || *scan == nullptr) return;
  if ((*scan)->cursor != 0) {
    (*scan)->loaded->intf.scan_end(&(*scan)->cursor);
  }
  delete *scan;
  *scan = nullptr;
}

}  // namespace villagesql
