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
#include <string>
#include <vector>

#include "my_base.h"
#include "mysql/strings/m_ctype.h"
#include "sql/field.h"
#include "sql/handler.h"
#include "sql/key.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/table.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/descriptor/index_type_descriptor.h"
#include "villagesql/schema/schema_manager.h"
#include "villagesql/schema/systable/custom_index_columns.h"
#include "villagesql/schema/systable/custom_indexes.h"
#include "villagesql/schema/victionary_client.h"
#include "villagesql/sql/custom_index_runtime_internal.h"

namespace villagesql {

constexpr size_t kErrorMessageLength = 512;

// Reads the committed index columns. The scan/insert path always reads
// committed columns, so thd is unused here. Declared in
// custom_index_runtime_internal.h so custom_index_knn_scan.cc and
// custom_index_runtime_dml.cc can call it.
//
// TODO(villagesql-indexing): add the rebuild variant (thd != nullptr, reading
// uncommitted columns via custom_index_columns().get_prefix) needed by
// ALTER-rebuild maintenance; it lives in custom_index_runtime_dml.cc.
bool get_custom_index_columns(THD * /*thd*/, uint64_t index_id,
                              std::vector<std::string> *out) {
  VictionaryClient &vclient = VictionaryClient::instance();
  std::vector<const IndexColumnEntry *> columns =
      vclient.GetColumnsForIndex(index_id);
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

bool get_loaded_custom_index(handler *file, uint key_idx,
                             const vef_type_index_intf_t **intf,
                             vef_index_ctx_t **ctx,
                             vef_storage_ctx_t **storage) {
  handler::CustomIndexHandle handle;
  if (file->get_custom_index_handle(key_idx, &handle)) return true;
  *intf = static_cast<const vef_type_index_intf_t *>(handle.intf);
  *ctx = static_cast<vef_index_ctx_t *>(handle.index_ctx);
  *storage = static_cast<vef_storage_ctx_t *>(handle.storage_ctx);
  return *intf == nullptr || *ctx == nullptr;
}

// ===========================================================================
// GLUE: handler-level INSERT maintenance.
//
// Everything below maintains custom indexes from the HANDLER level, off the
// ha_write_row hook: it reconstructs a row's identity and column values from
// the handler-supplied row image and drives the extension's intf.insert.
//
// There are two independent convergences toward InnoDB's native handling.
// Only the first is done:
//
//   1. Loaded-index instance — DONE. This code no longer keeps its own
//      loaded-index cache. It obtains the loaded instance (intf + index_ctx +
//      storage_ctx) from the storage engine via
//      handler::get_custom_index_handle
//      -> dict_index_t::custom_index, which InnoDB loads once at table-open
//      with the correct persisted storage_ref and whose lifetime the engine
//      owns. The scan path (custom_index_knn_scan.cc) uses the same handle, so
//      insert and scan share one instance.
//
//   2. Maintenance location — PENDING. The maintenance itself still runs above
//      the storage engine (this ha_write_row hook + orchestration + row-image
//      marshalling + the index-name -> key-slot lookup). The same job is done
//      natively inside InnoDB's row layer (keyed off dict_index_t) with direct
//      access to the clustered-index record and row id. If custom-index
//      maintenance converges onto that in-engine path, this whole section is
//      expected to be deleted.
// ===========================================================================

namespace {

// The scan/insert path here populates a custom index only via INSERT after
// the index exists, so it never maintains an ALTER-rebuild shadow.
//
// TODO(villagesql-indexing): wire ALTER-rebuild bridging so CREATE INDEX on an
// already-populated table indexes the existing rows. It maintains the #sql-xxx
// rebuild shadow as its rows are copied (is_runtime_alter_rebuild and the
// maintenance it gates), and relies on the THD villagesql_alter_target_db/table
// stash and the victionary get_prefix (uncommitted) lookups. It lives in
// custom_index_runtime_dml.cc.

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

// Build the row identity the extension stores so it can point back at the
// base-table row. This fills the intf.insert `pkey_columns` array — to the
// extension these are opaque identity bytes (it stores them and returns them
// at scan time); the ABI does not care what they mean. We currently populate
// them with the base table's primary-key column values, and the scan iterator
// fetches the row with a primary-key lookup (ha_index_read_map).
//
// We use PK columns because this runtime maintains the index at the HANDLER
// level (ha_write_row), where InnoDB's internal row reference is not available
// — the only row identity reconstructable from the handler-supplied row image
// is its PK columns, which also forces the base table to have a usable PK.
// Index maintenance driven from INSIDE the engine (e.g. from the InnoDB row
// layer, keyed off dict_index_t) has the clustered-index position / DB_ROW_ID
// directly and does not need this reconstruction. If maintenance moves into
// the engine, this function and its PK requirement go away.
//
// TODO(villagesql-indexing): drop the PK requirement without moving maintenance
// into the engine, by filling pkey_columns with the engine row reference
// (handler::ref, from handler::position()) instead of PK column values, and
// fetching base rows via ha_rnd_pos(ref). This is a SERVER-side change only —
// the pkey_columns bytes stay opaque to the extension, so no SDK/ABI change is
// needed. It works even when the engine synthesizes a hidden row id, so CREATE
// INDEX would succeed on tables without an explicit PK. Until then, base tables
// without a usable PK are rejected here.
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
    Field *field = find_field_in_table_sef(table, column.c_str());
    if (field == nullptr) return true;
    owned_buffers->emplace_back();
    out->push_back(
        field_data_for_record(field, record, &owned_buffers->back()));
  }
  return false;
}

// Find the table->key_info[] slot whose key name matches `index_name`. Returns
// true and sets *key_idx on match; false if the table has no such key. The
// storage engine uses this same key numbering (see innobase_get_index).
bool find_key_slot(TABLE *table, const char *index_name, uint *key_idx) {
  for (uint i = 0; i < table->s->keys; ++i) {
    if (table->key_info[i].name != nullptr &&
        my_strcasecmp(system_charset_info, table->key_info[i].name,
                      index_name) == 0) {
      *key_idx = i;
      return true;
    }
  }
  return false;
}

int apply_to_custom_indexes(
    THD * /*thd*/, TABLE *table, const uchar *record,
    const std::function<bool(const vef_type_index_intf_t *, vef_index_ctx_t *,
                             vef_storage_ctx_t *, vef_storage_col_data_t *,
                             vef_storage_col_data_t *, char *, uint32_t)>
        &callback) {
  // TODO(villagesql-indexing): Make internal system-table writes bypass
  // extension index maintenance through an explicit server-side context flag.
  // For now, avoid re-entering Victionary/custom-index code while persisting
  // VillageSQL metadata such as INSTALL/UNINSTALL EXTENSION changes.
  //
  // This path populates a custom index only via INSERT after the index
  // exists, so there is no rebuild lookup here.
  //
  // TODO(villagesql-indexing): add the ALTER-rebuild path (maintaining the
  // #sql-xxx shadow so CREATE INDEX on a populated table indexes existing
  // rows); it lives in custom_index_runtime_dml.cc.
  if (is_runtime_internal_table(table)) return 0;

  const char *lookup_db = table->s->db.str;
  const char *lookup_table = table->s->table_name.str;
  VictionaryClient &vclient = VictionaryClient::instance();
  if (!vclient.is_initialized()) return 0;

  std::vector<const IndexEntry *> indexes;
  std::map<uint64_t, std::vector<std::string>> index_columns;
  {
    auto read_lock = vclient.get_read_lock();
    indexes = vclient.GetCustomIndexesForTable(lookup_db, lookup_table);
    if (indexes.empty()) return 0;

    for (const IndexEntry *entry : indexes) {
      // Validate the index type is registered. The actual extension interface
      // used at runtime comes from the engine handle below, not from this
      // descriptor; this is a registration/consistency gate.
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

      std::vector<std::string> names;
      get_custom_index_columns(/*thd=*/nullptr, entry->index_id, &names);
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

    // Drive the extension against the loaded index instance the storage engine
    // already holds for this index (loaded at table-open with the correct
    // persisted storage_ref). This is the same handle the scan path uses, so
    // insert and scan operate on the same storage context.
    uint key_idx = 0;
    if (!find_key_slot(table, entry->index_name().c_str(), &key_idx)) {
      LogVSQL(ERROR_LEVEL,
              "Custom index '%s' has no matching key on table '%s.%s'",
              entry->index_name().c_str(), table->s->db.str,
              table->s->table_name.str);
      return HA_ERR_INTERNAL_ERROR;
    }
    const vef_type_index_intf_t *intf = nullptr;
    vef_index_ctx_t *ctx = nullptr;
    vef_storage_ctx_t *storage = nullptr;
    if (get_loaded_custom_index(table->file, key_idx, &intf, &ctx, &storage)) {
      LogVSQL(ERROR_LEVEL,
              "Custom index '%s' is not loaded by the storage engine",
              entry->index_name().c_str());
      return HA_ERR_INTERNAL_ERROR;
    }

    char error_msg[kErrorMessageLength]{};
    const bool failed =
        callback(intf, ctx, storage, key_columns.data(), pkey_columns.data(),
                 error_msg, sizeof(error_msg));
    if (failed) {
      // TODO(villagesql-indexing): the ABI conflates soft (e.g.
      // uniqueness violation) and hard (e.g. corrupted state) failures
      // into a single bool return. We log at WARNING and avoid
      // poisoning the index; revisit once the ABI distinguishes the two so
      // corruption can be surfaced more loudly while expected rejections stay
      // quiet. Tests that exercise the soft-failure path need to
      // mtr.add_suppression this WARNING line.
      LogVSQL(WARNING_LEVEL, "Custom index '%s' maintenance failed: %s",
              entry->index_name().c_str(), error_msg);
      return HA_ERR_INTERNAL_ERROR;
    }
  }

  return 0;
}

}  // namespace

int custom_index_after_write_row(THD *thd, TABLE *table, const uchar *record) {
  return apply_to_custom_indexes(
      thd, table, record,
      [](const vef_type_index_intf_t *intf, vef_index_ctx_t *ctx,
         vef_storage_ctx_t *storage, vef_storage_col_data_t *key_columns,
         vef_storage_col_data_t *pkey_columns, char *error_msg,
         uint32_t error_msg_len) {
        vef_storage_col_ref_t key_ref = VEF_STORAGE_EMPTY_COLUMN_REF;
        return intf->insert(ctx, storage, /*trx_ref=*/0, key_columns,
                            pkey_columns, &key_ref, error_msg, error_msg_len);
      });
}

}  // namespace villagesql
