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

#include "villagesql/services/preview/table_storage.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "mysql/strings/m_ctype.h"
#include "sql/create_field.h"
#include "sql/current_thd.h"
#include "sql/dd/cache/dictionary_client.h"
#include "sql/dd/types/column.h"
#include "sql/dd/types/table.h"
#include "sql/field.h"
#include "sql/handler.h"
#include "sql/item.h"
#include "sql/key.h"
#include "sql/key_spec.h"
#include "sql/mdl.h"
#include "sql/mysqld.h"
#include "sql/sql_alter.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/sql_error.h"
#include "sql/sql_lex.h"
#include "sql/sql_list.h"
#include "sql/sql_table.h"
#include "sql/table.h"

struct TableStorageSecondaryIndex {
  std::string name;
  std::vector<uint32_t> column_indices;
  bool unique = false;
};

struct vef_table_storage_t {
  std::string db;
  std::string name;
  std::vector<vef_table_storage_col_def_t> columns;
  std::vector<std::string> column_names;
  std::vector<uint32_t> primary_key_columns;
  std::vector<TableStorageSecondaryIndex> secondary_indexes;
};

struct vef_table_storage_handle_t {
  vef_table_storage_t *table = nullptr;
  TABLE *sql_table = nullptr;
  vef_table_storage_lock_t lock = VEF_TABLE_STORAGE_LOCK_READ;
  bool external_lock = false;
};

struct BufferedRow {
  std::vector<std::vector<unsigned char>> values;
  std::vector<bool> nulls;
  // Engine row reference captured at scan time, so scan_position can
  // hand it back without keeping the underlying engine cursor alive.
  std::vector<unsigned char> ref;
};

struct vef_table_storage_cursor_t {
  std::vector<BufferedRow> rows;
  size_t pos = 0;
};

namespace villagesql::services {

constexpr const char kHiddenSchema[] = "villagesql";
const char *const kTableStorageSchema = kHiddenSchema;

namespace {

constexpr uint32_t kMaxIdentifierLength = 64;

bool fail(const char *message, char *error_msg, uint32_t error_msg_len) {
  if (error_msg != nullptr && error_msg_len > 0) {
    snprintf(error_msg, error_msg_len, "%s", message);
  }
  return true;
}

}  // namespace

std::string physical_table_storage_name(std::string_view logical_name) {
  std::string name("__hidden_");
  name.reserve(name.size() + logical_name.size());
  for (char ch : logical_name) {
    if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
        (ch >= '0' && ch <= '9') || ch == '_') {
      name.push_back(ch);
    } else {
      name.push_back('_');
    }
  }
  if (name.size() > kMaxIdentifierLength) name.resize(kMaxIdentifierLength);
  return name;
}

namespace {

const char *column_type_sql(const vef_table_storage_col_def_t &column) {
  switch (column.type) {
    case VEF_TABLE_STORAGE_COL_BYTES:
      return column.max_length > 255 ? "VARBINARY(4096)" : "VARBINARY(255)";
    case VEF_TABLE_STORAGE_COL_UINT64:
      return "BIGINT UNSIGNED";
    case VEF_TABLE_STORAGE_COL_INT64:
      return "BIGINT";
  }
  return nullptr;
}

bool validate_def(const vef_table_storage_def_t *def, char *error_msg,
                  uint32_t error_msg_len) {
  if (def == nullptr || def->version != 1 || def->logical_name == nullptr ||
      def->columns == nullptr || def->column_count == 0 ||
      def->primary_key_columns == nullptr ||
      def->primary_key_column_count == 0) {
    return fail("invalid hidden table definition", error_msg, error_msg_len);
  }
  for (uint32_t i = 0; i < def->column_count; i++) {
    const auto &column = def->columns[i];
    if (column.name == nullptr || strlen(column.name) == 0 ||
        strlen(column.name) > kMaxIdentifierLength ||
        column_type_sql(column) == nullptr) {
      return fail("invalid hidden table column definition", error_msg,
                  error_msg_len);
    }
  }
  for (uint32_t i = 0; i < def->primary_key_column_count; i++) {
    if (def->primary_key_columns[i] >= def->column_count) {
      return fail("invalid hidden table primary key definition", error_msg,
                  error_msg_len);
    }
  }
  for (uint32_t i = 0; i < def->secondary_index_count; i++) {
    const vef_table_storage_index_def_t &idx = def->secondary_indexes[i];
    if (idx.name == nullptr || strlen(idx.name) == 0 ||
        strlen(idx.name) > kMaxIdentifierLength || idx.column_count == 0 ||
        idx.column_indices == nullptr) {
      return fail("invalid hidden table secondary index", error_msg,
                  error_msg_len);
    }
    for (uint32_t j = 0; j < idx.column_count; j++) {
      if (idx.column_indices[j] >= def->column_count) {
        return fail("secondary index references unknown column", error_msg,
                    error_msg_len);
      }
    }
  }
  return false;
}

bool parse_integer(const vef_table_storage_value_t &value, bool is_unsigned,
                   longlong *out, char *error_msg, uint32_t error_msg_len) {
  if (value.data == nullptr || value.length == 0) {
    *out = 0;
    return false;
  }

  std::string text(reinterpret_cast<const char *>(value.data), value.length);
  char *end = nullptr;
  if (is_unsigned) {
    const unsigned long long parsed = strtoull(text.c_str(), &end, 10);
    if (end == text.c_str() || *end != '\0') {
      return fail("invalid hidden table unsigned integer", error_msg,
                  error_msg_len);
    }
    *out = static_cast<longlong>(parsed);
    return false;
  }

  const long long parsed = strtoll(text.c_str(), &end, 10);
  if (end == text.c_str() || *end != '\0') {
    return fail("invalid hidden table integer", error_msg, error_msg_len);
  }
  *out = static_cast<longlong>(parsed);
  return false;
}

bool store_value(Field *field, const vef_table_storage_value_t &value,
                 vef_table_storage_col_type_t type, char *error_msg,
                 uint32_t error_msg_len) {
  if (field == nullptr) {
    return fail("hidden table field is missing", error_msg, error_msg_len);
  }

  if (value.is_null) {
    field->set_null();
    return false;
  }
  field->set_notnull();

  switch (type) {
    case VEF_TABLE_STORAGE_COL_BYTES:
      return field->store(reinterpret_cast<const char *>(value.data),
                          value.length, &my_charset_bin) != TYPE_OK;
    case VEF_TABLE_STORAGE_COL_UINT64: {
      longlong parsed = 0;
      if (parse_integer(value, true, &parsed, error_msg, error_msg_len)) {
        return true;
      }
      return field->store(parsed, true) != TYPE_OK;
    }
    case VEF_TABLE_STORAGE_COL_INT64: {
      longlong parsed = 0;
      if (parse_integer(value, false, &parsed, error_msg, error_msg_len)) {
        return true;
      }
      return field->store(parsed, false) != TYPE_OK;
    }
  }
  return fail("unsupported hidden table column type", error_msg, error_msg_len);
}

bool store_row_values(TABLE *table, const vef_table_storage_t *table_storage,
                      const vef_table_storage_value_t *values,
                      uint32_t value_count, char *error_msg,
                      uint32_t error_msg_len) {
  if (table == nullptr || table_storage == nullptr ||
      value_count != table_storage->columns.size()) {
    return fail("invalid hidden table row", error_msg, error_msg_len);
  }

  restore_record(table, s->default_values);
  for (uint32_t i = 0; i < value_count; i++) {
    if (store_value(table->field[i], values[i], table_storage->columns[i].type,
                    error_msg, error_msg_len)) {
      return true;
    }
  }
  return false;
}

bool copy_field_value(Field *field, vef_table_storage_col_type_t type,
                      BufferedRow *row) {
  if (field == nullptr || field->is_null()) {
    row->nulls.push_back(true);
    row->values.emplace_back();
    return false;
  }
  row->nulls.push_back(false);

  if (type == VEF_TABLE_STORAGE_COL_UINT64 ||
      type == VEF_TABLE_STORAGE_COL_INT64) {
    // Emit as ASCII decimal so the round-trip read → write is symmetric:
    // extensions supply integer values via vef_table_storage_value_t as
    // ASCII (parse_integer expects that on insert / update / PK lookup),
    // so reads should produce the same shape.
    const longlong value = field->val_int();
    char buf[32];
    int len;
    if (type == VEF_TABLE_STORAGE_COL_UINT64) {
      len = snprintf(buf, sizeof(buf), "%llu",
                     static_cast<unsigned long long>(value));
    } else {
      len = snprintf(buf, sizeof(buf), "%lld", static_cast<long long>(value));
    }
    if (len < 0) len = 0;
    const auto *begin = reinterpret_cast<const unsigned char *>(buf);
    row->values.emplace_back(begin, begin + len);
    return false;
  }

  String buffer;
  String *value = field->val_str(&buffer);
  if (value == nullptr) {
    row->nulls.back() = true;
    row->values.emplace_back();
    return false;
  }
  row->values.emplace_back(
      reinterpret_cast<const unsigned char *>(value->ptr()),
      reinterpret_cast<const unsigned char *>(value->ptr()) + value->length());
  return false;
}

int table_storage_lock_type(vef_table_storage_lock_t lock) {
  return lock == VEF_TABLE_STORAGE_LOCK_WRITE ? F_WRLCK : F_RDLCK;
}

enum_mdl_type table_storage_mdl_type(vef_table_storage_lock_t lock) {
  return lock == VEF_TABLE_STORAGE_LOCK_WRITE ? MDL_SHARED_WRITE
                                              : MDL_SHARED_READ;
}

TABLE *open_table_storage(THD *thd, const vef_table_storage_t *table,
                          vef_table_storage_lock_t lock) {
  char path[FN_REFLEN + 1];
  bool was_truncated = false;
  build_table_filename(path, sizeof(path) - 1 - reg_ext_length,
                       table->db.c_str(), table->name.c_str(), "", 0,
                       &was_truncated);
  if (was_truncated) return nullptr;

  MDL_request table_request;
  MDL_REQUEST_INIT(&table_request, MDL_key::TABLE, table->db.c_str(),
                   table->name.c_str(), table_storage_mdl_type(lock),
                   MDL_TRANSACTION);
  if (thd->mdl_context.acquire_lock(&table_request,
                                    thd->variables.lock_wait_timeout)) {
    return nullptr;
  }

  const dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
  const dd::Table *table_def = nullptr;
  if (thd->dd_client()->acquire(table->db.c_str(), table->name.c_str(),
                                &table_def) ||
      table_def == nullptr) {
    return nullptr;
  }

  return open_table_uncached(thd, path, table->db.c_str(), table->name.c_str(),
                             false, true, *table_def);
}

// (forward declaration; impl below)
bool materialize_physical_table_storage_impl(THD *thd,
                                             const vef_table_storage_def_t *def,
                                             const std::string &physical_name,
                                             char *error_msg,
                                             uint32_t error_msg_len);

}  // namespace

bool materialize_physical_table_storage(THD *thd,
                                        const vef_table_storage_def_t *def,
                                        char *error_msg,
                                        uint32_t error_msg_len) {
  if (def == nullptr || def->logical_name == nullptr) {
    snprintf(error_msg, error_msg_len, "invalid hidden table definition");
    return true;
  }
  const std::string physical_name =
      physical_table_storage_name(std::string_view(def->logical_name));
  return materialize_physical_table_storage_impl(thd, def, physical_name,
                                                 error_msg, error_msg_len);
}

namespace {

// Materialize the physical table backing the hidden-table descriptor via
// mysql_create_table with IF NOT EXISTS semantics. Internal impl —
// callers use the externally-visible materialize_physical_table_storage
// above.
bool materialize_physical_table_storage_impl(THD *thd,
                                             const vef_table_storage_def_t *def,
                                             const std::string &physical_name,
                                             char *error_msg,
                                             uint32_t error_msg_len) {
  Alter_info alter_info(thd->mem_root);
  alter_info.flags = Alter_info::ALTER_ADD_COLUMN | Alter_info::ALTER_ADD_INDEX;

  char length_bufs[16][16]{};
  for (uint32_t i = 0; i < def->column_count; i++) {
    if (i >= sizeof(length_bufs) / sizeof(length_bufs[0])) {
      return fail("too many hidden table columns", error_msg, error_msg_len);
    }
    enum_field_types type{};
    const char *length_str = nullptr;
    uint type_modifier = def->columns[i].nullable ? 0 : NOT_NULL_FLAG;
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
      return fail("out of memory", error_msg, error_msg_len);
    }
    if (cr_field->init(thd, def->columns[i].name, type, length_str,
                       /*decimals=*/nullptr, type_modifier, /*default=*/nullptr,
                       /*on_update=*/nullptr, &EMPTY_CSTR, /*change=*/nullptr,
                       /*interval_list=*/nullptr,
                       def->columns[i].type == VEF_TABLE_STORAGE_COL_BYTES
                           ? &my_charset_bin
                           : nullptr,
                       /*has_explicit_collation=*/false, /*geom_type=*/0,
                       /*gcol_info=*/nullptr, /*default_val_expr=*/nullptr,
                       /*srid=*/{}, dd::Column::enum_hidden_type::HT_VISIBLE,
                       /*is_array=*/false)) {
      return fail("failed to init column", error_msg, error_msg_len);
    }
    if (alter_info.create_list.push_back(cr_field)) {
      return fail("failed to add column", error_msg, error_msg_len);
    }
  }

  List<Key_part_spec> pk_parts;
  for (uint32_t i = 0; i < def->primary_key_column_count; i++) {
    const char *col_name = def->columns[def->primary_key_columns[i]].name;
    auto *part = new (thd->mem_root) Key_part_spec(
        {col_name, strlen(col_name)}, /*prefix_length=*/0, ORDER_ASC);
    if (part == nullptr || pk_parts.push_back(part)) {
      return fail("failed to add PK part", error_msg, error_msg_len);
    }
  }
  auto *pk = new (thd->mem_root) Key_spec(
      thd->mem_root, KEYTYPE_PRIMARY, NULL_CSTR, &default_key_create_info,
      /*generated=*/false, /*check_for_duplicate_indexes=*/true, pk_parts);
  if (pk == nullptr || alter_info.key_list.push_back(pk)) {
    return fail("failed to add PK", error_msg, error_msg_len);
  }

  for (uint32_t i = 0; i < def->secondary_index_count; i++) {
    const vef_table_storage_index_def_t &idx = def->secondary_indexes[i];
    List<Key_part_spec> idx_parts;
    for (uint32_t j = 0; j < idx.column_count; j++) {
      if (idx.column_indices[j] >= def->column_count) {
        return fail("secondary index column index out of range", error_msg,
                    error_msg_len);
      }
      const char *col_name = def->columns[idx.column_indices[j]].name;
      auto *part = new (thd->mem_root) Key_part_spec(
          {col_name, strlen(col_name)}, /*prefix_length=*/0, ORDER_ASC);
      if (part == nullptr || idx_parts.push_back(part)) {
        return fail("failed to add secondary index part", error_msg,
                    error_msg_len);
      }
    }
    const keytype kt = idx.unique ? KEYTYPE_UNIQUE : KEYTYPE_MULTIPLE;
    const LEX_CSTRING idx_name = {idx.name, strlen(idx.name)};
    auto *key = new (thd->mem_root) Key_spec(
        thd->mem_root, kt, idx_name, &default_key_create_info,
        /*generated=*/false, /*check_for_duplicate_indexes=*/true, idx_parts);
    if (key == nullptr || alter_info.key_list.push_back(key)) {
      return fail("failed to add secondary index", error_msg, error_msg_len);
    }
    alter_info.flags |= Alter_info::ALTER_ADD_INDEX;
  }

  HA_CREATE_INFO create_info;
  create_info.db_type = ha_resolve_by_legacy_type(thd, DB_TYPE_INNODB);
  if (create_info.db_type == nullptr) {
    return fail("InnoDB handlerton unavailable", error_msg, error_msg_len);
  }
  create_info.row_type = ROW_TYPE_DEFAULT;
  create_info.default_table_charset = default_charset_info;
  create_info.table_charset = default_charset_info;
  create_info.alias = physical_name.c_str();
  create_info.options |= HA_LEX_CREATE_IF_NOT_EXISTS;

  Table_ref tref(kHiddenSchema, std::strlen(kHiddenSchema),
                 physical_name.c_str(), physical_name.length(),
                 physical_name.c_str(), TL_WRITE);
  tref.mdl_request.set_type(MDL_EXCLUSIVE);
  tref.open_strategy = Table_ref::OPEN_FOR_CREATE;

  Query_tables_list backup;
  thd->lex->reset_n_backup_query_tables_list(&backup);
  tref.next_global = nullptr;
  tref.next_local = nullptr;
  thd->lex->query_tables = &tref;
  thd->lex->query_tables_last = &tref.next_global;

  Diagnostics_area private_da(false);
  thd->push_diagnostics_area(&private_da);
  const bool result = mysql_create_table(thd, &tref, &create_info, &alter_info);
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
             kHiddenSchema, physical_name.c_str(), inner_errno, inner_err);
    return true;
  }
  return false;
}

bool table_storage_create(const vef_table_storage_def_t *def,
                          vef_table_storage_t **table, char *error_msg,
                          uint32_t error_msg_len) {
  if (table == nullptr) {
    return fail("hidden table output pointer is null", error_msg,
                error_msg_len);
  }
  *table = nullptr;
  if (validate_def(def, error_msg, error_msg_len)) return true;

  // NOTE: this entry point only allocates the in-memory descriptor that
  // points at the (already-materialized) physical hidden table. The
  // physical table is materialized via the DDL path (see
  // custom_index_pre_create_storage / materialize_physical_table_storage).
  // Callers from inside the runtime's locked region (on_load) cannot
  // run mysql_create_table safely without deadlocking on g_runtime_mu.

  auto result = std::make_unique<vef_table_storage_t>();
  result->db = kHiddenSchema;
  result->name = physical_table_storage_name(def->logical_name);
  result->columns.assign(def->columns, def->columns + def->column_count);
  result->primary_key_columns.assign(
      def->primary_key_columns,
      def->primary_key_columns + def->primary_key_column_count);
  result->column_names.reserve(def->column_count);
  for (uint32_t i = 0; i < def->column_count; i++) {
    result->column_names.emplace_back(def->columns[i].name);
    result->columns[i].name = result->column_names.back().c_str();
  }
  result->secondary_indexes.reserve(def->secondary_index_count);
  for (uint32_t i = 0; i < def->secondary_index_count; i++) {
    const vef_table_storage_index_def_t &src = def->secondary_indexes[i];
    TableStorageSecondaryIndex idx;
    idx.name = src.name != nullptr ? src.name : "";
    idx.column_indices.assign(src.column_indices,
                              src.column_indices + src.column_count);
    idx.unique = src.unique;
    result->secondary_indexes.push_back(std::move(idx));
  }

  *table = result.release();
  return false;
}

bool table_storage_drop(vef_table_storage_t *table,
                        char *error_msg [[maybe_unused]],
                        uint32_t error_msg_len [[maybe_unused]]) {
  if (table == nullptr) return false;
  const bool error = false;
  delete table;
  return error;
}

bool table_storage_open(vef_table_storage_t *table,
                        vef_table_storage_lock_t lock,
                        vef_table_storage_handle_t **handle, char *error_msg,
                        uint32_t error_msg_len) {
  if (table == nullptr || handle == nullptr) {
    return fail("invalid hidden table open arguments", error_msg,
                error_msg_len);
  }
  THD *thd = current_thd;
  if (thd == nullptr) {
    return fail("table_storage requires a THD", error_msg, error_msg_len);
  }

  auto result = std::make_unique<vef_table_storage_handle_t>();
  result->table = table;
  result->sql_table = open_table_storage(thd, table, lock);
  if (result->sql_table == nullptr) {
    return fail("failed to open hidden table", error_msg, error_msg_len);
  }
  if (result->sql_table->file->ha_external_lock(
          thd, table_storage_lock_type(lock))) {
    return fail("failed to lock hidden table", error_msg, error_msg_len);
  }
  result->external_lock = true;
  result->lock = lock;
  *handle = result.release();
  return false;
}

void table_storage_close(vef_table_storage_handle_t *handle) {
  if (handle == nullptr) return;
  if (handle->external_lock && handle->sql_table != nullptr) {
    handle->sql_table->file->ha_external_lock(current_thd, F_UNLCK);
  }
  if (handle->sql_table != nullptr) {
    intern_close_table(handle->sql_table);
  }
  delete handle;
}

bool table_storage_insert(vef_table_storage_handle_t *handle,
                          const vef_table_storage_value_t *values,
                          uint32_t value_count, char *error_msg,
                          uint32_t error_msg_len) {
  if (handle == nullptr || handle->table == nullptr || values == nullptr ||
      value_count != handle->table->columns.size()) {
    return fail("invalid hidden table insert arguments", error_msg,
                error_msg_len);
  }
  if (handle->sql_table == nullptr ||
      handle->lock != VEF_TABLE_STORAGE_LOCK_WRITE) {
    return fail("hidden table insert requires a write handle", error_msg,
                error_msg_len);
  }

  my_bitmap_map *old_write_set =
      dbug_tmp_use_all_columns(handle->sql_table, handle->sql_table->write_set);
  if (store_row_values(handle->sql_table, handle->table, values, value_count,
                       error_msg, error_msg_len)) {
    dbug_tmp_restore_column_map(handle->sql_table->write_set, old_write_set);
    return true;
  }
  dbug_tmp_restore_column_map(handle->sql_table->write_set, old_write_set);

  const int error =
      handle->sql_table->file->ha_write_row(handle->sql_table->record[0]);
  if (error != 0) {
    snprintf(error_msg, error_msg_len, "hidden table insert failed: %d", error);
    return true;
  }
  return false;
}

bool table_storage_delete(vef_table_storage_handle_t *handle,
                          const vef_table_storage_value_t *primary_key_values,
                          uint32_t primary_key_value_count, char *error_msg,
                          uint32_t error_msg_len) {
  if (handle == nullptr || handle->table == nullptr ||
      primary_key_values == nullptr ||
      primary_key_value_count != handle->table->primary_key_columns.size()) {
    return fail("invalid hidden table delete arguments", error_msg,
                error_msg_len);
  }
  if (handle->sql_table == nullptr ||
      handle->lock != VEF_TABLE_STORAGE_LOCK_WRITE) {
    return fail("hidden table delete requires a write handle", error_msg,
                error_msg_len);
  }
  if (handle->sql_table->s->primary_key >= MAX_KEY) {
    return fail("hidden table has no primary key", error_msg, error_msg_len);
  }

  TABLE *table = handle->sql_table;
  restore_record(table, s->default_values);
  my_bitmap_map *old_write_set =
      dbug_tmp_use_all_columns(table, table->write_set);
  for (uint32_t i = 0; i < primary_key_value_count; i++) {
    const uint32_t column_index = handle->table->primary_key_columns[i];
    if (store_value(table->field[column_index], primary_key_values[i],
                    handle->table->columns[column_index].type, error_msg,
                    error_msg_len)) {
      dbug_tmp_restore_column_map(table->write_set, old_write_set);
      return true;
    }
  }
  dbug_tmp_restore_column_map(table->write_set, old_write_set);

  const uint primary_key = table->s->primary_key;
  KEY *key_info = table->key_info + primary_key;
  std::vector<unsigned char> key(key_info->key_length);
  key_copy(key.data(), table->record[0], key_info, key_info->key_length);

  int error = table->file->ha_index_read_idx_map(table->record[1], primary_key,
                                                 key.data(), HA_WHOLE_KEY,
                                                 HA_READ_KEY_EXACT);
  if (error == HA_ERR_KEY_NOT_FOUND || error == HA_ERR_END_OF_FILE) {
    return false;
  }
  if (error != 0) {
    snprintf(error_msg, error_msg_len, "hidden table lookup failed: %d", error);
    return true;
  }
  error = table->file->ha_delete_row(table->record[1]);
  if (error != 0) {
    snprintf(error_msg, error_msg_len, "hidden table delete failed: %d", error);
    return true;
  }
  return false;
}

bool table_storage_update(vef_table_storage_handle_t *handle,
                          const vef_table_storage_value_t *primary_key_values,
                          uint32_t primary_key_value_count,
                          const vef_table_storage_value_t *new_values,
                          uint32_t new_value_count, char *error_msg,
                          uint32_t error_msg_len) {
  if (handle == nullptr || handle->table == nullptr ||
      primary_key_values == nullptr || new_values == nullptr ||
      primary_key_value_count != handle->table->primary_key_columns.size() ||
      new_value_count != handle->table->columns.size()) {
    return fail("invalid hidden table update arguments", error_msg,
                error_msg_len);
  }
  if (handle->sql_table == nullptr ||
      handle->lock != VEF_TABLE_STORAGE_LOCK_WRITE) {
    return fail("hidden table update requires a write handle", error_msg,
                error_msg_len);
  }
  if (handle->sql_table->s->primary_key >= MAX_KEY) {
    return fail("hidden table has no primary key", error_msg, error_msg_len);
  }

  TABLE *table = handle->sql_table;

  // Seed record[0] with the PK columns so we can look up the existing row.
  restore_record(table, s->default_values);
  my_bitmap_map *old_write_set =
      dbug_tmp_use_all_columns(table, table->write_set);
  for (uint32_t i = 0; i < primary_key_value_count; i++) {
    const uint32_t column_index = handle->table->primary_key_columns[i];
    if (store_value(table->field[column_index], primary_key_values[i],
                    handle->table->columns[column_index].type, error_msg,
                    error_msg_len)) {
      dbug_tmp_restore_column_map(table->write_set, old_write_set);
      return true;
    }
  }
  dbug_tmp_restore_column_map(table->write_set, old_write_set);

  const uint primary_key = table->s->primary_key;
  KEY *key_info = table->key_info + primary_key;
  std::vector<unsigned char> key(key_info->key_length);
  key_copy(key.data(), table->record[0], key_info, key_info->key_length);

  int error = table->file->ha_index_read_idx_map(table->record[1], primary_key,
                                                 key.data(), HA_WHOLE_KEY,
                                                 HA_READ_KEY_EXACT);
  if (error == HA_ERR_KEY_NOT_FOUND || error == HA_ERR_END_OF_FILE) {
    return fail("hidden table row not found for update", error_msg,
                error_msg_len);
  }
  if (error != 0) {
    snprintf(error_msg, error_msg_len, "hidden table lookup failed: %d", error);
    return true;
  }

  // record[1] now holds the existing row. Build the new image in
  // record[0] by overlaying new_values onto a copy of the old row, so
  // any column the caller wishes to leave alone (theoretically — today
  // they must supply all columns) gets the old value.
  store_record(table, record[1]);
  old_write_set = dbug_tmp_use_all_columns(table, table->write_set);
  if (store_row_values(table, handle->table, new_values, new_value_count,
                       error_msg, error_msg_len)) {
    dbug_tmp_restore_column_map(table->write_set, old_write_set);
    return true;
  }
  dbug_tmp_restore_column_map(table->write_set, old_write_set);

  error = table->file->ha_update_row(table->record[1], table->record[0]);
  if (error == HA_ERR_RECORD_IS_THE_SAME) return false;
  if (error != 0) {
    snprintf(error_msg, error_msg_len, "hidden table update failed: %d", error);
    return true;
  }
  return false;
}

bool table_storage_scan_begin(vef_table_storage_handle_t *handle,
                              const vef_table_storage_scan_t *scan,
                              vef_table_storage_cursor_t **cursor, bool *eof,
                              char *error_msg, uint32_t error_msg_len) {
  if (cursor == nullptr || eof == nullptr || handle == nullptr ||
      handle->table == nullptr || scan == nullptr || scan->version != 1) {
    return fail("invalid hidden table scan arguments", error_msg,
                error_msg_len);
  }
  if (handle->sql_table == nullptr) {
    return fail("hidden table scan requires an open handle", error_msg,
                error_msg_len);
  }
  *cursor = nullptr;
  *eof = true;

  if (scan->scan_type != VEF_TABLE_STORAGE_SCAN_FULL &&
      scan->scan_type != VEF_TABLE_STORAGE_SCAN_PRIMARY_KEY &&
      scan->scan_type != VEF_TABLE_STORAGE_SCAN_SECONDARY_INDEX) {
    return fail("unsupported hidden table scan type", error_msg, error_msg_len);
  }
  if (scan->scan_type == VEF_TABLE_STORAGE_SCAN_PRIMARY_KEY &&
      (scan->key_values == nullptr ||
       scan->key_value_count != handle->table->primary_key_columns.size())) {
    return fail("invalid hidden table primary key scan", error_msg,
                error_msg_len);
  }
  if (scan->scan_type == VEF_TABLE_STORAGE_SCAN_SECONDARY_INDEX &&
      (scan->secondary_index_name == nullptr ||
       (scan->direction != VEF_TABLE_STORAGE_SCAN_DIR_ASC &&
        scan->direction != VEF_TABLE_STORAGE_SCAN_DIR_DESC))) {
    return fail("invalid hidden table secondary index scan", error_msg,
                error_msg_len);
  }

  TABLE *table = handle->sql_table;
  auto result = std::make_unique<vef_table_storage_cursor_t>();
  my_bitmap_map *old_read_set =
      dbug_tmp_use_all_columns(table, table->read_set);

  if (scan->scan_type == VEF_TABLE_STORAGE_SCAN_PRIMARY_KEY) {
    if (table->s->primary_key >= MAX_KEY) {
      dbug_tmp_restore_column_map(table->read_set, old_read_set);
      return fail("hidden table has no primary key", error_msg, error_msg_len);
    }

    my_bitmap_map *old_write_set =
        dbug_tmp_use_all_columns(table, table->write_set);
    restore_record(table, s->default_values);
    for (uint32_t i = 0; i < scan->key_value_count; i++) {
      const uint32_t column_index = handle->table->primary_key_columns[i];
      if (store_value(table->field[column_index], scan->key_values[i],
                      handle->table->columns[column_index].type, error_msg,
                      error_msg_len)) {
        dbug_tmp_restore_column_map(table->write_set, old_write_set);
        dbug_tmp_restore_column_map(table->read_set, old_read_set);
        return true;
      }
    }
    dbug_tmp_restore_column_map(table->write_set, old_write_set);

    const uint primary_key = table->s->primary_key;
    KEY *key_info = table->key_info + primary_key;
    std::vector<unsigned char> key(key_info->key_length);
    key_copy(key.data(), table->record[0], key_info, key_info->key_length);

    const int error = table->file->ha_index_read_idx_map(
        table->record[0], primary_key, key.data(), HA_WHOLE_KEY,
        HA_READ_KEY_EXACT);
    if (error == HA_ERR_KEY_NOT_FOUND || error == HA_ERR_END_OF_FILE) {
      dbug_tmp_restore_column_map(table->read_set, old_read_set);
      *cursor = result.release();
      return false;
    }
    if (error != 0) {
      dbug_tmp_restore_column_map(table->read_set, old_read_set);
      snprintf(error_msg, error_msg_len, "hidden table lookup failed: %d",
               error);
      return true;
    }

    BufferedRow row;
    row.values.reserve(handle->table->columns.size());
    row.nulls.reserve(handle->table->columns.size());
    for (size_t i = 0; i < handle->table->columns.size(); i++) {
      copy_field_value(table->field[i], handle->table->columns[i].type, &row);
    }
    table->file->position(table->record[0]);
    row.ref.assign(table->file->ref,
                   table->file->ref + table->file->ref_length);
    result->rows.push_back(std::move(row));
    dbug_tmp_restore_column_map(table->read_set, old_read_set);
    *eof = false;
    *cursor = result.release();
    return false;
  }

  if (scan->scan_type == VEF_TABLE_STORAGE_SCAN_SECONDARY_INDEX) {
    // Resolve the secondary index name to the engine's key number.
    uint key_no = MAX_KEY;
    for (uint i = 0; i < table->s->keys; i++) {
      if (i == table->s->primary_key) continue;
      if (strcmp(table->s->key_info[i].name, scan->secondary_index_name) == 0) {
        key_no = i;
        break;
      }
    }
    if (key_no >= table->s->keys) {
      dbug_tmp_restore_column_map(table->read_set, old_read_set);
      return fail("hidden table secondary index not found", error_msg,
                  error_msg_len);
    }

    const int init_error = table->file->ha_index_init(key_no, true);
    if (init_error != 0) {
      dbug_tmp_restore_column_map(table->read_set, old_read_set);
      snprintf(error_msg, error_msg_len, "hidden table index_init failed: %d",
               init_error);
      return true;
    }

    int read_error = (scan->direction == VEF_TABLE_STORAGE_SCAN_DIR_ASC)
                         ? table->file->ha_index_first(table->record[0])
                         : table->file->ha_index_last(table->record[0]);
    uint32_t rows_read = 0;
    while (read_error == 0) {
      BufferedRow row;
      row.values.reserve(handle->table->columns.size());
      row.nulls.reserve(handle->table->columns.size());
      for (size_t i = 0; i < handle->table->columns.size(); i++) {
        copy_field_value(table->field[i], handle->table->columns[i].type, &row);
      }
      table->file->position(table->record[0]);
      row.ref.assign(table->file->ref,
                     table->file->ref + table->file->ref_length);
      result->rows.push_back(std::move(row));
      rows_read++;
      if (scan->limit > 0 && rows_read >= scan->limit) break;
      read_error = (scan->direction == VEF_TABLE_STORAGE_SCAN_DIR_ASC)
                       ? table->file->ha_index_next(table->record[0])
                       : table->file->ha_index_prev(table->record[0]);
    }
    dbug_tmp_restore_column_map(table->read_set, old_read_set);
    const int end_error = table->file->ha_index_end();
    if (read_error != 0 && read_error != HA_ERR_END_OF_FILE &&
        read_error != HA_ERR_KEY_NOT_FOUND) {
      snprintf(error_msg, error_msg_len,
               "hidden table secondary index scan failed: %d", read_error);
      return true;
    }
    if (end_error != 0) {
      snprintf(error_msg, error_msg_len, "hidden table index_end failed: %d",
               end_error);
      return true;
    }
    *eof = result->rows.empty();
    *cursor = result.release();
    return false;
  }

  int error = table->file->ha_rnd_init(true);
  if (error != 0) {
    dbug_tmp_restore_column_map(table->read_set, old_read_set);
    snprintf(error_msg, error_msg_len, "hidden table scan init failed: %d",
             error);
    return true;
  }

  uint32_t rows_read = 0;
  while ((error = table->file->ha_rnd_next(table->record[0])) == 0) {
    BufferedRow row;
    row.values.reserve(handle->table->columns.size());
    row.nulls.reserve(handle->table->columns.size());
    for (size_t i = 0; i < handle->table->columns.size(); i++) {
      copy_field_value(table->field[i], handle->table->columns[i].type, &row);
    }
    table->file->position(table->record[0]);
    row.ref.assign(table->file->ref,
                   table->file->ref + table->file->ref_length);
    result->rows.push_back(std::move(row));
    rows_read++;
    if (scan->limit > 0 && rows_read >= scan->limit) break;
  }
  dbug_tmp_restore_column_map(table->read_set, old_read_set);
  const int end_error = table->file->ha_rnd_end();
  if (error != HA_ERR_END_OF_FILE && error != 0) {
    snprintf(error_msg, error_msg_len, "hidden table scan failed: %d", error);
    return true;
  }
  if (end_error != 0) {
    snprintf(error_msg, error_msg_len, "hidden table scan end failed: %d",
             end_error);
    return true;
  }

  *eof = result->rows.empty();
  *cursor = result.release();
  return false;
}

bool table_storage_scan_next(vef_table_storage_cursor_t *cursor, bool *eof,
                             char *error_msg, uint32_t error_msg_len) {
  if (cursor == nullptr || eof == nullptr) {
    return fail("invalid hidden table cursor", error_msg, error_msg_len);
  }
  if (cursor->pos < cursor->rows.size()) cursor->pos++;
  *eof = cursor->pos >= cursor->rows.size();
  return false;
}

bool table_storage_scan_fetch(vef_table_storage_cursor_t *cursor,
                              vef_table_storage_value_t *values,
                              uint32_t value_count, char *error_msg,
                              uint32_t error_msg_len) {
  if (cursor == nullptr || values == nullptr ||
      cursor->pos >= cursor->rows.size()) {
    return fail("invalid hidden table fetch", error_msg, error_msg_len);
  }
  const BufferedRow &row = cursor->rows[cursor->pos];
  if (value_count != row.values.size()) {
    return fail("hidden table fetch value count mismatch", error_msg,
                error_msg_len);
  }
  for (uint32_t i = 0; i < value_count; i++) {
    values[i].is_null = row.nulls[i];
    values[i].data = row.values[i].data();
    values[i].length = static_cast<uint32_t>(row.values[i].size());
  }
  return false;
}

void table_storage_scan_end(vef_table_storage_cursor_t *cursor) {
  delete cursor;
}

bool table_storage_ref_length(vef_table_storage_handle_t *handle,
                              uint32_t *ref_length_out, char *error_msg,
                              uint32_t error_msg_len) {
  if (handle == nullptr || handle->sql_table == nullptr ||
      ref_length_out == nullptr) {
    return fail("invalid hidden table ref_length arguments", error_msg,
                error_msg_len);
  }
  *ref_length_out = handle->sql_table->file->ref_length;
  return false;
}

bool table_storage_scan_position(vef_table_storage_cursor_t *cursor,
                                 const unsigned char **ref_out,
                                 uint32_t *ref_len_out, char *error_msg,
                                 uint32_t error_msg_len) {
  if (cursor == nullptr || ref_out == nullptr || ref_len_out == nullptr ||
      cursor->pos >= cursor->rows.size()) {
    return fail("invalid hidden table scan_position arguments", error_msg,
                error_msg_len);
  }
  const BufferedRow &row = cursor->rows[cursor->pos];
  if (row.ref.empty()) {
    return fail("hidden table cursor has no captured ref", error_msg,
                error_msg_len);
  }
  *ref_out = row.ref.data();
  *ref_len_out = static_cast<uint32_t>(row.ref.size());
  return false;
}

bool table_storage_scan_seek(vef_table_storage_handle_t *handle,
                             const unsigned char *ref, uint32_t ref_len,
                             vef_table_storage_cursor_t **cursor_out,
                             bool *eof_out, char *error_msg,
                             uint32_t error_msg_len) {
  if (handle == nullptr || handle->sql_table == nullptr || ref == nullptr ||
      cursor_out == nullptr || eof_out == nullptr) {
    return fail("invalid hidden table scan_seek arguments", error_msg,
                error_msg_len);
  }
  TABLE *table = handle->sql_table;
  if (ref_len != table->file->ref_length) {
    return fail("hidden table scan_seek ref length mismatch", error_msg,
                error_msg_len);
  }
  *cursor_out = nullptr;
  *eof_out = true;

  auto result = std::make_unique<vef_table_storage_cursor_t>();
  my_bitmap_map *old_read_set =
      dbug_tmp_use_all_columns(table, table->read_set);

  const int init_error = table->file->ha_rnd_init(false);
  if (init_error != 0) {
    dbug_tmp_restore_column_map(table->read_set, old_read_set);
    snprintf(error_msg, error_msg_len,
             "hidden table scan_seek rnd_init failed: %d", init_error);
    return true;
  }
  const int read_error = table->file->ha_rnd_pos(
      table->record[0], const_cast<unsigned char *>(ref));
  if (read_error == HA_ERR_KEY_NOT_FOUND || read_error == HA_ERR_END_OF_FILE ||
      read_error == HA_ERR_RECORD_DELETED) {
    table->file->ha_rnd_end();
    dbug_tmp_restore_column_map(table->read_set, old_read_set);
    *cursor_out = result.release();
    return false;
  }
  if (read_error != 0) {
    table->file->ha_rnd_end();
    dbug_tmp_restore_column_map(table->read_set, old_read_set);
    snprintf(error_msg, error_msg_len,
             "hidden table scan_seek rnd_pos failed: %d", read_error);
    return true;
  }

  BufferedRow row;
  row.values.reserve(handle->table->columns.size());
  row.nulls.reserve(handle->table->columns.size());
  for (size_t i = 0; i < handle->table->columns.size(); i++) {
    copy_field_value(table->field[i], handle->table->columns[i].type, &row);
  }
  row.ref.assign(ref, ref + ref_len);
  result->rows.push_back(std::move(row));

  table->file->ha_rnd_end();
  dbug_tmp_restore_column_map(table->read_set, old_read_set);
  *eof_out = false;
  *cursor_out = result.release();
  return false;
}

vef_preview_table_storage_t g_table_storage_vtable{
    VEF_PREVIEW_TABLE_STORAGE_ABI_VERSION,
    table_storage_create,
    table_storage_drop,
    table_storage_open,
    table_storage_close,
    table_storage_insert,
    table_storage_delete,
    table_storage_update,
    table_storage_scan_begin,
    table_storage_scan_next,
    table_storage_scan_fetch,
    table_storage_scan_end,
    table_storage_ref_length,
    table_storage_scan_position,
    table_storage_scan_seek};

}  // namespace

vef_preview_table_storage_t *preview_table_storage_vtable() {
  return &g_table_storage_vtable;
}

}  // namespace villagesql::services
