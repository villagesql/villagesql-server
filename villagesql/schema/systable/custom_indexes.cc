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

#include "villagesql/schema/systable/custom_indexes.h"

#include <memory>
#include <string>

#include "include/sql_string.h"
#include "scope_guard.h"
#include "sql-common/json_dom.h"
#include "sql-common/json_error_handler.h"
#include "sql/field.h"
#include "sql/table.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/systable/helpers.h"

namespace villagesql {

// Field layout for villagesql.custom_indexes:
//   field[0] = index_id               BIGINT UNSIGNED
//   field[1] = db_name                VARCHAR(64)
//   field[2] = table_name             VARCHAR(64)
//   field[3] = index_name             VARCHAR(64)
//   field[4] = extension_name         VARCHAR(64)
//   field[5] = extension_version      VARCHAR(64)
//   field[6] = index_type_name        VARCHAR(64)
//   field[7] = index_type_parameters  JSON
//
// Key layout:
//   key_info[0] = PRIMARY KEY (index_id)
//   key_info[1] = UNIQUE KEY idx_natural (db_name, table_name, index_name)
//
// update_in_table and delete_from_table use key_info[1] (the natural unique
// key) because callers hold a natural key string, not index_id.

bool TableTraits<IndexEntry>::read_from_table(TABLE &table, IndexEntry &entry) {
  Field **field = table.field;

  entry.index_id = static_cast<uint64_t>(field[0]->val_int());

  std::string db, tbl, idx;
  read_string_field(field[1], db);
  read_string_field(field[2], tbl);
  read_string_field(field[3], idx);
  entry.set_key(IndexKey(std::move(db), std::move(tbl), std::move(idx)));

  read_string_field(field[4], entry.extension_name);
  read_string_field(field[5], entry.extension_version);
  read_string_field(field[6], entry.index_type_name);
  read_string_field(field[7], entry.index_type_parameters);

  return false;
}

bool TableTraits<IndexEntry>::write_to_table(TABLE &table,
                                             const IndexEntry &entry) {
  Field **field = table.field;

  field[0]->store(static_cast<longlong>(entry.index_id), true);
  field[1]->store(entry.db_name().c_str(), entry.db_name().length(),
                  &my_charset_utf8mb4_bin);
  field[2]->store(entry.table_name().c_str(), entry.table_name().length(),
                  &my_charset_utf8mb4_bin);
  field[3]->store(entry.index_name().c_str(), entry.index_name().length(),
                  &my_charset_utf8mb4_bin);
  field[4]->store(entry.extension_name.c_str(), entry.extension_name.length(),
                  &my_charset_utf8mb4_bin);
  field[5]->store(entry.extension_version.c_str(),
                  entry.extension_version.length(), &my_charset_utf8mb4_bin);
  field[6]->store(entry.index_type_name.c_str(), entry.index_type_name.length(),
                  &my_charset_utf8mb4_bin);
  field[7]->store(entry.index_type_parameters.c_str(),
                  entry.index_type_parameters.length(),
                  &my_charset_utf8mb4_bin);

  int error = table.file->ha_write_row(table.record[0]);
  if (should_assert_if_true(error)) {
    LogVSQL(ERROR_LEVEL,
            "Failed to write row for index '%s' in table '%s.%s': error %d",
            entry.index_name().c_str(), entry.db_name().c_str(),
            entry.table_name().c_str(), error);
    return true;
  }

  return false;
}

bool TableTraits<IndexEntry>::update_in_table(TABLE &table,
                                              const IndexEntry &entry,
                                              const std::string &old_key) {
  std::string lookup_key = old_key.empty() ? entry.key().str() : old_key;

  // Parse natural key "db.table.index_name"
  size_t first_dot = lookup_key.find('.');
  size_t second_dot = lookup_key.find('.', first_dot + 1);
  if (should_assert_if_true(first_dot == std::string::npos ||
                            second_dot == std::string::npos)) {
    LogVSQL(ERROR_LEVEL, "Invalid key format for index update: %s",
            lookup_key.c_str());
    return true;
  }

  std::string old_db = lookup_key.substr(0, first_dot);
  std::string old_table =
      lookup_key.substr(first_dot + 1, second_dot - first_dot - 1);
  std::string old_index = lookup_key.substr(second_dot + 1);

  Field **field = table.field;

  // Set fields[1-3] for natural unique key lookup (key_info[1])
  field[1]->store(old_db.c_str(), old_db.length(), &my_charset_utf8mb4_bin);
  field[2]->store(old_table.c_str(), old_table.length(),
                  &my_charset_utf8mb4_bin);
  field[3]->store(old_index.c_str(), old_index.length(),
                  &my_charset_utf8mb4_bin);

  uchar key_buf[MAX_KEY_LENGTH];
  key_copy(key_buf, table.record[0], &table.key_info[1],
           table.key_info[1].key_length);

  store_record(&table, record[1]);

  int error = table.file->ha_index_init(1, false);
  if (error) {
    LogVSQL(ERROR_LEVEL, "Failed to init index for index update: error %d",
            error);
    return true;
  }

  auto index_end_guard =
      create_scope_guard([&table]() { table.file->ha_index_end(); });

  error = table.file->ha_index_read_map(table.record[0], key_buf, HA_WHOLE_KEY,
                                        HA_READ_KEY_EXACT);
  if (error) {
    LogVSQL(ERROR_LEVEL, "Failed to find row for index update: error %d",
            error);
    return true;
  }

  // Update all fields; index_id is preserved (rename does not change it)
  field[0]->store(static_cast<longlong>(entry.index_id), true);
  field[1]->store(entry.db_name().c_str(), entry.db_name().length(),
                  &my_charset_utf8mb4_bin);
  field[2]->store(entry.table_name().c_str(), entry.table_name().length(),
                  &my_charset_utf8mb4_bin);
  field[3]->store(entry.index_name().c_str(), entry.index_name().length(),
                  &my_charset_utf8mb4_bin);
  field[4]->store(entry.extension_name.c_str(), entry.extension_name.length(),
                  &my_charset_utf8mb4_bin);
  field[5]->store(entry.extension_version.c_str(),
                  entry.extension_version.length(), &my_charset_utf8mb4_bin);
  field[6]->store(entry.index_type_name.c_str(), entry.index_type_name.length(),
                  &my_charset_utf8mb4_bin);
  field[7]->store(entry.index_type_parameters.c_str(),
                  entry.index_type_parameters.length(),
                  &my_charset_utf8mb4_bin);

  error = table.file->ha_update_row(table.record[1], table.record[0]);
  if (error && error != HA_ERR_RECORD_IS_THE_SAME) {
    LogVSQL(ERROR_LEVEL, "Failed to update index row: error %d", error);
    return true;
  }

  return false;
}

bool TableTraits<IndexEntry>::delete_from_table(TABLE &table,
                                                const IndexEntry &entry) {
  Field **field = table.field;

  // Set fields[1-3] for natural unique key lookup (key_info[1])
  field[1]->store(entry.db_name().c_str(), entry.db_name().length(),
                  &my_charset_utf8mb4_bin);
  field[2]->store(entry.table_name().c_str(), entry.table_name().length(),
                  &my_charset_utf8mb4_bin);
  field[3]->store(entry.index_name().c_str(), entry.index_name().length(),
                  &my_charset_utf8mb4_bin);

  uchar key_buf[MAX_KEY_LENGTH];
  key_copy(key_buf, table.record[0], &table.key_info[1],
           table.key_info[1].key_length);

  int error = table.file->ha_index_init(1, false);
  if (error) {
    LogVSQL(ERROR_LEVEL, "Failed to init index for index delete: error %d",
            error);
    return true;
  }

  auto index_end_guard =
      create_scope_guard([&table]() { table.file->ha_index_end(); });

  error = table.file->ha_index_read_map(table.record[0], key_buf, HA_WHOLE_KEY,
                                        HA_READ_KEY_EXACT);
  if (error) {
    if (error == HA_ERR_KEY_NOT_FOUND) {
      LogVSQL(WARNING_LEVEL, "Custom index row not found for delete: %s.%s.%s",
              entry.db_name().c_str(), entry.table_name().c_str(),
              entry.index_name().c_str());
      return false;
    }
    LogVSQL(ERROR_LEVEL, "Failed to find row for index delete: error %d",
            error);
    return true;
  }

  error = table.file->ha_delete_row(table.record[0]);
  if (error) {
    LogVSQL(ERROR_LEVEL, "Failed to delete index row: error %d", error);
    return true;
  }

  return false;
}

std::string params_to_json(const Mem_root_array<IndexWithParam> &params) {
  Json_object_ptr obj(new (std::nothrow) Json_object());
  if (!obj) return "{}";

  // Parameters are stored in Json_object using a std::map ordered by
  // Json_key_comparator (length first, then memcmp), producing a
  // deterministic canonical serialization order.
  for (const IndexWithParam &p : params) {
    std::string key(p.key.str, p.key.length);
    Json_dom_ptr val;
    if (p.is_string) {
      val.reset(new (std::nothrow) Json_string(
          std::string(p.value.str.str, p.value.str.length)));
    } else {
      val.reset(new (std::nothrow) Json_uint(p.value.num));
    }
    if (!val || obj->add_alias(key, std::move(val))) return "{}";  // OOM
  }

  Json_wrapper wrapper(obj.release());
  StringBuffer<256> buf;
  if (wrapper.to_string(&buf, false, "params_to_json", JsonDepthErrorHandler))
    return "{}";
  return std::string(buf.ptr(), buf.length());
}

}  // namespace villagesql
