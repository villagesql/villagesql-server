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

#include "villagesql/schema/systable/custom_index_columns.h"

#include "my_inttypes.h"
#include "scope_guard.h"
#include "sql/field.h"
#include "sql/table.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/systable/helpers.h"

namespace villagesql {

// Field layout for villagesql.custom_index_columns:
//   field[0] = index_id               BIGINT UNSIGNED
//   field[1] = key_position           INT UNSIGNED
//   field[2] = column_name            VARCHAR(64)
//   field[3] = extension_name         VARCHAR(64)   (profile's extension)
//   field[4] = extension_version      VARCHAR(64)   (profile's extension)
//   field[5] = profile_name           VARCHAR(64)
//
// Key layout:
//   key_info[0] = PRIMARY KEY (index_id, key_position)
//
// All operations use the PRIMARY KEY for lookups.
// update_in_table supports updating non-key fields (e.g. profile rebinding
// after an extension upgrade). The key (index_id, key_position) never changes.

bool TableTraits<IndexColumnEntry>::read_from_table(TABLE &table,
                                                    IndexColumnEntry &entry) {
  Field **field = table.field;

  uint64_t index_id = static_cast<uint64_t>(field[0]->val_int());
  unsigned int key_pos = 0;
  read_unsigned_field(field[1], key_pos);
  entry.set_key(IndexColumnKey(index_id, static_cast<uint32_t>(key_pos)));

  read_string_field(field[2], entry.column_name);
  read_string_field(field[3], entry.profile_extension_name);
  read_string_field(field[4], entry.profile_extension_version);
  read_string_field(field[5], entry.profile_name);

  return false;
}

bool TableTraits<IndexColumnEntry>::write_to_table(
    TABLE &table, const IndexColumnEntry &entry) {
  Field **field = table.field;

  field[0]->store(static_cast<longlong>(entry.index_id()), true);
  field[1]->store(static_cast<longlong>(entry.key_position()), true);
  field[2]->store(entry.column_name.c_str(), entry.column_name.length(),
                  &my_charset_utf8mb4_bin);
  field[3]->store(entry.profile_extension_name.c_str(),
                  entry.profile_extension_name.length(),
                  &my_charset_utf8mb4_bin);
  field[4]->store(entry.profile_extension_version.c_str(),
                  entry.profile_extension_version.length(),
                  &my_charset_utf8mb4_bin);
  field[5]->store(entry.profile_name.c_str(), entry.profile_name.length(),
                  &my_charset_utf8mb4_bin);

  int error = table.file->ha_write_row(table.record[0]);
  if (should_assert_if_true(error)) {
    LogVSQL(ERROR_LEVEL,
            "Failed to write row for index column (index_id=%" PRIu64
            ", key_position=%u): error %d",
            entry.index_id(), entry.key_position(), error);
    return true;
  }

  return false;
}

bool TableTraits<IndexColumnEntry>::update_in_table(
    TABLE &table, const IndexColumnEntry &entry, const std::string &old_key) {
  std::string lookup_key = old_key.empty() ? entry.key().str() : old_key;

  // Parse key "index_id.key_position"
  size_t dot = lookup_key.find('.');
  if (should_assert_if_true(dot == std::string::npos)) {
    LogVSQL(ERROR_LEVEL, "Invalid key format for index column update: %s",
            lookup_key.c_str());
    return true;
  }

  uint64_t old_index_id =
      static_cast<uint64_t>(std::stoull(lookup_key.substr(0, dot)));
  uint32_t old_key_position =
      static_cast<uint32_t>(std::stoul(lookup_key.substr(dot + 1)));

  Field **field = table.field;

  // Set fields[0-1] for primary key lookup (key_info[0])
  field[0]->store(static_cast<longlong>(old_index_id), true);
  field[1]->store(static_cast<longlong>(old_key_position), true);

  uchar key_buf[MAX_KEY_LENGTH];
  key_copy(key_buf, table.record[0], table.key_info,
           table.key_info->key_length);

  store_record(&table, record[1]);

  int error = table.file->ha_index_init(0, false);
  if (error) {
    LogVSQL(ERROR_LEVEL,
            "Failed to init index for index column update: error %d", error);
    return true;
  }

  auto index_end_guard =
      create_scope_guard([&table]() { table.file->ha_index_end(); });

  error = table.file->ha_index_read_map(table.record[0], key_buf, HA_WHOLE_KEY,
                                        HA_READ_KEY_EXACT);
  if (error) {
    LogVSQL(ERROR_LEVEL, "Failed to find row for index column update: error %d",
            error);
    return true;
  }

  // Update all fields; key (index_id, key_position) is preserved
  field[0]->store(static_cast<longlong>(entry.index_id()), true);
  field[1]->store(static_cast<longlong>(entry.key_position()), true);
  field[2]->store(entry.column_name.c_str(), entry.column_name.length(),
                  &my_charset_utf8mb4_bin);
  field[3]->store(entry.profile_extension_name.c_str(),
                  entry.profile_extension_name.length(),
                  &my_charset_utf8mb4_bin);
  field[4]->store(entry.profile_extension_version.c_str(),
                  entry.profile_extension_version.length(),
                  &my_charset_utf8mb4_bin);
  field[5]->store(entry.profile_name.c_str(), entry.profile_name.length(),
                  &my_charset_utf8mb4_bin);

  error = table.file->ha_update_row(table.record[1], table.record[0]);
  if (error && error != HA_ERR_RECORD_IS_THE_SAME) {
    LogVSQL(ERROR_LEVEL, "Failed to update index column row: error %d", error);
    return true;
  }

  return false;
}

bool TableTraits<IndexColumnEntry>::delete_from_table(
    TABLE &table, const IndexColumnEntry &entry) {
  Field **field = table.field;

  // Set fields[0-1] for primary key lookup (key_info[0])
  field[0]->store(static_cast<longlong>(entry.index_id()), true);
  field[1]->store(static_cast<longlong>(entry.key_position()), true);

  uchar key_buf[MAX_KEY_LENGTH];
  key_copy(key_buf, table.record[0], table.key_info,
           table.key_info->key_length);

  int error = table.file->ha_index_init(0, false);
  if (error) {
    LogVSQL(ERROR_LEVEL,
            "Failed to init index for index column delete: error %d", error);
    return true;
  }

  auto index_end_guard =
      create_scope_guard([&table]() { table.file->ha_index_end(); });

  error = table.file->ha_index_read_map(table.record[0], key_buf, HA_WHOLE_KEY,
                                        HA_READ_KEY_EXACT);
  if (error) {
    if (error == HA_ERR_KEY_NOT_FOUND) {
      LogVSQL(WARNING_LEVEL,
              "Custom index column row not found for delete: "
              "index_id=%" PRIu64 ", key_position=%u",
              entry.index_id(), entry.key_position());
      return false;
    }
    LogVSQL(ERROR_LEVEL, "Failed to find row for index column delete: error %d",
            error);
    return true;
  }

  error = table.file->ha_delete_row(table.record[0]);
  if (error) {
    LogVSQL(ERROR_LEVEL, "Failed to delete index column row: error %d", error);
    return true;
  }

  return false;
}

}  // namespace villagesql
