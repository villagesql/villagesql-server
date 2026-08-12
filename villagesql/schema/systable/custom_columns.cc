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

#include "villagesql/schema/systable/custom_columns.h"

#include "scope_guard.h"
#include "sql/field.h"
#include "sql/table.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/systable/helpers.h"

namespace villagesql {

bool TableTraits<ColumnEntry>::read_from_table(TABLE &table,
                                               ColumnEntry &entry) {
  Field **field = table.field;

  // Read key components (fields 0-2)
  std::string db, tbl, col;
  read_string_field(field[0], db);
  read_string_field(field[1], tbl);
  read_string_field(field[2], col);
  entry.set_key(ColumnKey(std::move(db), std::move(tbl), std::move(col)));

  // extension_name (field 3)
  read_string_field(field[3], entry.extension_name);

  // extension_version (field 4)
  read_string_field(field[4], entry.extension_version);

  // type_name (field 5)
  read_string_field(field[5], entry.type_name);

  // type_parameters (field 6)
  read_string_field(field[6], entry.type_parameters);

  return false;  // Success
}

bool TableTraits<ColumnEntry>::write_to_table(TABLE &table,
                                              const ColumnEntry &entry) {
  Field **field = table.field;

  // Set all fields for the row
  // db_name (field 0) - required
  field[0]->store(entry.db_name().c_str(), entry.db_name().length(),
                  &my_charset_utf8mb4_bin);

  // table_name (field 1) - required
  field[1]->store(entry.table_name().c_str(), entry.table_name().length(),
                  &my_charset_utf8mb4_bin);

  // column_name (field 2) - required
  field[2]->store(entry.column_name().c_str(), entry.column_name().length(),
                  &my_charset_utf8mb4_bin);

  // extension_name (field 3) - required
  field[3]->store(entry.extension_name.c_str(), entry.extension_name.length(),
                  &my_charset_utf8mb4_bin);

  // extension_version (field 4) - required
  field[4]->store(entry.extension_version.c_str(),
                  entry.extension_version.length(), &my_charset_utf8mb4_bin);

  // type_name (field 5) - required
  field[5]->store(entry.type_name.c_str(), entry.type_name.length(),
                  &my_charset_utf8mb4_bin);

  // type_parameters (field 6)
  field[6]->store(entry.type_parameters.c_str(), entry.type_parameters.length(),
                  &my_charset_utf8mb4_bin);

  // Write the row - use ha_write_row for INSERT
  int error = table.file->ha_write_row(table.record[0]);
  if (should_assert_if_true(error)) {
    LogVSQL(ERROR_LEVEL,
            "Failed to write row for column '%s' in table '%s.%s': error %d",
            entry.column_name().c_str(), entry.db_name().c_str(),
            entry.table_name().c_str(), error);
    return true;
  }

  return false;
}

bool TableTraits<ColumnEntry>::update_in_table(TABLE &table,
                                               const ColumnEntry &entry,
                                               const std::string &old_key) {
  // Determine which key to use for lookup
  std::string lookup_key = old_key.empty() ? entry.key().str() : old_key;

  // Parse the lookup key to get db, table, column names
  // Key format: "db.table.column"
  size_t first_dot = lookup_key.find('.');
  size_t second_dot = lookup_key.find('.', first_dot + 1);
  if (should_assert_if_true(first_dot == std::string::npos ||
                            second_dot == std::string::npos)) {
    LogVSQL(ERROR_LEVEL, "Invalid key format for update: %s",
            lookup_key.c_str());
    return true;
  }

  std::string old_db = lookup_key.substr(0, first_dot);
  std::string old_table =
      lookup_key.substr(first_dot + 1, second_dot - first_dot - 1);
  std::string old_column = lookup_key.substr(second_dot + 1);

  // Set up the key fields for index lookup
  Field **field = table.field;

  field[0]->store(old_db.c_str(), old_db.length(), &my_charset_utf8mb4_bin);
  field[1]->store(old_table.c_str(), old_table.length(),
                  &my_charset_utf8mb4_bin);
  field[2]->store(old_column.c_str(), old_column.length(),
                  &my_charset_utf8mb4_bin);

  // Build the index key from the populated fields
  uchar key_buf[MAX_KEY_LENGTH];
  key_copy(key_buf, table.record[0], table.key_info,
           table.key_info->key_length);

  // Save old record for update
  store_record(&table, record[1]);

  // Find the row using index read
  int error = table.file->ha_index_init(0, false);
  if (error) {
    LogVSQL(ERROR_LEVEL, "Failed to init index for update: error %d", error);
    return true;
  }

  auto index_end_guard =
      create_scope_guard([&table]() { table.file->ha_index_end(); });

  error = table.file->ha_index_read_map(table.record[0], key_buf, HA_WHOLE_KEY,
                                        HA_READ_KEY_EXACT);
  if (error) {
    LogVSQL(ERROR_LEVEL, "Failed to find row for update: error %d", error);
    return true;
  }

  // Now update the fields with new values
  field[0]->store(entry.db_name().c_str(), entry.db_name().length(),
                  &my_charset_utf8mb4_bin);
  field[1]->store(entry.table_name().c_str(), entry.table_name().length(),
                  &my_charset_utf8mb4_bin);
  field[2]->store(entry.column_name().c_str(), entry.column_name().length(),
                  &my_charset_utf8mb4_bin);
  field[3]->store(entry.extension_name.c_str(), entry.extension_name.length(),
                  &my_charset_utf8mb4_bin);
  field[4]->store(entry.extension_version.c_str(),
                  entry.extension_version.length(), &my_charset_utf8mb4_bin);
  field[5]->store(entry.type_name.c_str(), entry.type_name.length(),
                  &my_charset_utf8mb4_bin);
  field[6]->store(entry.type_parameters.c_str(), entry.type_parameters.length(),
                  &my_charset_utf8mb4_bin);

  // Update the row
  error = table.file->ha_update_row(table.record[1], table.record[0]);

  if (error && error != HA_ERR_RECORD_IS_THE_SAME) {
    LogVSQL(ERROR_LEVEL, "Failed to update row: error %d", error);
    return true;
  }

  return false;  // Success
}

bool TableTraits<ColumnEntry>::delete_from_table(TABLE &table,
                                                 const ColumnEntry &entry) {
  // Set up the key fields for index lookup
  Field **field = table.field;
  field[0]->store(entry.db_name().c_str(), entry.db_name().length(),
                  &my_charset_utf8mb4_bin);
  field[1]->store(entry.table_name().c_str(), entry.table_name().length(),
                  &my_charset_utf8mb4_bin);
  field[2]->store(entry.column_name().c_str(), entry.column_name().length(),
                  &my_charset_utf8mb4_bin);

  // Build the index key from the populated fields
  uchar key_buf[MAX_KEY_LENGTH];
  key_copy(key_buf, table.record[0], table.key_info,
           table.key_info->key_length);

  // Find the row using index read
  int error = table.file->ha_index_init(0, false);
  if (error) {
    LogVSQL(ERROR_LEVEL, "Failed to init index for delete: error %d", error);
    return true;
  }

  auto index_end_guard =
      create_scope_guard([&table]() { table.file->ha_index_end(); });

  error = table.file->ha_index_read_map(table.record[0], key_buf, HA_WHOLE_KEY,
                                        HA_READ_KEY_EXACT);
  if (error) {
    if (error == HA_ERR_KEY_NOT_FOUND) {
      // Row doesn't exist - this is OK for delete
      LogVSQL(WARNING_LEVEL, "Custom column row not found for delete: %s.%s.%s",
              entry.db_name().c_str(), entry.table_name().c_str(),
              entry.column_name().c_str());
      return false;
    }
    LogVSQL(ERROR_LEVEL, "Failed to find row for delete: error %d", error);
    return true;
  }

  // Delete the row
  error = table.file->ha_delete_row(table.record[0]);

  if (error) {
    LogVSQL(ERROR_LEVEL, "Failed to delete row: error %d", error);
    return true;
  }

  return false;  // Success
}

}  // namespace villagesql
