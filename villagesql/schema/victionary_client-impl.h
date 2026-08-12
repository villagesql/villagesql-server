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

#include "villagesql/include/error.h"

namespace villagesql {

template <typename EntryType, PersistenceMode Mode>
template <PersistenceMode M,
          std::enable_if_t<M == PersistenceMode::PERSISTENT, int>>
bool SystemTableMap<EntryType, Mode>::reload_from_table(
    THD *thd, const char *schema_name, const char *table_name) {
  assert_write_lock_held();  // Caller must hold write lock

  if (should_assert_if_null(thd) || should_assert_if_null(schema_name) ||
      should_assert_if_null(table_name)) {
    LogVSQL(ERROR_LEVEL, "reload_from_table() called with null parameters");
    return true;
  }

  // Clear existing entries for this table first
  clear();

  // Prepare table list
  Table_ref tables(schema_name, table_name, TL_READ);

  // Check access permissions if ACL is enabled
  if (should_assert_if_true(!thd->slave_thread &&
                            !thd->is_initialize_system_thread() &&
                            check_one_table_access(thd, SELECT_ACL, &tables))) {
    LogVSQL(ERROR_LEVEL, "No permission to read %s.%s", schema_name,
            table_name);
    return true;
  }

  // Open the table
  TABLE *table = open_ltable(thd, &tables, TL_READ, MYSQL_LOCK_IGNORE_TIMEOUT);
  if (should_assert_if_null(table)) {
    LogVSQL(ERROR_LEVEL, "Cannot open %s.%s table", schema_name, table_name);
    return true;
  }

  // Ensure cleanup on exit
  auto cleanup_guard =
      create_scope_guard([&thd]() { commit_and_close_mysql_tables(thd); });

  // Mark all fields as readable
  table->use_all_columns();

  // Read all rows
  if (should_assert_if_true(table->file->ha_rnd_init(true))) {
    LogVSQL(ERROR_LEVEL, "Failed to scan %s.%s table", schema_name, table_name);
    return true;
  }

  auto rnd_end_guard =
      create_scope_guard([&table]() { table->file->ha_rnd_end(); });

  bool error = false;
  size_t loaded_count = 0;
  int read_error;

  while (!(read_error = table->file->ha_rnd_next(table->record[0]))) {
    EntryType entry;

    // Use TableTraits to read from table row
    if (should_assert_if_true(
            TableTraits<EntryType>::read_from_table(*table, entry))) {
      LogVSQL(ERROR_LEVEL, "Failed to read %s.%s table row", schema_name,
              table_name);
      error = true;
      continue;  // Skip this row, try to load others
    }

    // Use entry's key() method
    std::string key_str = entry.key().str();

    // Add to committed map
    m_committed[key_str] = std::make_shared<EntryType>(std::move(entry));
    loaded_count++;
  }

  if (should_assert_if_true(read_error != HA_ERR_END_OF_FILE)) {
    LogVSQL(ERROR_LEVEL, "Error reading %s.%s table: %d", schema_name,
            table_name, read_error);
    error = true;
  }

  LogVSQL(INFORMATION_LEVEL, "Loaded %zu %s.%s entries", loaded_count,
          schema_name, table_name);

  return error;
}

template <typename EntryType, PersistenceMode Mode>
template <PersistenceMode M,
          std::enable_if_t<M == PersistenceMode::PERSISTENT, int>>
bool SystemTableMap<EntryType, Mode>::write_uncommitted_to_table(
    THD *thd, const char *schema_name, const char *table_name) {
  assert_read_or_write_lock_held();  // Caller must hold at least read lock

  if (!thd || !schema_name || !table_name) {
    return false;  // Nothing to write
  }

  // Get uncommitted operations for this THD
  auto thd_it = m_uncommitted.find(thd);
  if (thd_it == m_uncommitted.end() || thd_it->second.elements == 0) {
    return false;  // No operations to write
  }

  // Find the open TABLE* for this system table in THD's open tables list
  TABLE *sys_table = find_open_table(thd, schema_name, table_name);
  if (should_assert_if_null(sys_table)) {
    LogVSQL(ERROR_LEVEL, "%s.%s not open for writing", schema_name, table_name);
    return true;
  }

  // Enable writes to all fields.
  sys_table->use_all_columns();

  // Apply each pending operation to the table in order
  for (PendingOperation<EntryType> *op = thd_it->second.first; op;
       op = op->next) {
    switch (op->op_type) {
      case OperationType::INSERT:
        // Prepare record for write (sets defaults)
        restore_record(sys_table, s->default_values);

        // Use TableTraits to populate the row fields and insert
        if (should_assert_if_true(TableTraits<EntryType>::write_to_table(
                *sys_table, *op->entry))) {
          LogVSQL(ERROR_LEVEL, "Error inserting into %s.%s for key %s",
                  schema_name, table_name, op->entry->key().str().c_str());
          return true;
        }
        break;

      case OperationType::UPDATE:
        // Use TableTraits to update the row
        if (should_assert_if_true(TableTraits<EntryType>::update_in_table(
                *sys_table, *op->entry, op->key.str()))) {
          LogVSQL(ERROR_LEVEL, "Error updating %s.%s from key %s to %s",
                  schema_name, table_name, op->entry->key().str().c_str(),
                  op->key.str().c_str());
          return true;
        }
        break;

      case OperationType::DELETE: {
        // For DELETE, we only have the key, not the full entry.
        // Create a temporary entry from the key for delete_from_table.
        EntryType temp_entry(op->key);
        if (should_assert_if_true(TableTraits<EntryType>::delete_from_table(
                *sys_table, temp_entry))) {
          LogVSQL(ERROR_LEVEL, "Error deleting from %s.%s for key %s",
                  schema_name, table_name, op->key.str().c_str());
          return true;
        }
        break;
      }
    }
  }

  return false;
}

}  // namespace villagesql
