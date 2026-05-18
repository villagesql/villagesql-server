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

#ifndef VILLAGESQL_SQL_METADATA_MODIFIER_H_
#define VILLAGESQL_SQL_METADATA_MODIFIER_H_

#include <string>
#include <utility>
#include <vector>

#include "sql/mdl.h"
#include "villagesql/schema/systable/custom_columns.h"
#include "villagesql/schema/systable/custom_index_columns.h"
#include "villagesql/schema/systable/custom_indexes.h"
#include "villagesql/sql/custom_vdf.h"

class Alter_info;
class Create_field;
class Croutine_hash_entry;
struct HA_CREATE_INFO;
struct handlerton;
struct TABLE;
class Table_ref;
class THD;
template <typename T>
class List;
template <typename T>
class SQL_I_List;
class sp_head;
class sp_name;

namespace villagesql {

/**
 * Metadata_modifier - Central manager for custom column metadata operations.
 *
 * This class handles tracking, validation, and persistence of custom column
 * metadata during DDL operations. It ensures proper MDL locking on extensions
 * and integrates with MySQL's transaction system for atomic DDL.
 *
 * ## Responsibilities:
 * - Track custom column additions, deletions, and modifications during DDL
 * - Acquire MDL_SHARED locks on extensions to prevent concurrent uninstall
 * - Validate that custom types exist before committing DDL changes
 * - Delegate low-level access and persistence of metadata to
 *   VictionaryClient, which encapsulates direct interactions with the
 *   villagesql.custom_columns system table
 *
 * ## Usage Pattern:
 * 1. DDL Planning Phase:
 *    Call process_create(), process_drop(), process_alter(), or
 *    process_rename() during DDL planning to track custom columns and acquire
 *    extension MDL locks.
 *
 * 2. DDL Execution Phase:
 *    Call store() to write tracked changes to the system table. This must be
 *    called before the DDL statement commits.
 *
 * 3. Transaction Completion:
 *    The transaction layer (transaction.cc) automatically calls commit() on
 *    transaction success or rollback() on transaction failure to update the
 *    in-memory victionary cache.
 *
 * ## Example:
 *   // During CREATE TABLE
 *   if (Metadata_modifier::process_create(thd, create_info, alter_info,
 *                                         db, table_name)) {
 *     return true;  // Error acquiring locks or validating types
 *   }
 *   // ... execute DDL ...
 *   if (Metadata_modifier::store(thd)) {
 *     return true;  // Error writing to system table
 *   }
 *   // Transaction layer handles commit()/rollback()
 *
 * ## Thread Safety:
 * All public methods are static and thread-safe. Internal state is stored
 * per-THD in the VictionaryClient's uncommitted transaction map.
 */
class Metadata_modifier {
 public:
  // Process custom columns for CREATE TABLE operations.
  // Returns false on success, true on error.
  static bool process_create(THD *thd, const HA_CREATE_INFO *create_info,
                             const Alter_info *alter_info, const char *db,
                             const char *table_name);

  // Process custom columns for DROP TABLE operations.
  // Returns false on success, true on error.
  template <typename TableContainer>
  static bool process_drop(THD *thd, const TableContainer &tables);

  // Process custom columns for ALTER TABLE operations.
  // Returns false on success, true on error.
  static bool process_alter(THD *thd, const HA_CREATE_INFO *create_info,
                            Table_ref *table_list,
                            const Alter_info *alter_info);

  // Process custom columns for RENAME TABLE operations.
  // table_list contains pairs of tables (old, new, old, new, ...).
  // Returns false on success, true on error.
  static bool process_rename(THD *thd, Table_ref *table_list);

  // Process custom function calls from the given list.
  // Returns false on success, true on error.
  static bool process_calls(THD *thd,
                            SQL_I_List<Croutine_hash_entry> &croutines_list);

  // Store all uncommitted entries to the victionary system tables.
  // Returns false on success, true on error.
  static bool store(THD *thd);

  // Commit all modifications to the victionary system tables.
  // Returns false on success, true on error.
  static bool commit(THD *thd);

  // Rollback all modifications before storing to the victionary system tables.
  static void rollback(THD *thd);

  // RAII guard for ALTER TABLE: clears villagesql_alter_custom_fields on all
  // exit paths, and rolls back victionary modifications unless disarm() is
  // called after a successful store().
  class AlterGuard {
   public:
    explicit AlterGuard(THD *thd) : thd_(thd) {}
    AlterGuard(const AlterGuard &) = delete;
    AlterGuard &operator=(const AlterGuard &) = delete;
    ~AlterGuard();
    void disarm() { armed_ = false; }

   private:
    THD *thd_;
    bool armed_ = true;
  };

  // Acquire an X (exclusive) MDL lock on an extension with the specified
  // duration. Normalizes the extension name before acquiring the lock.
  // Returns false on success, true on error.
  static bool lock_extension_exclusive(THD *thd,
                                       const std::string &extension_name,
                                       enum_mdl_duration duration);

  // Returns true if there are custom column or index entries to process.
  bool has_entries() const {
    return !to_add_.empty() || !to_remove_.empty() || !to_rename_.empty() ||
           !to_call_.empty() || !to_add_indexes_.empty() ||
           !to_add_index_columns_.empty() || !to_remove_indexes_.empty() ||
           !to_remove_index_columns_.empty();
  }

 private:
  // Pair containing (db_name, table_name)
  using Table_name = std::pair<const char *, const char *>;

  // Add column entries from create_fields to the list of columns to be added.
  // Returns false on success, true on error.
  bool add_columns(THD *thd, Table_name db_table,
                   const List<Create_field> &create_fields);

  // Collect custom index entries from alter_info->key_list into
  // to_add_indexes_ and to_add_index_columns_. Only processes Key_spec entries
  // whose KEY_CREATE_INFO::custom_index_type is set.
  // Returns false on success, true on error.
  bool add_indexes(THD *thd, const char *db, const char *table_name,
                   const Alter_info *alter_info);

  // Stage removal of custom indexes named in alter_info->drop_list (DROP INDEX
  // via ALTER TABLE). Looks up each dropped key in VictionaryClient and queues
  // its IndexKey and all child IndexColumnKeys for deletion.
  // Returns false on success, true on error.
  bool remove_indexes(THD *thd, const char *db, const char *table_name,
                      const Alter_info *alter_info);

  // Stage removal of all custom indexes for a table (DROP TABLE path).
  // Returns false on success, true on error.
  bool remove_all_indexes(THD *thd, const char *db, const char *table_name);

  // Remove a column entry to the list of columns to be removed.
  // Returns false on success, true on error.
  bool remove_columns(THD *thd, Table_name db_table);

  // Update the table name associated with columns from old_name to new_name.
  // Returns false on success, true on error.
  bool rename_columns_table(THD *thd, Table_name old_name, Table_name new_name);

  // Modify column entries based on alter_info. table_ref is the table being
  // altered.
  // Returns false on success, true on error.
  bool alter_columns(THD *thd, Table_ref *table_ref,
                     const Alter_info *alter_info);

  // Lock extensions, validate custom columns and mark modifications to
  // victionary.
  // Returns false on success, true on error.
  bool lock_and_apply(THD *thd);

  // Acquire MDL_SHARED locks on all extensions referenced in pending
  // modifications. Returns false on success, true on error.
  bool lock_extensions_shared(THD *thd);

  // Validate that all referenced types exist in VictionaryClient with matching
  // extension name and version. Returns false on success, true on error.
  bool validate_entries();

  // Mark all pending column modifications in VictionaryClient.
  // Sets marked_column to true if any column entries were marked.
  // Sets marked_index to true if any index entries were marked.
  // Returns false on success, true on error.
  bool mark_victionary_modifications(THD *thd, bool &marked_column,
                                     bool &marked_index);

  // Add system tables for staged modifications to the query list.
  // Opens COLUMNS_TABLE_NAME only if marked_column, and INDEXES_TABLE_NAME
  // and INDEX_COLUMNS_TABLE_NAME only if marked_index.
  // Returns false on success, true on error.
  bool add_system_tables(THD *thd, bool marked_column, bool marked_index);

  // Add custom function entries.
  // Returns false on success, true on error.
  bool add_functions(SQL_I_List<Croutine_hash_entry> &croutines_list);

  // Verify that the storage engine is InnoDB; otherwise, report an error.
  // Returns false on success and true on error.
  static bool ensure_engine_is_innodb(const handlerton *hton,
                                      const char *operation);

  std::vector<ColumnEntry> to_add_;
  std::vector<ColumnEntry> to_remove_;
  size_t existing_custom_count_ = 0;
  // Pair of (new_entry, old_key) where old_key is the key of the old entry
  std::vector<std::pair<ColumnEntry, ColumnKey>> to_rename_;
  // Custom functions to be called
  std::vector<Croutine_entry> to_call_;
  // Custom index entries staged for insertion
  std::vector<IndexEntry> to_add_indexes_;
  std::vector<IndexColumnEntry> to_add_index_columns_;
  // Custom index entries staged for deletion
  std::vector<IndexKey> to_remove_indexes_;
  std::vector<IndexColumnKey> to_remove_index_columns_;
};

// Template implementation for process_drop
template <typename TableContainer>
bool Metadata_modifier::process_drop(THD *thd, const TableContainer &tables) {
  Metadata_modifier modifier;
  for (auto *table : tables) {
    Table_name db_table = {table->db, table->table_name};
    if (modifier.remove_columns(thd, db_table)) {
      return true;
    }
    if (modifier.remove_all_indexes(thd, table->db, table->table_name)) {
      return true;
    }
  }

  if (modifier.lock_and_apply(thd)) {
    return true;
  }

  return false;
}

// Persist custom type metadata for all custom-typed variables (params and
// DECLARE locals) in a stored procedure/function to
// villagesql.custom_sp_params. Called after the routine is created in the data
// dictionary. Returns false on success, true on error.
extern bool PersistCustomSpParams(THD *thd, sp_head *sp);

// Delete custom type metadata for all variables of a stored procedure/function
// from villagesql.custom_sp_params. Called before the routine is dropped.
// Returns false on success, true on error.
extern bool DeleteCustomSpParams(THD *thd, const sp_name *name);

}  // namespace villagesql

#endif  // VILLAGESQL_SQL_METADATA_MODIFIER_H_
