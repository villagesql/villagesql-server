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

#include "villagesql/schema/victionary_client.h"

#include "my_inttypes.h"
#include "scope_guard.h"
#include "sql/auth/auth_common.h"
#include "sql/auth/sql_security_ctx.h"
#include "sql/field.h"
#include "sql/handler.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/table.h"
#include "sql/thd_raii.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/schema_manager.h"

namespace villagesql {

// ===== VictionaryClient implementation =====

VictionaryClient &VictionaryClient::instance() {
  static VictionaryClient instance;
  return instance;
}

VictionaryClient::~VictionaryClient() {
  if (m_initialized.load()) {
    // During program shutdown, logging system may be destroyed
    // So we skip the destroy() call which logs messages
    // Just clean up the lock directly
    mysql_rwlock_destroy(&m_lock);
    m_initialized.store(false);
  }
}

bool VictionaryClient::init(THD *thd) {
  // Check if already initialized or currently initializing - both are errors
  if (should_assert_if_true(m_initialized.load()) ||
      m_initializing.exchange(true)) {
    LogVSQL(ERROR_LEVEL, "VictionaryClient initialized twice!");
    return true;
  }

  // Clear the flag when done, even for errors
  auto guard = create_scope_guard([this]() { m_initializing.store(false); });

  if (should_assert_if_null(thd)) {
    LogVSQL(ERROR_LEVEL, "THD invalid for VictionaryClient!");
    return true;
  }

  // Initialize the rwlock. We need to do this here, instead of in the
  // constructor, since we do lazy initialization of the singleton.
  if (should_assert_if_true(mysql_rwlock_init(PSI_NOT_INSTRUMENTED, &m_lock))) {
    LogVSQL(ERROR_LEVEL, "Failed to initialize VictionaryClient lock");
    return true;
  }

  // No assert: reload can fail on bad table contents (e.g. rows with
  // duplicate canonical keys), which is a datadir fault, not a code bug.
  if (reload_all_tables(thd)) {
    LogVSQL(ERROR_LEVEL, "Failed to load all villagesql tables");
    return true;
  }

  m_initialized.store(true);

  LogVSQL(INFORMATION_LEVEL, "VictionaryClient initialized");
  return false;
}

bool VictionaryClient::init_for_testing() {
  // Check if already initialized or currently initializing - both are errors
  if (should_assert_if_true(m_initialized.load() ||
                            m_initializing.exchange(true))) {
    LogVSQL(ERROR_LEVEL, "VictionaryClient initialized twice!");
    return true;
  }

  // Clear the flag when done, even for errors
  auto guard = create_scope_guard([this]() { m_initializing.store(false); });

  // Initialize the rwlock. We need to do this here, instead of in the
  // constructor, since we do lazy initialization of the singleton.
  if (should_assert_if_true(mysql_rwlock_init(PSI_NOT_INSTRUMENTED, &m_lock))) {
    LogVSQL(ERROR_LEVEL, "Failed to initialize VictionaryClient lock");
    return true;
  }

  // Skip loading from database tables - just mark as initialized
  m_initialized.store(true);

  LogVSQL(INFORMATION_LEVEL, "VictionaryClient initialized for testing");
  return false;
}

void VictionaryClient::destroy() {
  if (!m_initialized.load()) {
    return;
  }

  // Clear all tables
  clear_all_tables();

  // Destroy the lock
  mysql_rwlock_destroy(&m_lock);

  m_initialized.store(false);
  LogVSQL(INFORMATION_LEVEL, "VictionaryClient destroyed");
}

std::vector<const ColumnEntry *> VictionaryClient::GetCustomColumnsForTable(
    const std::string &db_name, const std::string &table_name) const {
  if (!m_initialized.load()) {
    return {};
  }

  // Use get_prefix_committed to find all columns for this table
  return m_columns.get_prefix_committed(ColumnKeyPrefix(db_name, table_name));
}

std::vector<const SpParamEntry *> VictionaryClient::GetCustomSpParamsForSP(
    const std::string &db_name, const std::string &sp_name) const {
  if (!m_initialized.load()) {
    return {};
  }

  return m_sp_params.get_prefix_committed(SpParamKeyPrefix(db_name, sp_name));
}

void VictionaryClient::commit_all_tables(THD *thd) {
  if (!m_initialized.load() || !thd) return;

  auto guard = get_write_lock();

  m_properties.commit(thd);
  m_columns.commit(thd);
  m_sp_params.commit(thd);
  m_extensions.commit(thd);
  m_type_descriptors.commit(thd);
  m_extension_descriptors.commit(thd);
  m_type_contexts.commit(thd);
  m_funcs.commit(thd);
  m_index_contexts.commit(thd);
  m_custom_indexes.commit(thd);
  m_custom_index_columns.commit(thd);
  m_index_type_descriptors.commit(thd);
  m_index_profile_descriptors.commit(thd);
}

void VictionaryClient::rollback_all_tables(THD *thd) {
  if (!m_initialized.load() || !thd) return;

  auto guard = get_write_lock();

  m_properties.rollback(thd);
  m_columns.rollback(thd);
  m_sp_params.rollback(thd);
  m_extensions.rollback(thd);
  m_type_descriptors.rollback(thd);
  m_extension_descriptors.rollback(thd);
  m_type_contexts.rollback(thd);
  m_funcs.rollback(thd);
  m_index_contexts.rollback(thd);
  m_custom_indexes.rollback(thd);
  m_custom_index_columns.rollback(thd);
  m_index_type_descriptors.rollback(thd);
  m_index_profile_descriptors.rollback(thd);
}

bool VictionaryClient::write_all_uncommitted_entries(THD *thd) {
  if (!thd) {
    return false;  // No THD, nothing to write
  }

  if (!m_initialized.load()) {
    // If this is called before init(), then there cannot be any uncommitted
    // entries, as the lock is not initialized. This can happen very early on in
    // startup/initialization of the database.
    return false;
  }

  // We do not replicate VillageSQL system table changes. Disable binlogging
  // of the inserts/updates/deletes to system tables, so that they are not
  // replicated in row-based mode. This follows the same pattern as INSTALL
  // PLUGIN (see sql/sql_plugin.cc). Each replica will manage its own metadata
  // based on the DDL statements that are replicated.
  const Disable_binlog_guard binlog_guard(thd);

  // Acquire read lock to access uncommitted entries
  auto guard = get_read_lock();

  bool error = false;

  // Write uncommitted entries for each system table
  if (m_properties.write_uncommitted_to_table(
          thd, SchemaManager::VILLAGESQL_SCHEMA_NAME,
          SchemaManager::PROPERTIES_TABLE_NAME)) {
    error = true;
  }
  if (m_columns.write_uncommitted_to_table(
          thd, SchemaManager::VILLAGESQL_SCHEMA_NAME,
          SchemaManager::COLUMNS_TABLE_NAME)) {
    error = true;
  }
  if (m_sp_params.write_uncommitted_to_table(
          thd, SchemaManager::VILLAGESQL_SCHEMA_NAME,
          SchemaManager::SP_PARAMS_TABLE_NAME)) {
    error = true;
  }
  if (m_extensions.write_uncommitted_to_table(
          thd, SchemaManager::VILLAGESQL_SCHEMA_NAME,
          SchemaManager::EXTENSIONS_TABLE_NAME)) {
    error = true;
  }
  // custom_indexes must be written before custom_index_columns so the parent
  // row exists when the per-column rows reference it.
  if (m_custom_indexes.write_uncommitted_to_table(
          thd, SchemaManager::VILLAGESQL_SCHEMA_NAME,
          SchemaManager::INDEXES_TABLE_NAME)) {
    error = true;
  }
  if (m_custom_index_columns.write_uncommitted_to_table(
          thd, SchemaManager::VILLAGESQL_SCHEMA_NAME,
          SchemaManager::INDEX_COLUMNS_TABLE_NAME)) {
    error = true;
  }

  // Errors are already set by write_uncommitted_to_table and TableTraits
  // functions
  return error;
}

bool VictionaryClient::reload_all_tables(THD *thd) {
  if (should_assert_if_false(!m_initialized.load() && m_initializing.load())) {
    LogVSQL(
        ERROR_LEVEL,
        "VictionaryClient must be initializing when calling reload_all_tables");
    return true;
  }

  LogVSQL(INFORMATION_LEVEL, "Reloading all system table metadata");

  // Acquire write lock for reloading
  auto guard = get_write_lock();

  bool error = false;

  // Reload each system table
  if (m_properties.reload_from_table(thd, SchemaManager::VILLAGESQL_SCHEMA_NAME,
                                     SchemaManager::PROPERTIES_TABLE_NAME)) {
    error = true;
  }
  if (m_extensions.reload_from_table(thd, SchemaManager::VILLAGESQL_SCHEMA_NAME,
                                     SchemaManager::EXTENSIONS_TABLE_NAME)) {
    error = true;
  }
  if (m_columns.reload_from_table(thd, SchemaManager::VILLAGESQL_SCHEMA_NAME,
                                  SchemaManager::COLUMNS_TABLE_NAME)) {
    error = true;
  }
  if (m_sp_params.reload_from_table(thd, SchemaManager::VILLAGESQL_SCHEMA_NAME,
                                    SchemaManager::SP_PARAMS_TABLE_NAME)) {
    error = true;
  }
  if (m_custom_indexes.reload_from_table(thd,
                                         SchemaManager::VILLAGESQL_SCHEMA_NAME,
                                         SchemaManager::INDEXES_TABLE_NAME)) {
    error = true;
  }
  // Seed the index_id counter from the maximum existing id so that
  // allocate_index_id() always produces a value greater than any persisted id.
  init_index_id_counter();
  if (m_custom_index_columns.reload_from_table(
          thd, SchemaManager::VILLAGESQL_SCHEMA_NAME,
          SchemaManager::INDEX_COLUMNS_TABLE_NAME)) {
    error = true;
  }

  return error;
}

std::vector<const IndexEntry *> VictionaryClient::GetCustomIndexesForTable(
    const std::string &db_name, const std::string &table_name) const {
  if (!m_initialized.load()) return {};
  return m_custom_indexes.get_prefix_committed(
      IndexKeyPrefix(db_name, table_name));
}

std::vector<const IndexColumnEntry *> VictionaryClient::GetColumnsForIndex(
    uint64_t index_id) const {
  if (!m_initialized.load()) return {};
  return m_custom_index_columns.get_prefix_committed(
      IndexColumnKeyPrefix(index_id));
}

void VictionaryClient::init_index_id_counter() {
  // Must be called while holding write lock (reload_all_tables holds it).
  uint64_t max_id = 0;
  for (const IndexEntry *entry : m_custom_indexes.get_all_committed()) {
    if (entry->index_id > max_id) max_id = entry->index_id;
  }
  m_next_index_id.store(max_id);
  LogVSQL(INFORMATION_LEVEL,
          "Index ID counter initialized to %" PRIu64 " (max existing id)",
          max_id);
}

void VictionaryClient::clear_all_tables() {
  if (!m_initialized.load()) {
    return;
  }

  auto guard = get_write_lock();

  m_properties.clear();
  m_columns.clear();
  m_sp_params.clear();
  m_extensions.clear();
  m_type_descriptors.clear();
  m_extension_descriptors.clear();
  m_type_contexts.clear();
  m_funcs.clear();
  m_index_contexts.clear();
  m_custom_indexes.clear();
  m_custom_index_columns.clear();
  m_index_type_descriptors.clear();
  m_index_profile_descriptors.clear();

  LogVSQL(INFORMATION_LEVEL, "Cleared all system table metadata");
}

}  // namespace villagesql
