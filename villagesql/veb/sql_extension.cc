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

#include "villagesql/veb/sql_extension.h"

#include <cctype>
#include <string>
#include <tuple>

#include "my_sys.h"
#include "mysql/components/services/registry.h"
#include "mysql/service_security_context.h"
#include "mysql/service_srv_session.h"
#include "mysql_com.h"
#include "mysqld_error.h"
#include "scope_guard.h"
#include "sql/dd/cache/dictionary_client.h"
#include "sql/debug_sync.h"
#include "sql/iterators/row_iterator.h"
#include "sql/lock.h"
#include "sql/mdl.h"
#include "sql/protocol_callback.h"
#include "sql/sql_backup_lock.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#ifndef NDEBUG
#include "sql/item_func.h"
#endif
#include "sql/sql_executor.h"
#include "sql/sql_plugin.h"
#include "sql/sql_udf.h"
#include "sql/srv_session.h"
#include "sql/table.h"
#include "sql/thd_raii.h"
#include "sql_string.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/descriptor/extension_descriptor.h"
#include "villagesql/schema/schema_manager.h"
#include "villagesql/schema/systable/extensions.h"
#include "villagesql/schema/victionary_client.h"
#include "villagesql/services/capability_registry.h"
#include "villagesql/sql/metadata_modifier.h"
#include "villagesql/veb/extension_uninstall_checks.h"
#include "villagesql/veb/register.h"
#include "villagesql/veb/validate.h"
#include "villagesql/veb/veb_file.h"

// Global variables for VEB directory configuration
char *opt_veb_dir_ptr;
char opt_veb_dir[FN_REFLEN];

// EXTENSION MDL locks (defined in sql/mdl.h):
// - An X (exclusive) lock is acquired when installing or uninstalling
//   an extension. This is the lock taken in the install/uninstall path
//   immediately after acquiring the global read lock and backup lock.
// - To protect against concurrent uninstall, DDL operations that add or
//   remove columns of extension-defined types acquire an S (shared) MDL
//   lock on the extension (see Metadata_modifier::lock_extensions_shared).
// - DDL acquires the S lock on extensions after acquiring the table and
//   other required object locks. This ensures the table is not being altered
//   while determining the set of extensions that must be locked.
// - This locking order is deadlock-safe, provided the uninstall command does
//   not itself execute any DDL on dependent objects.
bool Sql_cmd_install_extension::execute(THD *thd) {
  // We do not replicate the INSTALL EXTENSION statement
  const Disable_binlog_guard binlog_guard(thd);

  std::string extension_name(m_name.str, m_name.length);

  // INSTALL EXTENSION is DDL-like (modifies system tables), so we follow the
  // INSTALL PLUGIN pattern: disable autocommit to prevent premature commits
  // when data dictionary tables close (see CF_NEEDS_AUTOCOMMIT_OFF in
  // sql_parse.h).
  const Disable_autocommit_guard autocommit_guard(thd);
  const dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());

  LogVSQL(INFORMATION_LEVEL, "Installing extension: '%s'",
          extension_name.c_str());

  // Validate extension name first (before file operations)
  if (extension_name.empty()) {
    villagesql_error("Extension name cannot be empty", MYF(0));
    return end_transaction(thd, true);
  }

  if (extension_name.length() > 64) {
    villagesql_error(
        "Extension name '%s' exceeds maximum length of 64 characters", MYF(0),
        extension_name.c_str());
    return end_transaction(thd, true);
  }

  if (!std::isalpha(static_cast<unsigned char>(extension_name[0]))) {
    villagesql_error("Extension name '%s' must start with a letter", MYF(0),
                     extension_name.c_str());
    return end_transaction(thd, true);
  }

  char last_char = extension_name[extension_name.length() - 1];
  if (!std::isalnum(static_cast<unsigned char>(last_char))) {
    villagesql_error("Extension name '%s' must end with a letter or digit",
                     MYF(0), extension_name.c_str());
    return end_transaction(thd, true);
  }

  for (char c : extension_name) {
    if (!std::isalnum(static_cast<unsigned char>(c)) && c != '_' && c != '-') {
      villagesql_error(
          "Extension name '%s' contains invalid character '%c' "
          "(only letters, digits, underscore, and hyphen allowed)",
          MYF(0), extension_name.c_str(), c);
      return end_transaction(thd, true);
    }
  }

  // Acquire global shared read lock to check and prevent installation in
  // "read only mode". Acquire shared backup lock to synchronize with final
  // phase of backup operation.
  if (acquire_shared_global_read_lock(thd, thd->variables.lock_wait_timeout) ||
      acquire_shared_backup_lock(thd, thd->variables.lock_wait_timeout))
    return true;

  // Acquire X MDL lock with statement duration on the normalized extension
  // name to prevent concurrent install/uninstall operations on the same
  // extension.
  if (villagesql::Metadata_modifier::lock_extension_exclusive(
          thd, extension_name, MDL_STATEMENT)) {
    return true;
  }

  // Load manifest from VEB file to get version
  std::string version;
  if (villagesql::veb::load_veb_manifest(extension_name, version)) {
    // Error already reported by load_veb_manifest
    return end_transaction(thd, true);
  }

  auto &victionary = villagesql::VictionaryClient::instance();

  // Early check: fail fast if extension already exists (from in-memory cache).
  // This avoids executing install.sql for an extension that will be rejected.
  // We do a final authoritative check later under table lock to handle races.
  // NOTE: We must release the read lock BEFORE calling end_transaction, because
  // end_transaction -> trans_rollback -> rollback_all_tables needs write lock.
  {
    auto read_lock = victionary.get_read_lock();
    const auto *existing = victionary.extensions().get_committed(
        villagesql::ExtensionKey(extension_name));
    if (existing) {
      villagesql_error("Extension '%s' is already installed", MYF(0),
                       extension_name.c_str());
    }
  }
  if (thd->is_error()) {
    return end_transaction(thd, true);
  }

  // Expand VEB archive to directory
  std::string expanded_path;
  std::string sha256_hash;
  if (villagesql::veb::expand_veb_to_directory(extension_name, expanded_path,
                                               sha256_hash)) {
    // Error already reported by expand_veb_to_directory
    return end_transaction(thd, true);
  }

  std::string so_path =
      villagesql::veb::get_extension_so_path(extension_name, sha256_hash);
  if (so_path.empty()) {
    villagesql_error("Failed to construct .so path for extension '%s'", MYF(0),
                     extension_name.c_str());
    return end_transaction(thd, true);
  }

  villagesql::veb::ExtensionRegistration registration;
  vef_protocol_t server_protocol =
      static_cast<vef_protocol_t>(villagesql::veb::vef_server_protocol_version);
#ifndef NDEBUG
  {
    auto it = thd->user_vars.find("vef_debug_protocol_override");
    if (it != thd->user_vars.end()) {
      bool null_value = false;
      const longlong val = it->second->val_int(&null_value);
      if (!null_value && val > 0)
        server_protocol = static_cast<vef_protocol_t>(val);
    }
  }
#endif
  std::string load_error;
  if (villagesql::veb::load_vef_extension(
          {.extension_name = extension_name,
           .reason = villagesql::services::LoadReason::kInstall,
           .thd = thd},
          so_path, server_protocol, registration, load_error)) {
    LogVSQL(ERROR_LEVEL, "Failed to load VEF extension '%s': %s",
            extension_name.c_str(), load_error.c_str());
    villagesql_error("Failed to load VEF extension '%s': %s", MYF(0),
                     extension_name.c_str(), load_error.c_str());
    return end_transaction(thd, true);
  }

  std::string reg_error;
  std::optional<villagesql::veb::ValidatedRegistration> validated =
      villagesql::veb::validate_extension_registration(
          registration, extension_name, version, reg_error);
  if (!validated) {
    villagesql_error("Failed to install extension '%s': %s", MYF(0),
                     extension_name.c_str(), reg_error.c_str());
    return end_transaction(thd, true);
  }


  bool mark_success = true;
  {
    auto write_lock = victionary.get_write_lock();

    if (villagesql::veb::register_validated_extension(
            *thd, std::move(*validated), reg_error)) {
      villagesql_error("Failed to install extension '%s': %s", MYF(0),
                       extension_name.c_str(), reg_error.c_str());
      // Rollback should be done after releasing the victionary lock.
      mark_success = false;

    } else if (victionary.extension_descriptors().MarkForInsertion(
                   *thd, villagesql::ExtensionDescriptor(
                             villagesql::ExtensionDescriptorKey(extension_name,
                                                                version),
                             std::move(registration)))) {
      villagesql_error("Failed to register descriptor for extension '%s'",
                       MYF(0), extension_name.c_str());
      mark_success = false;
    }
  }

  if (!mark_success) {
    return end_transaction(thd, true);
  }

  // Open villagesql.extensions table for writing.
  Table_ref ext_table(villagesql::SchemaManager::VILLAGESQL_SCHEMA_NAME,
                      villagesql::SchemaManager::EXTENSIONS_TABLE_NAME,
                      TL_WRITE, MDL_SHARED_WRITE);
  if (open_and_lock_tables(thd, &ext_table, MYSQL_LOCK_IGNORE_TIMEOUT)) {
    villagesql_error("Cannot open extensions table", MYF(0));
    return end_transaction(thd, true);
  }

  // Check if extension already exists and mark for insertion while holding lock
  mark_success = false;
  {
    auto write_lock = victionary.get_write_lock();

    const auto *existing = victionary.extensions().get_committed(
        villagesql::ExtensionKey(extension_name));
    if (existing) {
      villagesql_error("Extension '%s' is already installed", MYF(0),
                       extension_name.c_str());
    } else {
      // Create extension entry and mark for insertion - version is used below
      // and needs to be copied as a result.
      villagesql::ExtensionEntry new_ext(
          villagesql::ExtensionKey(extension_name), version,
          std::move(sha256_hash));
      if (victionary.extensions().MarkForInsertion(*thd, std::move(new_ext))) {
        villagesql_error("Failed to register extension '%s'", MYF(0),
                         extension_name.c_str());
      } else {
        mark_success = true;
      }
    }
  }

  if (!mark_success) {
    return end_transaction(thd, true);
  }

  // Write to table
  if (victionary.write_all_uncommitted_entries(thd)) {
    villagesql_error("Failed to write extension '%s' to table", MYF(0),
                     extension_name.c_str());
    return end_transaction(thd, true);
  }

  LogVSQL(INFORMATION_LEVEL,
          "Extension '%s' (version %s) installed successfully",
          extension_name.c_str(), version.c_str());

  my_ok(thd);
  return end_transaction(thd, false);
}

namespace villagesql {
namespace {

bool check_for_columns_of_extension(
    const villagesql::ExtensionEntry &ext_entry,
    const std::vector<const ColumnEntry *> &all_columns) {
  std::string error_message;
  int count = 0;
  const ColumnEntry *first_col = nullptr;

  for (const auto *col : all_columns) {
    if (col->extension_name == ext_entry.extension_name() &&
        col->extension_version == ext_entry.extension_version) {
      if (count == 0) {
        first_col = col;
      }
      count++;
    }
  }

  if (first_col != nullptr) {
    villagesql_error(
        "Cannot drop extension `%s` as %d column(s) depend on it, "
        "e.g. %s.%s.%s has type %s",
        MYF(0), ext_entry.extension_name().c_str(), count,
        first_col->db_name().c_str(), first_col->table_name().c_str(),
        first_col->column_name().c_str(), first_col->type_name.c_str());
    return true;
  }

  return false;
}

bool check_for_sp_params_of_extension(
    const villagesql::ExtensionEntry &ext_entry,
    const std::vector<const SpParamEntry *> &all_sp_params) {
  const SpParamEntry *first = nullptr;
  int count = 0;

  for (const auto *entry : all_sp_params) {
    if (entry->extension_name == ext_entry.extension_name() &&
        entry->extension_version == ext_entry.extension_version) {
      if (count == 0) first = entry;
      count++;
    }
  }

  if (first != nullptr) {
    villagesql_error(
        "Cannot uninstall extension '%s': stored procedure %s.%s uses "
        "custom type %s",
        MYF(0), ext_entry.extension_name().c_str(), first->db_name().c_str(),
        first->sp_name().c_str(), first->type_name.c_str());
    return true;
  }

  return false;
}

// If the transaction commits, then `to_unregister` is used to unregister the
// .so file.
//
// If `expected_version` is non-empty, the installed extension's version must
// match exactly or uninstall is rejected. Callers pass an empty string when no
// VERSION clause was specified, falling back to whichever version is currently
// installed.
bool remove_extension_from_victionary(
    THD *thd, VictionaryClient &victionary, const std::string &extension_name,
    const std::string &expected_version,
    std::optional<veb::ExtensionRegistration> &to_unregister) {
  auto write_lock = victionary.get_write_lock();

  const auto *ext_entry = victionary.extensions().get_committed(
      villagesql::ExtensionKey(extension_name));
  if (ext_entry == nullptr) {
    villagesql_error("Extension '%s' is not installed", MYF(0),
                     extension_name.c_str());

    return true;
  }

  if (!expected_version.empty() &&
      ext_entry->extension_version != expected_version) {
    villagesql_error(
        "Cannot uninstall extension '%s': installed version is '%s' but "
        "VERSION '%s' was specified",
        MYF(0), extension_name.c_str(), ext_entry->extension_version.c_str(),
        expected_version.c_str());
    return true;
  }

  // Delete all custom types for this extension (RESTRICT behavior - fails
  // if any type has dependent columns or stored procedures)
  const auto &all_columns = victionary.columns().get_all_committed();
  if (check_for_columns_of_extension(*ext_entry, all_columns)) {
    return true;
  }

  const auto &all_sp_params = victionary.sp_params().get_all_committed();
  if (check_for_sp_params_of_extension(*ext_entry, all_sp_params)) {
    return true;
  }

  const auto &all_indexes = victionary.custom_indexes().get_all_committed();
  if (villagesql::check_for_indexes_of_extension(*ext_entry, all_indexes)) {
    return true;
  }

  // Check for active references to VDFs, TypeContexts, and TypeDescriptors.
  // A use_count > 1 means something other than Victionary holds a reference
  // (e.g., an executing query).
  const auto &all_funcs = victionary.funcs().get_all_committed();
  for (const auto *func : all_funcs) {
    if (func->extension_name() == extension_name &&
        func->extension_version() == ext_entry->extension_version) {
      long use_count = victionary.funcs().get_use_count(func->key().str());
      if (use_count > 1) {
        villagesql_error(
            "Cannot uninstall extension '%s': VDF '%s' is currently in "
            "use",
            MYF(0), extension_name.c_str(), func->function_name().c_str());
        return true;
      }
    }
  }

  const auto &all_type_contexts =
      victionary.type_contexts().get_all_committed();
  for (const auto *type_context : all_type_contexts) {
    if (type_context->extension_name() == extension_name &&
        type_context->extension_version() == ext_entry->extension_version) {
      long use_count =
          victionary.type_contexts().get_use_count(type_context->key().str());
      if (use_count > 1) {
        villagesql_error(
            "Cannot uninstall extension '%s': type '%s' is currently in use",
            MYF(0), extension_name.c_str(), type_context->type_name().c_str());
        return true;
      }
    }
  }

  const auto &all_type_descs =
      victionary.type_descriptors().get_all_committed();
  for (const auto *type_desc : all_type_descs) {
    if (type_desc->extension_name() == extension_name &&
        type_desc->extension_version() == ext_entry->extension_version) {
      long use_count =
          victionary.type_descriptors().get_use_count(type_desc->key().str());
      if (use_count > 1) {
        villagesql_error(
            "Cannot uninstall extension '%s': type '%s' is currently in use",
            MYF(0), extension_name.c_str(), type_desc->type_name().c_str());
        return true;
      }
    }
  }

  // Delete TypeContexts for this extension (we do it before TypeDescriptors
  // since TypeContext holds a raw pointer to TypeDescriptor, but under the
  // lock, it doesn't really matter)
  for (const auto *type_context : all_type_contexts) {
    if (type_context->extension_name() == extension_name &&
        type_context->extension_version() == ext_entry->extension_version) {
      victionary.type_contexts().MarkForDeletion(*thd, type_context->key());
    }
  }

  // Delete TypeDescriptors for this extension
  for (const auto *type_desc : all_type_descs) {
    if (type_desc->extension_name() == extension_name &&
        type_desc->extension_version() == ext_entry->extension_version) {
      victionary.type_descriptors().MarkForDeletion(*thd, type_desc->key());
    }
  }

  // Delete VDFs for this extension
  for (const auto *func : all_funcs) {
    if (func->extension_name() == extension_name &&
        func->extension_version() == ext_entry->extension_version) {
      victionary.funcs().MarkForDeletion(*thd, func->key());
    }
  }

  victionary.extensions().MarkForDeletion(*thd, ext_entry->key());
  const auto *ext_desc = victionary.extension_descriptors().get_committed(
      ExtensionDescriptorKey(extension_name, ext_entry->extension_version));
  if (ext_desc != nullptr) {
    to_unregister.emplace(ext_desc->registration());
    victionary.extension_descriptors().MarkForDeletion(*thd, ext_desc->key());
  }

  return false;
}

}  // namespace
}  // namespace villagesql

bool Sql_cmd_uninstall_extension::execute(THD *thd) {
  // We do not replicate the UNINSTALL EXTENSION statement
  const Disable_binlog_guard binlog_guard(thd);

  // Acquire global shared read lock to check and prevent installation in
  // "read only mode". Acquire shared backup lock to synchronize with final
  // phase of backup operation.
  if (acquire_shared_global_read_lock(thd, thd->variables.lock_wait_timeout) ||
      acquire_shared_backup_lock(thd, thd->variables.lock_wait_timeout))
    return true;

  std::string extension_name(m_name.str, m_name.length);
  std::string expected_version =
      m_version.str ? to_string(m_version) : std::string();

  // Acquire X MDL lock with statement duration on the normalized extension
  // name to synchronize with following operations. All such operations must
  // acquire IX lock on the extension name.
  // 1. Concurrent install/uninstall operations with same extension name.
  // 2. Concurrent DDL creating columns with types defined by the extension.
  // 3. Concurrent statement running custom functions defined by the extension.
  if (villagesql::Metadata_modifier::lock_extension_exclusive(
          thd, extension_name, MDL_STATEMENT)) {
    return true;
  }
  DEBUG_SYNC_C("uninstall_after_extension_lock");

  // Start transaction
  const Disable_autocommit_guard autocommit_guard(thd);
  const dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());

  LogVSQL(INFORMATION_LEVEL, "Uninstalling extension: '%s'",
          extension_name.c_str());

  // Open all required tables in one call:
  // - extensions (WRITE) - to delete extension record
  Table_ref extensions_table(villagesql::SchemaManager::VILLAGESQL_SCHEMA_NAME,
                             villagesql::SchemaManager::EXTENSIONS_TABLE_NAME,
                             TL_WRITE, MDL_SHARED_WRITE);

  // Set the links for open_and_lock_tables
  extensions_table.next_global = extensions_table.next_local = nullptr;

  if (open_and_lock_tables(thd, &extensions_table, MYSQL_LOCK_IGNORE_TIMEOUT)) {
    villagesql_error("Cannot open extension tables", MYF(0));
    return end_transaction(thd, true);
  }

  // Get victionary client
  auto &victionary = villagesql::VictionaryClient::instance();

  // State tracking for three-phase operation:
  // Phase 1 (under lock): lookups and mark operations
  // Phase 2 (lock released): write to table and commit
  // Phase 3 (lock still released): commit

  std::optional<villagesql::veb::ExtensionRegistration> to_unregister;
  // Phase 1: Do all lookups and mark operations while holding lock
  if (villagesql::remove_extension_from_victionary(
          thd, victionary, extension_name, expected_version, to_unregister)) {
    return end_transaction(thd, true);
  }

  // Phase 2: write tables
  if (victionary.write_all_uncommitted_entries(thd)) {
    villagesql_error("Failed to delete extension '%s'", MYF(0),
                     extension_name.c_str());
    return end_transaction(thd, true);
  }

  if (to_unregister.has_value()) {
    villagesql::veb::unload_vef_extension(
        {.reason = villagesql::services::UnloadReason::kUninstall, .thd = thd},
        *to_unregister);
  }

  LogVSQL(INFORMATION_LEVEL, "Extension '%s' uninstalled successfully",
          extension_name.c_str());
  my_ok(thd);

  // Phase 3: perform the commit
  return end_transaction(thd, false);
}
