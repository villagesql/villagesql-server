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

// VillageSQL in-memory cache for system tables and extension objects
// (Victionary).
// For adding new system tables, see villagesql/schema/ADDING_SYSTEM_TABLES.md

#ifndef VILLAGESQL_SCHEMA_VICTIONARY_CLIENT_H_
#define VILLAGESQL_SCHEMA_VICTIONARY_CLIENT_H_

#include <atomic>
#include <cassert>
#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "my_alloc.h"
#include "my_base.h"
#include "mysql/psi/mysql_rwlock.h"
#include "scope_guard.h"  // create_scope_guard
#include "sql/auth/auth_common.h"
#include "sql/auth/sql_security_ctx.h"
#include "sql/handler.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/sql_list.h"
#include "sql/table.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/descriptor/extension_descriptor.h"
#include "villagesql/schema/descriptor/func_descriptor.h"
#include "villagesql/schema/descriptor/type_context.h"
#include "villagesql/schema/descriptor/type_descriptor.h"
#include "villagesql/schema/systable/custom_columns.h"
#include "villagesql/schema/systable/custom_sp_params.h"
#include "villagesql/schema/systable/extensions.h"
#include "villagesql/schema/systable/properties.h"
#include "villagesql/schema/util.h"

// Forward declaration for friend class
namespace villagesql_unittest {
class VictionaryClientTest;
}

namespace villagesql {

// Forward declaration of TableTraits (specialized per entry type)
template <typename EntryType>
struct TableTraits;

// Operation types for pending changes to system tables or extension objects
enum class OperationType {
  INSERT,  // Insert a new row
  UPDATE,  // Update an existing row
  DELETE   // Delete an existing row
};

// Persistence mode for SystemTableMap
// PERSISTENT: Default behavior - loads from and writes to backing system table
// MEMORY_ONLY: Pure in-memory storage - no table I/O, just in-memory objects
enum class PersistenceMode { PERSISTENT, MEMORY_ONLY };

// Wrapper for a pending operation on a system table entry
// This is an intrusive list node for use with SQL_I_List
//
// Uses shared_ptr<EntryType> to:
// - Avoid requiring copy constructors on EntryType
// - Allow cheap commit (just move the shared_ptr to m_committed)
// - Unify DELETE handling (nullptr entry means delete by key)
template <typename EntryType>
struct PendingOperation {
  using key_type = typename EntryType::key_type;

  OperationType op_type;
  std::shared_ptr<EntryType> entry;  // nullptr for DELETE-by-key

  // For UPDATE operations: the old key if the key changes (e.g., rename)
  // For DELETE operations: the key to delete (when entry is nullptr)
  // TODO(villagesql): maybe adding a DELETE+INSERT would be simpler?
  key_type key;

  // Intrusive list pointer for SQL_I_List
  PendingOperation *next;

  PendingOperation() = delete;

  // Constructor for INSERT (no key needed)
  PendingOperation(OperationType type, std::shared_ptr<EntryType> e)
      : op_type(type), entry(std::move(e)), key(), next(nullptr) {}

  // Constructor for UPDATE (with old key) or DELETE (with key to delete)
  PendingOperation(OperationType type, std::shared_ptr<EntryType> e, key_type k)
      : op_type(type), entry(std::move(e)), key(std::move(k)), next(nullptr) {}
};

// Generic map for any VillageSQL system object or table row. Each collection
// (SystemTableMap) internally correspends to a systable (if in PERSISTENT mode)
// or a keyed collection of similarly-typed extension objects (if MEMORY_ONLY
// mode). Maps the key of the system object/table row to an entry representing
// the contents of the row or the object itself. Provides thread-safe storage of
// committed and uncommitted entries (uncommitted entries are internally keyed
// also by THD).
// Note: All methods assume caller holds VictionaryClient's lock
//
// Template parameters:
//   EntryType - The entry type (e.g., ColumnEntry)
//   Mode - PersistenceMode::PERSISTENT (default) for normal table I/O,
//          PersistenceMode::MEMORY_ONLY for pure in-memory operation
template <typename EntryType,
          PersistenceMode Mode = PersistenceMode::PERSISTENT>
class SystemTableMap {
 public:
  // Expose the persistence mode for compile-time checks
  static constexpr PersistenceMode persistence_mode = Mode;

  // Constructor takes pointer to parent's lock for assertions
  explicit SystemTableMap(mysql_rwlock_t *lock) : m_parent_lock(lock) {}
  ~SystemTableMap() = default;

  // Disable copy/move
  SystemTableMap(const SystemTableMap &) = delete;
  SystemTableMap &operator=(const SystemTableMap &) = delete;

  // ===== Core lookup operations =====
  // Note: Caller must hold VictionaryClient lock
  // See acquire_read_lock(), acquire_write_lock(), and release_lock() in
  // VictionaryClient, below.

  // Get entry considering uncommitted operations for this THD.
  // Returns the most recent version of the entry (uncommitted if exists, else
  // committed). If the entry is marked for deletion, returns nullptr
  // Requires >=read lock
  const EntryType *get(THD *thd, const std::string &key_str) const {
    assert_read_or_write_lock_held();
    // Check if this is in the context of a THD and has uncommitted operations
    if (thd) {
      auto thd_it = m_uncommitted.find(thd);
      if (thd_it != m_uncommitted.end()) {
        // Search through the list of pending operations recording the most
        // recent operation on this key
        const PendingOperation<EntryType> *most_recent_op = nullptr;
        for (const PendingOperation<EntryType> *op = thd_it->second.first; op;
             op = op->next) {
          // Get the key for this operation
          // For ops with entry, use entry's key; otherwise use stored key
          const std::string &op_key =
              op->entry ? op->entry->key().str() : op->key.str();
          if (op_key == key_str || (op->op_type == OperationType::UPDATE &&
                                    op->key.str() == key_str)) {
            most_recent_op = op;
          }
        }

        // If we found an operation on this key, use it
        if (most_recent_op) {
          if (most_recent_op->op_type == OperationType::DELETE) {
            return nullptr;  // Entry is marked for deletion
          }
          assert(most_recent_op->entry);
          return most_recent_op->entry.get();
        }
      }
    }

    // No uncommitted operations for this key, fall back to committed
    return get_committed(key_str);
  }

  // Ignores any local updates to the system table
  // Requires >=read lock
  const EntryType *get_committed(
      const typename EntryType::key_type &key) const {
    return get_committed(key.str());
  }

  const EntryType *get_committed(const std::string &key_str) const {
    assert_read_or_write_lock_held();
    auto it = m_committed.find(key_str);
    if (it == m_committed.end()) {
      m_misses++;
      return nullptr;
    }
    m_hits++;
    return it->second.get();
  }

  // ===== Entry acquisition with reference counting =====

  // Acquire an entry with reference counting tied to a MEM_ROOT's lifecycle.
  // Returns a pointer that remains valid until cleanup_scope is cleared.
  // This allows safe use of the pointer beyond the VictionaryClient lock scope.
  //
  // The returned pointer is kept alive by incrementing the entry's reference
  // count (via shared_ptr copy). When cleanup_scope is cleared, a cleanup
  // callback releases the reference.
  //
  // REQUIRES: Caller must hold at least read lock.
  // Returns: Pointer to the entry, or nullptr if not found or on error.
  const EntryType *acquire(const typename EntryType::key_type &key,
                           MEM_ROOT &cleanup_scope) {
    return acquire(key.str(), cleanup_scope);
  }

  const EntryType *acquire(const std::string &key_str,
                           MEM_ROOT &cleanup_scope) {
    assert_read_or_write_lock_held();

    auto it = m_committed.find(key_str);
    if (it == m_committed.end()) {
      return nullptr;
    }

    // Debug flag to skip reference counting (for testing scenarios where
    // we want to simulate missing refcount protection)
    DBUG_EXECUTE_IF("skip_victionary_refcount", return it->second.get(););

    // Register shared_ptr to release the ref when the MEM_ROOT clears
    if (cleanup_scope.register_cleanup(it->second)) {
      return nullptr;
    }

    return it->second.get();
  }

  // Acquire a shared_ptr to an entry directly (caller manages lifetime).
  // Use this when you need to hold a reference longer than a single query,
  // such as for session-scoped user variables.
  //
  // REQUIRES: Caller must hold at least read lock.
  // Returns: shared_ptr to the entry, or empty shared_ptr if not found.
  std::shared_ptr<const EntryType> acquire_client_managed(
      const typename EntryType::key_type &key) {
    assert_read_or_write_lock_held();

    auto it = m_committed.find(key.str());
    if (it == m_committed.end()) {
      return std::shared_ptr<const EntryType>();
    }

    return it->second;
  }

  // Get the reference count for an entry (for testing/debugging).
  // Returns 0 if the entry doesn't exist.
  // REQUIRES: Caller must hold at least read lock.
  long get_use_count(const std::string &key_str) const {
    assert_read_or_write_lock_held();
    auto it = m_committed.find(key_str);
    if (it == m_committed.end()) {
      return 0;
    }
    return it->second.use_count();
  }

  // Acquire an entry, creating it if it doesn't exist.
  // If the entry exists, acquires it with reference counting (like acquire()).
  // If the entry doesn't exist, creates it using TableTraits::create() with the
  // provided arguments, stores it, and then acquires it.
  //
  // The create_args are forwarded to TableTraits<EntryType>::create(key, ...).
  // For example, for TypeContext:
  //   acquire_or_create(key, cleanup_scope, type_entry, type_descriptor)
  //
  // REQUIRES: Caller must hold write lock (needed to potentially insert).
  // Returns: Pointer to the entry, or nullptr on error.
  template <typename... Args>
  const EntryType *acquire_or_create(const typename EntryType::key_type &key,
                                     MEM_ROOT &cleanup_scope,
                                     Args &&...create_args) {
    assert_write_lock_held();

    const std::string key_str = key.str();
    auto it = m_committed.find(key_str);

    if (it == m_committed.end()) {
      // Entry doesn't exist - create it
      auto new_entry = TableTraits<EntryType>::create(
          key, std::forward<Args>(create_args)...);
      if (!new_entry) {
        return nullptr;
      }

      // Store in committed map
      it = m_committed.emplace(key_str, std::move(new_entry)).first;
    }

    // Debug flag to skip reference counting (for testing scenarios where
    // we want to simulate missing refcount protection)
    DBUG_EXECUTE_IF("skip_victionary_refcount", return it->second.get(););

    // Register shared_ptr to release the ref when the MEM_ROOT clears
    if (cleanup_scope.register_cleanup(it->second)) {
      return nullptr;
    }

    return it->second.get();
  }

  // Get or create an entry and return a client-managed shared_ptr to it.
  // Creates the entry if it does not exist. Does not register any mem_root
  // cleanup — the caller is responsible for releasing the shared_ptr.
  //
  // REQUIRES: Caller must hold write lock (needed to potentially insert).
  // Returns: shared_ptr to the entry, or empty shared_ptr on error.
  template <typename... Args>
  std::shared_ptr<const EntryType> get_or_create_client_managed(
      const typename EntryType::key_type &key, Args &&...create_args) {
    assert_write_lock_held();

    const std::string key_str = key.str();
    auto it = m_committed.find(key_str);

    if (it == m_committed.end()) {
      auto new_entry = TableTraits<EntryType>::create(
          key, std::forward<Args>(create_args)...);
      if (!new_entry) {
        return std::shared_ptr<const EntryType>();
      }
      it = m_committed.emplace(key_str, std::move(new_entry)).first;
    }

    return it->second;
  }

  bool has_uncommitted(THD *thd) const {
    assert_read_or_write_lock_held();
    auto thd_it = m_uncommitted.find(thd);
    return (thd_it != m_uncommitted.end() && thd_it->second.elements != 0);
  }

  // ===== High-level operation marking APIs =====
  // REQUIRES: Caller must hold write lock
  // Entry parameters use rvalue references to make ownership transfer explicit.
  // Callers must use std::move() to pass entries.

  using key_type = typename EntryType::key_type;

  // Mark an entry for insertion
  bool MarkForInsertion(THD &thd, EntryType &&entry) {
    return add_uncommitted_insert_or_update(thd, OperationType::INSERT,
                                            std::move(entry));
  }

  // Mark an entry for update
  // old_key is the key to look up for update (may differ from entry.key() for
  // renames)
  // Pass by value to enable move semantics
  bool MarkForUpdate(THD &thd, EntryType &&entry, key_type old_key) {
    return add_uncommitted_insert_or_update(
        thd, OperationType::UPDATE, std::move(entry), std::move(old_key));
  }

  // Mark an entry for deletion by key
  // DELETE only needs the key - no entry required
  // Pass by value to enable move semantics
  bool MarkForDeletion(THD &thd, key_type key) {
    return add_uncommitted_delete(thd, std::move(key));
  }

  // ===== Internal operation tracking =====

 private:
  // Internal: mark INSERT or UPDATE operation
  // For INSERT, key is unused (default empty). For UPDATE, key is the old key.
  // Caller must hold write lock
  bool add_uncommitted_insert_or_update(THD &thd, OperationType op_type,
                                        EntryType &&entry, key_type key = {}) {
    assert_write_lock_held();
    assert(op_type == OperationType::INSERT ||
           op_type == OperationType::UPDATE);

    // Create shared_ptr for the entry
    // TODO(villagesql-ga): what happens if the underlying allocation fails? It
    // probably throws an exception and crashes, but we should validate that. If
    // that's the behavior, we either need to wrap make_shared<> or else come up
    // with a nothrow alternative that achieves the same result. This would help
    // us more consistently handle OOM conditions.
    auto entry_ptr = std::make_shared<EntryType>(std::move(entry));
    if (should_assert_if_null(entry_ptr)) {
      my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR), sizeof(EntryType));
      return true;
    }

    // Allocate pending operation on heap - cleaned up in commit() or rollback()
    PendingOperation<EntryType> *op = new PendingOperation<EntryType>(
        op_type, std::move(entry_ptr), std::move(key));
    if (should_assert_if_null(op)) {
      my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR),
               sizeof(PendingOperation<EntryType>));
      return true;
    }

    // Append to the list for this THD
    m_uncommitted[&thd].link_in_list(op, &op->next);
    return false;
  }

  // Internal: mark DELETE operation (by key, no entry needed)
  // Caller must hold write lock
  bool add_uncommitted_delete(THD &thd, key_type key) {
    assert_write_lock_held();

    // Allocate pending operation on heap - cleaned up in commit() or rollback()
    // entry is nullptr for DELETE-by-key
    PendingOperation<EntryType> *op = new PendingOperation<EntryType>(
        OperationType::DELETE, nullptr, std::move(key));
    if (should_assert_if_null(op)) {
      my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR),
               sizeof(PendingOperation<EntryType>));
      return true;
    }

    // Append to the list for this THD
    m_uncommitted[&thd].link_in_list(op, &op->next);
    return false;
  }

 public:
  // ===== Transaction lifecycle =====

  // Requires write lock
  void commit(THD *thd) {
    assert_write_lock_held();
    if (!thd) return;

    auto thd_it = m_uncommitted.find(thd);
    if (thd_it == m_uncommitted.end()) {
      return;  // No uncommitted operations
    }

    // Apply all pending operations to committed map in order
    PendingOperation<EntryType> *op = thd_it->second.first;
    while (op) {
      PendingOperation<EntryType> *next_op = op->next;

      switch (op->op_type) {
        case OperationType::INSERT:
        case OperationType::UPDATE: {
          // For INSERT and UPDATE, add/replace entry in committed map
          assert(op->entry);
          const std::string key = op->entry->key().str();
          m_committed[key] = std::move(op->entry);
          // For UPDATE with key change, also remove old key
          const std::string old_key = op->key.str();
          if (op->op_type == OperationType::UPDATE && !old_key.empty() &&
              old_key != key) {
            m_committed.erase(old_key);
          }
          break;
        }

        case OperationType::DELETE:
          // For DELETE, remove from committed map using stored key
          m_committed.erase(op->key.str());
          break;
      }

      // Delete the node (heap allocated)
      delete op;
      op = next_op;
    }

    m_uncommitted.erase(thd);
  }

  // Requires write lock
  void rollback(THD *thd) {
    assert_write_lock_held();
    if (!thd) return;

    auto thd_it = m_uncommitted.find(thd);
    if (thd_it != m_uncommitted.end()) {
      // Delete all pending operations
      PendingOperation<EntryType> *op = thd_it->second.first;
      while (op) {
        PendingOperation<EntryType> *next_op = op->next;
        delete op;
        op = next_op;
      }
    }

    m_uncommitted.erase(thd);
  }

  // ===== Bulk operations =====

  // Requires write lock
  void clear() {
    assert_write_lock_held();
    m_committed.clear();
    m_uncommitted.clear();
  }

  // ===== Table I/O (PERSISTENT mode only) =====
  // These methods are only available when Mode == PersistenceMode::PERSISTENT.
  // For MEMORY_ONLY maps, these methods do not exist (compile error if called).

  // Reload all entries from the backing system table
  // Caller must hold write lock
  // Returns false on success, true on error
  template <PersistenceMode M = Mode,
            std::enable_if_t<M == PersistenceMode::PERSISTENT, int> = 0>
  bool reload_from_table(THD *thd, const char *schema_name,
                         const char *table_name);

  // Write all uncommitted entries for this THD to the backing system table
  // Caller must hold read lock
  // Returns false on success, true on error
  template <PersistenceMode M = Mode,
            std::enable_if_t<M == PersistenceMode::PERSISTENT, int> = 0>
  bool write_uncommitted_to_table(THD *thd, const char *schema_name,
                                  const char *table_name);

  // ===== Prefix queries =====
  // These methods use the EntryType's key_prefix_type for type safety.
  // Entry types without key_prefix_type don't support prefix queries.

  // Get all entries matching a prefix
  // REQUIRES: Caller must hold read lock
  // WARNING: Returned pointers are only valid while lock is held
  template <typename T = EntryType>
  std::vector<const EntryType *> get_prefix_committed(
      const typename T::key_prefix_type &prefix) const {
    assert_read_or_write_lock_held();
    std::vector<const EntryType *> result;
    const std::string &prefix_str = prefix.str();
    if (prefix_str.empty()) {
      return result;
    }
    std::string upper = prefix_str;
    upper.back() = upper.back() + 1;
    auto it = m_committed.lower_bound(prefix_str);
    while (it != m_committed.end() && it->first < upper) {
      result.push_back(it->second.get());
      ++it;
    }
    return result;
  }

  // Get all committed entries in the map
  // Useful for iterating over all entries (e.g., during startup validation)
  // REQUIRES: Caller must hold read lock
  // WARNING: The returned pointers are only valid while the lock is held.
  // The lock must remain held for the entire duration that the caller uses
  // the returned vector and any dereferenced pointers.
  std::vector<const EntryType *> get_all_committed() const {
    assert_read_or_write_lock_held();
    std::vector<const EntryType *> result;
    result.reserve(m_committed.size());

    for (const auto &[key, entry] : m_committed) {
      result.push_back(entry.get());
    }

    return result;
  }

  // Check if entries exist matching a prefix
  // REQUIRES: Caller must hold read lock
  template <typename T = EntryType>
  bool has_prefix_committed(const typename T::key_prefix_type &prefix) const {
    assert_read_or_write_lock_held();
    const std::string &prefix_str = prefix.str();
    if (prefix_str.empty()) {
      return false;
    }
    std::string upper = prefix_str;
    upper.back() = upper.back() + 1;
    auto it = m_committed.lower_bound(prefix_str);
    return (it != m_committed.end() && it->first < upper);
  }

  // ===== Statistics =====

  struct Stats {
    size_t committed_entries;
    size_t uncommitted_entries;
    uint64_t hits;
    uint64_t misses;
  };

  // Requires read lock
  Stats get_stats() const {
    assert_read_or_write_lock_held();

    size_t uncommitted_count = 0;
    for (const auto &[thd, list] : m_uncommitted) {
      uncommitted_count += list.elements;
    }

    return {m_committed.size(), uncommitted_count, m_hits.load(),
            m_misses.load()};
  }

 private:
  using EntryKeyType = typename EntryType::key_type;
  using CommittedMap = std::map<std::string, std::shared_ptr<EntryType>>;
  using UncommittedList = SQL_I_List<PendingOperation<EntryType>>;

  CommittedMap m_committed;
  std::map<THD *, UncommittedList> m_uncommitted;

  // Per-map statistics
  mutable std::atomic<uint64_t> m_hits{0};
  mutable std::atomic<uint64_t> m_misses{0};

  // Pointer to parent's lock for assertions
  mysql_rwlock_t *m_parent_lock;

  // Helpers to assert that lock is held (checked during debug mode)
  // Assert that write lock is held
  void assert_write_lock_held() const {
    assert(m_parent_lock != nullptr &&
           "SystemTableMap used before parent lock was initialized");
    // Try to acquire read lock - should fail if we have write lock
    assert(mysql_rwlock_tryrdlock(m_parent_lock) != 0 &&
           "Write lock not held - able to acquire read lock");
  }

  // Assert that read lock is held (could be read or write)
  void assert_read_or_write_lock_held() const {
    assert(m_parent_lock != nullptr &&
           "SystemTableMap used before parent lock was initialized");
    // Try to acquire write lock - should fail if we have any lock
    assert(mysql_rwlock_trywrlock(m_parent_lock) != 0 &&
           "No lock held - able to acquire write lock");
  }

  // Note: No locks - parent VictionaryClient's lock protects all access

  // Allow VictionaryClient to access private members for bulk loading
  friend class VictionaryClient;
};

// Convenience alias for in-memory-only extension object maps
template <typename EntryType>
using ExtensionObjectMap =
    SystemTableMap<EntryType, PersistenceMode::MEMORY_ONLY>;

// VictionaryClient: In-memory registry of all VillageSQL extension
// objects/system table metadata. This is the authoritative source for system
// table state during server runtime, as well as key VillageSQL extension
// objects.
class VictionaryClient {
 public:
  // Singleton instance
  static VictionaryClient &instance();

  // Initialize the client (called during server startup), and load all records.
  bool init(THD *thd);

  // Destroy the client (called during server shutdown)
  void destroy();

  // ===== Access to specific table maps =====

  SystemTableMap<PropertyEntry> &properties() { return m_properties; }
  SystemTableMap<ColumnEntry> &columns() { return m_columns; }
  SystemTableMap<SpParamEntry> &sp_params() { return m_sp_params; }
  SystemTableMap<ExtensionEntry> &extensions() { return m_extensions; }
  ExtensionObjectMap<TypeDescriptor> &type_descriptors() {
    return m_type_descriptors;
  }
  ExtensionObjectMap<ExtensionDescriptor> &extension_descriptors() {
    return m_extension_descriptors;
  }
  ExtensionObjectMap<TypeContext> &type_contexts() { return m_type_contexts; }
  ExtensionObjectMap<FuncDescriptor> &funcs() { return m_funcs; }

  const SystemTableMap<PropertyEntry> &properties() const {
    return m_properties;
  }
  const SystemTableMap<ColumnEntry> &columns() const { return m_columns; }
  const SystemTableMap<SpParamEntry> &sp_params() const { return m_sp_params; }
  const SystemTableMap<ExtensionEntry> &extensions() const {
    return m_extensions;
  }
  const ExtensionObjectMap<TypeDescriptor> &type_descriptors() const {
    return m_type_descriptors;
  }
  const ExtensionObjectMap<ExtensionDescriptor> &extension_descriptors() const {
    return m_extension_descriptors;
  }
  const ExtensionObjectMap<TypeContext> &type_contexts() const {
    return m_type_contexts;
  }
  const ExtensionObjectMap<FuncDescriptor> &funcs() const { return m_funcs; }

  // ===== Convenience query methods =====

  // Get all custom columns for a table
  // Caller must hold at least read lock
  // Returns vector of pointers to ColumnEntry (valid while lock is held)
  std::vector<const ColumnEntry *> GetCustomColumnsForTable(
      const std::string &db_name, const std::string &table_name) const;

  // Get all custom SP params for a stored procedure/function
  // Caller must hold at least read lock
  // Returns vector of pointers to SpParamEntry (valid while lock is held)
  std::vector<const SpParamEntry *> GetCustomSpParamsForSP(
      const std::string &db_name, const std::string &sp_name) const;

  // ===== Transaction lifecycle - operates on ALL tables =====

  // Commit all uncommitted entries across all tables for this transaction
  void commit_all_tables(THD *thd);

  // Rollback all uncommitted entries across all tables for this transaction
  void rollback_all_tables(THD *thd);

  // Write all uncommitted entries to their backing system tables
  // Must be called BEFORE transaction commits so writes are part of txn
  // Returns false on success, true on error
  bool write_all_uncommitted_entries(THD *thd);

  // ===== Global operations =====

  // Clear all tables (mostly for testing)
  void clear_all_tables();

  // ===== Locking helpers (public RAII interface only) =====

  // RAII lock guard for read locks
  class ReadLockGuard {
   public:
    explicit ReadLockGuard(const VictionaryClient *client) : client_(client) {
      client_->acquire_read_lock();
    }

    ~ReadLockGuard() { client_->release_lock(); }

    // Disable copy and move
    ReadLockGuard(const ReadLockGuard &) = delete;
    ReadLockGuard &operator=(const ReadLockGuard &) = delete;
    ReadLockGuard(ReadLockGuard &&) = delete;
    ReadLockGuard &operator=(ReadLockGuard &&) = delete;

   private:
    const VictionaryClient *client_;
  };

  // RAII lock guard for write locks
  class WriteLockGuard {
   public:
    explicit WriteLockGuard(VictionaryClient *client) : client_(client) {
      client_->acquire_write_lock();
    }

    ~WriteLockGuard() { client_->release_lock(); }

    // Disable copy and move
    WriteLockGuard(const WriteLockGuard &) = delete;
    WriteLockGuard &operator=(const WriteLockGuard &) = delete;
    WriteLockGuard(WriteLockGuard &&) = delete;
    WriteLockGuard &operator=(WriteLockGuard &&) = delete;

   private:
    VictionaryClient *client_;
  };

  // Factory methods for lock guards (allows auto type deduction)
  ReadLockGuard get_read_lock() const { return ReadLockGuard(this); }
  WriteLockGuard get_write_lock() { return WriteLockGuard(this); }

  // Check if initialized
  bool is_initialized() const { return m_initialized.load(); }

  // Assert that write lock is held (for debugging)
  void assert_write_lock_held() const {
    // Try to acquire read lock - should fail if we have write lock
    assert(mysql_rwlock_tryrdlock(&m_lock) != 0 &&
           "Write lock not held - able to acquire read lock");
  }

 private:
  VictionaryClient()
      : m_properties(&m_lock),  // Pass lock pointer to map
        m_columns(&m_lock),
        m_sp_params(&m_lock),
        m_extensions(&m_lock),
        m_type_descriptors(&m_lock),
        m_extension_descriptors(&m_lock),
        m_type_contexts(&m_lock),
        m_funcs(&m_lock) {}
  ~VictionaryClient();

  // Disable copy and assignment
  VictionaryClient(const VictionaryClient &) = delete;
  VictionaryClient &operator=(const VictionaryClient &) = delete;

  // Reload all tables from their backing system tables, which happens during
  // init(). Returns false on success, true on error.
  bool reload_all_tables(THD *thd);

  // Initialize for unit testing without loading from database tables.
  // This only initializes the lock and marks the client as initialized.
  // Returns false on success, true on error.
  bool init_for_testing();

  // Friend classes
  friend class villagesql_unittest::VictionaryClientTest;
  friend class ReadLockGuard;
  friend class WriteLockGuard;

  // ===== Private locking methods (use public RAII guards instead) =====
  void acquire_read_lock() const {
    assert((m_initialized.load() || m_initializing.load()) &&
           "VictionaryClient not initialized or initializing");
    [[maybe_unused]] int result = mysql_rwlock_rdlock(&m_lock);
    assert(result == 0 && "Failed to acquire read lock");
  }

  void acquire_write_lock() {
    assert((m_initialized.load() || m_initializing.load()) &&
           "VictionaryClient not initialized or initializing");
    [[maybe_unused]] int result = mysql_rwlock_wrlock(&m_lock);
    assert(result == 0 && "Failed to acquire write lock");
  }

  void release_lock() const {
    assert((m_initialized.load() || m_initializing.load()) &&
           "VictionaryClient not initialized or initializing");
    [[maybe_unused]] int result = mysql_rwlock_unlock(&m_lock);
    assert(result == 0 && "Failed to release lock");
  }

  // Single lock protecting ALL maps
  mutable mysql_rwlock_t m_lock;
  std::atomic<bool> m_initialized{false};
  std::atomic<bool> m_initializing{false};

  // Hard-coded map instances - one per system table
  SystemTableMap<PropertyEntry> m_properties;
  SystemTableMap<ColumnEntry> m_columns;
  SystemTableMap<SpParamEntry> m_sp_params;
  SystemTableMap<ExtensionEntry> m_extensions;

  // In-memory descriptor maps (MEMORY_ONLY - no backing table)
  ExtensionObjectMap<TypeDescriptor> m_type_descriptors;
  ExtensionObjectMap<ExtensionDescriptor> m_extension_descriptors;
  ExtensionObjectMap<TypeContext> m_type_contexts;
  ExtensionObjectMap<FuncDescriptor> m_funcs;
};

// ===== Helper functions =====

// Find an open table in THD's open_tables list
// Returns nullptr if not found
inline TABLE *find_open_table(THD *thd, const char *schema_name,
                              const char *table_name) {
  if (!thd || !schema_name || !table_name) {
    return nullptr;
  }

  for (TABLE *table = thd->open_tables; table; table = table->next) {
    Table_ref *table_list = table->pos_in_table_list;
    if (table_list && system_name_eq(table_list->db, schema_name) &&
        system_name_eq(table_list->table_name, table_name)) {
      return table;
    }
  }

  return nullptr;  // Not found
}

}  // namespace villagesql

// ===== SystemTableMap template method implementations =====
#include "villagesql/schema/victionary_client-impl.h"

#endif  // VILLAGESQL_SCHEMA_VICTIONARY_CLIENT_H_
