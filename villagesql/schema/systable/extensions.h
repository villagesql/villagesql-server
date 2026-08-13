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

#ifndef VILLAGESQL_SCHEMA_SYSTABLE_EXTENSIONS_H_
#define VILLAGESQL_SCHEMA_SYSTABLE_EXTENSIONS_H_

#include <optional>
#include <string>
#include <string_view>

#include "villagesql/schema/identifier_names.h"

// Forward declarations
struct TABLE;

namespace villagesql {

// Forward declaration for TableTraits
template <typename EntryType>
struct TableTraits;

// A deferred version update queued against an installed extension, applied
// at the next server restart. The class encapsulates the JSON wire format
// used by the storage layer so callers work with typed accessors instead
// of raw JSON.
class PendingAction {
 public:
  // Construct a request to swap the installed extension to target_version
  // at the next restart. requested_at is captured here (server local clock)
  // so callers don't need to know how timestamps are stamped.
  static PendingAction CreateVersionUpdate(std::string target_version,
                                           std::string target_veb_sha256);

  // Annotate this action with a failure reason (and the time it was
  // observed). Used at restart-apply time: when the swap cannot proceed,
  // the caller stamps the action so the row remains queryable as
  // "pending, attempted, failed". Replaces any previous failure record.
  void MarkFailed(std::string error_message);

  // Table-level round-trip for the systable I/O layer. The class owns its
  // own column-name(s); callers pass the table and the class finds the
  // right field(s).
  //
  // ReadFromTable: NULL column sets `out` to std::nullopt. Non-NULL is
  // parsed; on failure returns true with error_message populated.
  //
  // StoreToTable: nullopt stores NULL; engaged optional stores the
  // serialized form.
  //
  // Returns true on internal errors (e.g. expected column missing).
  static bool ReadFromTable(TABLE &table, std::optional<PendingAction> &out,
                            std::string &error_message);
  static bool StoreToTable(TABLE &table,
                           const std::optional<PendingAction> &value,
                           std::string &error_message);

  // SQL expressions for I_S view definitions to project individual logical
  // fields of a pending action against a row of the extensions table
  // aliased as `table_alias`. Returned strings are ready to feed to
  // `m_target_def.add_field`'s SQL-expression argument.
  //
  // The view definitions stay free of any knowledge that the underlying
  // storage is JSON; future schema-shape changes affect only the
  // implementations below.
  static std::string TargetVersionSqlExpr(std::string_view table_alias);
  static std::string RequestedAtSqlExpr(std::string_view table_alias);
  static std::string LastErrorSqlExpr(std::string_view table_alias);
  static std::string LastErrorAtSqlExpr(std::string_view table_alias);

  // Default-constructed action is in an unspecified but valid state. Used
  // by the storage layer as the out-parameter buffer for Deserialize;
  // callers should not read from a default-constructed PendingAction
  // before Deserialize or CreateVersionUpdate has populated it.
  PendingAction() = default;

  // Getters. The returned references are valid for as long as this
  // PendingAction is alive.
  const std::string &target_version() const;
  const std::string &target_veb_sha256() const;
  const std::string &requested_at() const;

  // Failure record. Empty when the action has not yet been attempted or
  // the latest attempt succeeded. Non-empty when the last attempt
  // recorded an error via MarkFailed.
  const std::string &last_error() const;
  const std::string &last_error_at() const;
  bool has_failure() const;

 private:
  // Wire format: build / parse the JSON serialization of this action.
  // Used internally by ReadFromTable / StoreToTable. Deserialize returns
  // true on failure with error_message set.
  std::string Serialize() const;
  static bool Deserialize(const std::string &raw, PendingAction &out,
                          std::string &error_message);

  // Internal layout is private. Field names and JSON shape may change
  // without breaking callers as long as the public getters keep returning
  // semantically equivalent values.
  std::string target_version_;
  std::string target_veb_sha256_;
  std::string requested_at_;
  std::string last_error_;
  std::string last_error_at_;
};

// Key for extensions table entries
// Format: "normalized_extension_name"
// Extensions use extension name normalization rules for case-insensitive
// comparison. Stores original name for display, normalized key for lookups.
struct ExtensionKey {
 public:
  ExtensionKey() = default;

  explicit ExtensionKey(std::string name)
      : extension_name_(std::move(name)),
        normalized_key_(canonical_extension_name(extension_name_)) {}

  const std::string &str() const { return normalized_key_; }

  // Component accessor (returns original value)
  const std::string &extension_name() const { return extension_name_; }

  // Comparison operators for std::map (use normalized key)
  bool operator<(const ExtensionKey &other) const {
    return normalized_key_ < other.normalized_key_;
  }
  bool operator==(const ExtensionKey &other) const {
    return normalized_key_ == other.normalized_key_;
  }

 private:
  std::string extension_name_;
  std::string normalized_key_;
};

// Entry for extensions system table
struct ExtensionEntry {
 public:
  using key_type = ExtensionKey;

  // Non-key data (public)
  std::string extension_version;
  std::string veb_sha256;

  // Pending deferred action for this extension. Absent (std::nullopt) when
  // no action is pending; present when one has been queued for the next
  // restart. The PendingAction class encapsulates the wire format and the
  // typed accessors; callers should not depend on the storage layout.
  std::optional<PendingAction> pending_action;

  // Full constructor with all fields
  ExtensionEntry(ExtensionKey key, std::string version, std::string sha256)
      : extension_version(std::move(version)),
        veb_sha256(std::move(sha256)),
        key_(std::move(key)) {}

  // Construct with key only, other fields can be set separately (useful for
  // testing)
  explicit ExtensionEntry(ExtensionKey key) : key_(std::move(key)) {}

  ExtensionEntry() = default;

  const ExtensionKey &key() const { return key_; }

  // Accessor for key component (delegate to key)
  const std::string &extension_name() const { return key_.extension_name(); }

  // Convenience: true if a deferred action is pending for this extension.
  bool has_pending_action() const { return pending_action.has_value(); }

 protected:
  void set_key(ExtensionKey key) { key_ = std::move(key); }
  friend struct TableTraits<ExtensionEntry>;

 private:
  ExtensionKey key_;
};

// TableTraits specialization for ExtensionEntry
template <>
struct TableTraits<ExtensionEntry> {
  // ===== Serialization (read from TABLE) =====

  // Read a row from villagesql.extensions table into an ExtensionEntry
  // Returns false on success, true on error
  static bool read_from_table(TABLE &table, ExtensionEntry &entry);

  // ===== Deserialization (write to TABLE) =====

  // Write an ExtensionEntry to villagesql.extensions table
  // Assumes table is already positioned for write (empty_record called, etc.)
  // Returns false on success, true on error
  static bool write_to_table(TABLE &table, const ExtensionEntry &entry);

  // Update an ExtensionEntry in villagesql.extensions table
  // old_key is the key of the row to update (may differ from entry.key() if
  // key columns changed). That is, if the key has changed, the old key must be
  // set in old_key; otherwise, entry.key() is used.
  // Returns false on success, true on error
  static bool update_in_table(TABLE &table, const ExtensionEntry &entry,
                              const ExtensionKey &old_key);

  // Delete an ExtensionEntry from villagesql.extensions table
  // Returns false on success, true on error
  static bool delete_from_table(TABLE &table, const ExtensionEntry &entry);
};

}  // namespace villagesql

#endif  // VILLAGESQL_SCHEMA_SYSTABLE_EXTENSIONS_H_
