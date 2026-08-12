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

#ifndef VILLAGESQL_SCHEMA_SYSTABLE_CUSTOM_COLUMNS_H_
#define VILLAGESQL_SCHEMA_SYSTABLE_CUSTOM_COLUMNS_H_

#include <string>

#include "villagesql/schema/identifier_names.h"

// Forward declarations
struct TABLE;

namespace villagesql {

// Forward declarations
class TypeContext;
template <typename EntryType>
struct TableTraits;

// Prefix key for querying all columns in a table
// Format: "normalized_db.normalized_table."
struct ColumnKeyPrefix {
 public:
  // If table_name is empty, produces "db." prefix for all tables in that db
  ColumnKeyPrefix(std::string db_name, std::string table_name)
      : db_(std::move(db_name)),
        table_(std::move(table_name)),
        normalized_prefix_(
            canonical_database_name(db_) + "." +
            (table_.empty() ? "" : canonical_table_name(table_) + ".")) {}

  const std::string &str() const { return normalized_prefix_; }

  // Component accessors (return original values)
  const std::string &db() const { return db_; }
  const std::string &table() const { return table_; }

 private:
  std::string db_;
  std::string table_;
  std::string normalized_prefix_;
};

// Key for custom_columns table entries
// Format: "normalized_db.normalized_table.normalized_column"
// Stores original component values for display, plus normalized key for
// lookups.
struct ColumnKey {
 public:
  ColumnKey() = default;

  ColumnKey(std::string db_name, std::string table_name,
            std::string column_name)
      : db_(std::move(db_name)),
        table_(std::move(table_name)),
        column_(std::move(column_name)),
        normalized_key_(canonical_database_name(db_) + "." +
                        canonical_table_name(table_) + "." +
                        canonical_column_name(column_)) {}

  const std::string &str() const { return normalized_key_; }

  // Component accessors (return original values)
  const std::string &db() const { return db_; }
  const std::string &table() const { return table_; }
  const std::string &column() const { return column_; }

  // Comparison operators for std::map (use normalized key)
  bool operator<(const ColumnKey &other) const {
    return normalized_key_ < other.normalized_key_;
  }
  bool operator==(const ColumnKey &other) const {
    return normalized_key_ == other.normalized_key_;
  }

 private:
  std::string db_;
  std::string table_;
  std::string column_;
  std::string normalized_key_;
};

// Entry for custom_columns system table
// This represents a column in a user table that uses a custom type
struct ColumnEntry {
 public:
  using key_type = ColumnKey;
  using key_prefix_type = ColumnKeyPrefix;

  // Non-key data (public)
  std::string extension_name;
  std::string extension_version;
  std::string type_name;
  std::string type_parameters = "{}";  // JSON-serialized TypeParameters

  // Full constructor with all fields
  ColumnEntry(ColumnKey key, std::string ext_name, std::string ext_version,
              std::string t_name, std::string type_params = "{}")
      : extension_name(std::move(ext_name)),
        extension_version(std::move(ext_version)),
        type_name(std::move(t_name)),
        type_parameters(std::move(type_params)),
        key_(std::move(key)) {}

  // Construct with key only, other fields can be set separately (useful for
  // testing)
  explicit ColumnEntry(ColumnKey key) : key_(std::move(key)) {}

  ColumnEntry() = default;

  const ColumnKey &key() const { return key_; }

  // Accessors for key components (delegate to key)
  const std::string &db_name() const { return key_.db(); }
  const std::string &table_name() const { return key_.table(); }
  const std::string &column_name() const { return key_.column(); }

 protected:
  void set_key(ColumnKey key) { key_ = std::move(key); }
  friend struct TableTraits<ColumnEntry>;

 private:
  ColumnKey key_;
};

// TableTraits specialization for ColumnEntry
template <>
struct TableTraits<ColumnEntry> {
  // ===== Serialization (read from TABLE) =====

  // Read a row from villagesql.custom_columns table into a ColumnEntry
  // Returns false on success, true on error
  static bool read_from_table(TABLE &table, ColumnEntry &entry);

  // ===== Deserialization (write to TABLE) =====

  // Write a ColumnEntry to villagesql.custom_columns table
  // Assumes table is already positioned for write (empty_record called, etc.)
  // Returns false on success, true on error
  static bool write_to_table(TABLE &table, const ColumnEntry &entry);

  // Update a ColumnEntry in villagesql.custom_columns table
  // old_key is the key of the row to update (may differ from entry.key() if
  // key columns changed)
  // Returns false on success, true on error
  static bool update_in_table(TABLE &table, const ColumnEntry &entry,
                              const std::string &old_key);

  // Delete a ColumnEntry from villagesql.custom_columns table
  // Returns false on success, true on error
  static bool delete_from_table(TABLE &table, const ColumnEntry &entry);
};

}  // namespace villagesql

#endif  // VILLAGESQL_SCHEMA_SYSTABLE_CUSTOM_COLUMNS_H_
