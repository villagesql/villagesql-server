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

#ifndef VILLAGESQL_SCHEMA_SYSTABLE_PROPERTIES_H_
#define VILLAGESQL_SCHEMA_SYSTABLE_PROPERTIES_H_

#include <cassert>
#include <string>
#include "villagesql/schema/identifier_names.h"
#include "villagesql/schema/systable/helpers.h"

// Forward declarations
struct TABLE;

namespace villagesql {

// Forward declaration for TableTraits
template <typename EntryType>
struct TableTraits;

// Key for properties table entries
// Format: "normalized_property_name"
// Properties use extension name rules for string comparison.
// Stores original name for display, normalized key for lookups.
struct PropertyKey {
 public:
  PropertyKey() = default;

  explicit PropertyKey(std::string name)
      : name_(std::move(name)),
        normalized_key_(canonical_extension_name(name_)) {}

  const std::string &str() const { return normalized_key_; }

  // Component accessor (returns original value)
  const std::string &name() const { return name_; }

  // Comparison operators for std::map (use normalized key)
  bool operator<(const PropertyKey &other) const {
    return normalized_key_ < other.normalized_key_;
  }
  bool operator==(const PropertyKey &other) const {
    return normalized_key_ == other.normalized_key_;
  }

 private:
  std::string name_;
  std::string normalized_key_;
};

// Entry for properties system table
struct PropertyEntry {
 public:
  using key_type = PropertyKey;

  // Non-key data (public)
  std::string value;
  std::string description;

  explicit PropertyEntry(PropertyKey key) : key_(std::move(key)) {}

  PropertyEntry() = default;

  const PropertyKey &key() const { return key_; }

  // Accessor for key component (delegate to key)
  const std::string &name() const { return key_.name(); }

 protected:
  void set_key(PropertyKey key) { key_ = std::move(key); }
  friend struct TableTraits<PropertyEntry>;

 private:
  PropertyKey key_;
};

// TableTraits specialization for PropertyEntry
// Handles prefix construction, serialization (read), and deserialization
// (write)
template <>
struct TableTraits<PropertyEntry> {
  // ===== Serialization (read from TABLE) =====

  // Read a row from villagesql.properties table into a PropertyEntry
  // Returns false on success, true on error
  static bool read_from_table(TABLE &table, PropertyEntry &entry);

  // ===== Deserialization (write to TABLE) =====

  // Write a PropertyEntry to villagesql.properties table
  // Assumes table is already positioned for write (empty_record called, etc.)
  // Returns false on success, true on error
  static bool write_to_table(TABLE &table, const PropertyEntry &entry);

  // Stub implementations for UPDATE and DELETE (not yet implemented for
  // properties)
  static bool update_in_table(TABLE & /*table*/,
                              const PropertyEntry & /*entry*/,
                              const PropertyKey & /*old_key*/) {
    assert(VILLAGESQL_NOT_IMPLEMENTED);
    return true;  // Not implemented - return error
  }

  static bool delete_from_table(TABLE & /*table*/,
                                const PropertyEntry & /*entry*/) {
    assert(VILLAGESQL_NOT_IMPLEMENTED);
    return true;  // Not implemented - return error
  }
};

}  // namespace villagesql

#endif  // VILLAGESQL_SCHEMA_SYSTABLE_PROPERTIES_H_
