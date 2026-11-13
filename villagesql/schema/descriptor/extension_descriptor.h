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

// ExtensionDescriptor: In-memory descriptor for loaded VillageSQL extensions.
// Unlike ExtensionEntry (which is backed by the extensions system table),
// ExtensionDescriptor holds runtime state like the pointer to the loaded
// ExtensionRegistration from the .so file.

#ifndef VILLAGESQL_SCHEMA_DESCRIPTOR_EXTENSION_DESCRIPTOR_H_
#define VILLAGESQL_SCHEMA_DESCRIPTOR_EXTENSION_DESCRIPTOR_H_

#include <string>
#include <utility>

#include "villagesql/veb/veb_file.h"

namespace villagesql {

template <typename EntryType>
struct TableTraits;

namespace veb {
struct ExtensionRegistration;
}  // namespace veb

// Key for ExtensionDescriptor entries in the VictionaryClient map.
// Format: "normalized_extension_name.normalized_version"
// Stores original component values for display, plus normalized key for
// lookups.
struct ExtensionDescriptorKey {
 public:
  ExtensionDescriptorKey() = default;

  ExtensionDescriptorKey(std::string extension_name,
                         std::string extension_version);

  const std::string &str() const { return normalized_key_; }

  // Component accessors (return original values)
  const std::string &extension_name() const { return extension_name_; }
  const std::string &extension_version() const { return extension_version_; }

  bool operator<(const ExtensionDescriptorKey &other) const {
    return normalized_key_ < other.normalized_key_;
  }
  bool operator==(const ExtensionDescriptorKey &other) const {
    return normalized_key_ == other.normalized_key_;
  }

 private:
  std::string extension_name_;
  std::string extension_version_;
  std::string normalized_key_;
};

// ExtensionDescriptor: In-memory descriptor for a loaded extension.
// Holds a pointer to the ExtensionRegistration for the loaded .so file.
// This is transactional (follows commit/rollback semantics) but not persisted.
class ExtensionDescriptor {
 public:
  using key_type = ExtensionDescriptorKey;

  // Default constructor - creates an empty/invalid descriptor
  // Required for use with SystemTableMap's PendingOperation
  ExtensionDescriptor() = default;

  // Construct with key only, other fields can be set separately (useful for
  // testing)
  explicit ExtensionDescriptor(ExtensionDescriptorKey key)
      : key_(std::move(key)) {}

  // Full constructor with registration pointer
  // `registration` is cleaned up outside of this class.
  ExtensionDescriptor(ExtensionDescriptorKey key,
                      veb::ExtensionRegistration registration)
      : key_(std::move(key)), registration_(std::move(registration)) {}

  ExtensionDescriptor(const ExtensionDescriptor &) = default;
  ExtensionDescriptor &operator=(const ExtensionDescriptor &) = default;

  // Enable move (needed for SystemTableMap storage)
  ExtensionDescriptor(ExtensionDescriptor &&) = default;
  ExtensionDescriptor &operator=(ExtensionDescriptor &&) = default;

  ~ExtensionDescriptor() = default;

  // Key accessor (required by SystemTableMap)
  const ExtensionDescriptorKey &key() const { return key_; }

  // Accessors for key components (delegate to key)
  const std::string &extension_name() const { return key_.extension_name(); }
  const std::string &extension_version() const {
    return key_.extension_version();
  }

  // Registration pointer accessor (non-owning)
  const veb::ExtensionRegistration &registration() const {
    return registration_;
  }

 private:
  ExtensionDescriptorKey key_;

  // The .so in `registration_` is unloaded outside of this class at
  // shutdown or uninstall.
  veb::ExtensionRegistration registration_;
};

// TableTraits specialization for ExtensionDescriptor.
// Empty because ExtensionDescriptor doesn't have table-backed operations.
template <>
struct TableTraits<ExtensionDescriptor> {};

}  // namespace villagesql

#endif  // VILLAGESQL_SCHEMA_DESCRIPTOR_EXTENSION_DESCRIPTOR_H_
