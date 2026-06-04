// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

// IndexTypeDescriptor: In-memory descriptor for a VillageSQL custom index type.
// Built from extension registration (IndexTypeDesc) and held in
// VictionaryClient for the lifetime of the loaded extension.

#ifndef VILLAGESQL_SCHEMA_DESCRIPTOR_INDEX_TYPE_DESCRIPTOR_H_
#define VILLAGESQL_SCHEMA_DESCRIPTOR_INDEX_TYPE_DESCRIPTOR_H_

#include <string>

#include "villagesql/schema/systable/helpers.h"
#include "villagesql/sdk/include/villagesql/abi/preview/index.h"

namespace villagesql {

template <typename EntryType>
struct TableTraits;

// Prefix key for querying IndexTypeDescriptors by index type name (and
// optionally extension name). Format: "normalized_index_type_name." or
// "normalized_index_type_name.normalized_ext_name."
struct IndexTypeDescriptorKeyPrefix {
  // Query by index type name only (for unqualified lookups).
  explicit IndexTypeDescriptorKeyPrefix(std::string index_type_name)
      : index_type_name_(std::move(index_type_name)),
        normalized_prefix_(normalize_type_name(index_type_name_) + ".") {}

  // Query by index type name + extension name.
  IndexTypeDescriptorKeyPrefix(std::string index_type_name,
                               std::string extension_name)
      : index_type_name_(std::move(index_type_name)),
        extension_name_(std::move(extension_name)),
        normalized_prefix_(
            normalize_type_name(index_type_name_) + "." +
            (extension_name_.empty()
                 ? ""
                 : normalize_extension_name(extension_name_) + ".")) {}

  const std::string &str() const { return normalized_prefix_; }

  bool matches_key(const std::string &key) const {
    return key.size() >= normalized_prefix_.size() &&
           key.compare(0, normalized_prefix_.size(), normalized_prefix_) == 0;
  }

  const std::string &index_type_name() const { return index_type_name_; }
  const std::string &extension_name() const { return extension_name_; }

 private:
  std::string index_type_name_;
  std::string extension_name_;
  std::string normalized_prefix_;
};

// Key for IndexTypeDescriptor entries.
// Format:
// "normalized_index_type_name.normalized_extension_name.normalized_version"
struct IndexTypeDescriptorKey {
 public:
  IndexTypeDescriptorKey() = default;

  IndexTypeDescriptorKey(std::string index_type_name,
                         std::string extension_name,
                         std::string extension_version);

  const std::string &str() const { return normalized_key_; }
  const std::string &index_type_name() const { return index_type_name_; }
  const std::string &extension_name() const { return extension_name_; }
  const std::string &extension_version() const { return extension_version_; }

  bool operator<(const IndexTypeDescriptorKey &other) const {
    return normalized_key_ < other.normalized_key_;
  }
  bool operator==(const IndexTypeDescriptorKey &other) const {
    return normalized_key_ == other.normalized_key_;
  }

 private:
  std::string index_type_name_;
  std::string extension_name_;
  std::string extension_version_;
  std::string normalized_key_;
};

// IndexTypeDescriptor: Immutable in-memory descriptor for a custom index type.
// Holds the ABI interface table (function pointers) registered by the
// extension.
class IndexTypeDescriptor {
 public:
  using key_type = IndexTypeDescriptorKey;
  using key_prefix_type = IndexTypeDescriptorKeyPrefix;

  IndexTypeDescriptor() = default;

  IndexTypeDescriptor(IndexTypeDescriptorKey key, vef_type_index_intf_t intf);

  // Disable copy, enable move.
  IndexTypeDescriptor(const IndexTypeDescriptor &) = delete;
  IndexTypeDescriptor &operator=(const IndexTypeDescriptor &) = delete;
  IndexTypeDescriptor(IndexTypeDescriptor &&) = default;
  IndexTypeDescriptor &operator=(IndexTypeDescriptor &&) = default;

  const IndexTypeDescriptorKey &key() const { return key_; }
  const std::string &index_type_name() const { return key_.index_type_name(); }
  const std::string &extension_name() const { return key_.extension_name(); }
  const std::string &extension_version() const {
    return key_.extension_version();
  }
  const vef_type_index_intf_t &intf() const { return intf_; }

 private:
  IndexTypeDescriptorKey key_;
  vef_type_index_intf_t intf_{};
};

// TableTraits specialization — MEMORY_ONLY, no backing table.
template <>
struct TableTraits<IndexTypeDescriptor> {};

}  // namespace villagesql

#endif  // VILLAGESQL_SCHEMA_DESCRIPTOR_INDEX_TYPE_DESCRIPTOR_H_
