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

// IndexProfileDescriptor: In-memory descriptor for a VillageSQL index profile.
// A profile binds a custom index type to a data type and declares the index
// functions and ordering to use. Built from extension registration
// (IndexProfileDesc) and held in VictionaryClient for the lifetime of the
// loaded extension.

#ifndef VILLAGESQL_SCHEMA_DESCRIPTOR_INDEX_PROFILE_DESCRIPTOR_H_
#define VILLAGESQL_SCHEMA_DESCRIPTOR_INDEX_PROFILE_DESCRIPTOR_H_

#include <cstdint>
#include <string>
#include <vector>

#include "villagesql/schema/descriptor/index_type_descriptor.h"
#include "villagesql/schema/descriptor/type_descriptor.h"
#include "villagesql/schema/identifier_names.h"
#include "villagesql/sdk/include/villagesql/abi/preview/index.h"

namespace villagesql {

template <typename EntryType>
struct TableTraits;

// Prefix key for querying IndexProfileDescriptors by profile name (and
// optionally extension name). Format: "normalized_profile_name." or
// "normalized_profile_name.normalized_ext_name."
struct IndexProfileDescriptorKeyPrefix {
  // Query by profile name only (for unqualified lookups).
  explicit IndexProfileDescriptorKeyPrefix(std::string profile_name)
      : profile_name_(std::move(profile_name)),
        normalized_prefix_(canonical_extension_name(profile_name_) + ".") {}

  // Query by profile name + extension name.
  IndexProfileDescriptorKeyPrefix(std::string profile_name,
                                  std::string extension_name)
      : profile_name_(std::move(profile_name)),
        extension_name_(std::move(extension_name)),
        normalized_prefix_(
            canonical_extension_name(profile_name_) + "." +
            (extension_name_.empty()
                 ? ""
                 : canonical_extension_name(extension_name_) + ".")) {}

  const std::string &str() const { return normalized_prefix_; }
  const std::string &profile_name() const { return profile_name_; }
  const std::string &extension_name() const { return extension_name_; }

 private:
  std::string profile_name_;
  std::string extension_name_;
  std::string normalized_prefix_;
};

// Key for IndexProfileDescriptor entries.
// Format:
// "normalized_profile_name.normalized_extension_name.normalized_version"
// Version is included because a profile is defined by a specific extension
// version, matching the TypeDescriptorKey and IndexTypeDescriptorKey pattern.
struct IndexProfileDescriptorKey {
 public:
  IndexProfileDescriptorKey() = default;

  IndexProfileDescriptorKey(std::string profile_name,
                            std::string extension_name,
                            std::string extension_version);

  const std::string &str() const { return normalized_key_; }
  const std::string &profile_name() const { return profile_name_; }
  const std::string &extension_name() const { return extension_name_; }
  const std::string &extension_version() const { return extension_version_; }

  bool operator<(const IndexProfileDescriptorKey &other) const {
    return normalized_key_ < other.normalized_key_;
  }
  bool operator==(const IndexProfileDescriptorKey &other) const {
    return normalized_key_ == other.normalized_key_;
  }

 private:
  std::string profile_name_;
  std::string extension_name_;
  std::string extension_version_;
  std::string normalized_key_;
};

// IndexProfileDescriptor: Immutable in-memory descriptor for an index profile.
// Records which data type and index type the profile applies to, the function
// bindings, scan ordering, and whether this is the default profile for the
// (data_type, index_type) pair.
class IndexProfileDescriptor {
 public:
  using key_type = IndexProfileDescriptorKey;
  using key_prefix_type = IndexProfileDescriptorKeyPrefix;

  IndexProfileDescriptor(IndexProfileDescriptorKey key,
                         TypeDescriptorKeyPrefix type_ref,
                         IndexTypeDescriptorKeyPrefix index_type_ref,
                         std::vector<vef_index_profile_fn_binding_t> functions,
                         std::vector<vef_index_profile_fn_binding_t> helpers,
                         uint8_t ordering, bool default_for_type);

  // Disable copy, enable move.
  IndexProfileDescriptor(const IndexProfileDescriptor &) = delete;
  IndexProfileDescriptor &operator=(const IndexProfileDescriptor &) = delete;
  IndexProfileDescriptor(IndexProfileDescriptor &&) = default;
  IndexProfileDescriptor &operator=(IndexProfileDescriptor &&) = default;

  const IndexProfileDescriptorKey &key() const { return key_; }
  const std::string &profile_name() const { return key_.profile_name(); }
  const std::string &extension_name() const { return key_.extension_name(); }
  const std::string &extension_version() const {
    return key_.extension_version();
  }

  // Prefix key for resolving the data type reference in VictionaryClient.
  const TypeDescriptorKeyPrefix &type_ref() const { return type_ref_; }

  // Prefix key for resolving the index type reference in VictionaryClient.
  const IndexTypeDescriptorKeyPrefix &index_type_ref() const {
    return index_type_ref_;
  }

  // Qualify an unresolved type_ref with the resolved extension name.
  // Called during registration when the profile was declared with an
  // unqualified data type name; the registrar resolves the extension and
  // writes it back before inserting the descriptor.
  void set_type_ref(TypeDescriptorKeyPrefix ref) { type_ref_ = std::move(ref); }

  // Qualify an unresolved index_type_ref with the resolved extension name.
  // Called during registration when the profile was declared with an
  // unqualified index type name; the registrar resolves the extension and
  // writes it back before inserting the descriptor.
  void set_index_type_ref(IndexTypeDescriptorKeyPrefix ref) {
    index_type_ref_ = std::move(ref);
  }

  // Bare name accessors (no extension qualifier) for diagnostics.
  const std::string &type_name() const { return type_ref_.type_name(); }
  const std::string &index_type_name() const {
    return index_type_ref_.index_type_name();
  }

  const std::vector<vef_index_profile_fn_binding_t> &functions() const {
    return functions_;
  }

  // Helper functions invoked only by the index implementation via profile_fn.
  const std::vector<vef_index_profile_fn_binding_t> &helpers() const {
    return helpers_;
  }

  uint8_t ordering() const { return ordering_; }
  bool ordering_asc() const {
    return (ordering_ & VEF_INDEX_ORDERING_ASC) != 0;
  }
  bool ordering_desc() const {
    return (ordering_ & VEF_INDEX_ORDERING_DESC) != 0;
  }

  // True if this profile is the default for its (type_name, index_type_name)
  // pair — used when no profile is named at CREATE INDEX time.
  bool default_for_type() const { return default_for_type_; }

 private:
  IndexProfileDescriptorKey key_;
  TypeDescriptorKeyPrefix type_ref_;
  IndexTypeDescriptorKeyPrefix index_type_ref_;
  std::vector<vef_index_profile_fn_binding_t> functions_;
  std::vector<vef_index_profile_fn_binding_t> helpers_;
  uint8_t ordering_{VEF_INDEX_ORDERING_ASC};
  bool default_for_type_{false};
};

// TableTraits specialization — MEMORY_ONLY, no backing table.
template <>
struct TableTraits<IndexProfileDescriptor> {};

}  // namespace villagesql

#endif  // VILLAGESQL_SCHEMA_DESCRIPTOR_INDEX_PROFILE_DESCRIPTOR_H_
