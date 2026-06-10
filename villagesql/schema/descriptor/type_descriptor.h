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

// TypeDescriptor: In-memory descriptor for VillageSQL custom types.
// TypeDescriptor is built programmatically from extension .so files and
// holds direct function pointers rather than function names.

#ifndef VILLAGESQL_SCHEMA_DESCRIPTOR_TYPE_DESCRIPTOR_H_
#define VILLAGESQL_SCHEMA_DESCRIPTOR_TYPE_DESCRIPTOR_H_

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "villagesql/schema/systable/helpers.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"
#include "villagesql/types/storage.h"
#include "villagesql/types/type_function.h"

namespace villagesql {

template <typename EntryType>
struct TableTraits;

// Prefix key for querying TypeDescriptors by type name (and optionally
// extension). Format: "normalized_type_name." or
// "normalized_type_name.normalized_ext_name."
struct TypeDescriptorKeyPrefix {
 public:
  // Query by type name only
  explicit TypeDescriptorKeyPrefix(std::string_view type_name)
      : type_name_(type_name),
        normalized_prefix_(normalize_type_name(type_name_) + ".") {}

  // Query by type name + optional extension name
  TypeDescriptorKeyPrefix(std::string_view type_name,
                          std::string_view extension_name)
      : type_name_(type_name),
        extension_name_(extension_name),
        normalized_prefix_(
            normalize_type_name(type_name_) + "." +
            (extension_name_.empty()
                 ? ""
                 : normalize_extension_name(extension_name_) + ".")) {}

  const std::string &str() const { return normalized_prefix_; }

  bool matches_key(const std::string &key) const {
    return key.size() >= normalized_prefix_.size() &&
           key.compare(0, normalized_prefix_.size(), normalized_prefix_) == 0;
  }

  const std::string &type_name() const { return type_name_; }
  const std::string &extension_name() const { return extension_name_; }

 private:
  std::string type_name_;
  std::string extension_name_;
  std::string normalized_prefix_;
};

// Key for TypeDescriptor entries in the VictionaryClient map.
// Format: "normalized_type_name.normalized_extension_name.normalized_version"
// Stores original component values for display, plus normalized key for
// lookups.
struct TypeDescriptorKey {
 public:
  TypeDescriptorKey() = default;

  TypeDescriptorKey(std::string type_name, std::string extension_name,
                    std::string extension_version);

  const std::string &str() const { return normalized_key_; }

  // Component accessors (return original values)
  const std::string &type_name() const { return type_name_; }
  const std::string &extension_name() const { return extension_name_; }
  const std::string &extension_version() const { return extension_version_; }

  bool operator<(const TypeDescriptorKey &other) const {
    return normalized_key_ < other.normalized_key_;
  }
  bool operator==(const TypeDescriptorKey &other) const {
    return normalized_key_ == other.normalized_key_;
  }

 private:
  std::string type_name_;
  std::string extension_name_;
  std::string extension_version_;
  std::string normalized_key_;
};

// TypeDescriptor: Immutable in-memory descriptor for a custom type.
// Built programmatically from extension registration, not from table rows.
// Holds TypeFunction instances (EncodeFunction, DecodeFunction, etc.) that
// store the raw callable (function pointer or VDF descriptor). These serve as
// templates for building bound Ops in TypeContext.
class TypeDescriptor {
 public:
  using key_type = TypeDescriptorKey;
  using key_prefix_type = TypeDescriptorKeyPrefix;

  // Default constructor - creates an empty/invalid descriptor
  // Required for use with SystemTableMap's PendingOperation
  TypeDescriptor() = default;

  // Construct with key only, other fields can be set separately (useful for
  // testing)
  explicit TypeDescriptor(TypeDescriptorKey key) : key_(std::move(key)) {}

  // Full constructor. hash, int_to_params, and resolve_params are optional.
  // Storage is wired in separately via set_storage_intf() after all types
  // and column storage descriptors are loaded.
  TypeDescriptor(
      TypeDescriptorKey key, vef_protocol_t protocol, unsigned char impl_type,
      int64_t persisted_len, int64_t max_unpersisted_len,
      int64_t max_persisted_len, EncodeFunction encode, DecodeFunction decode,
      CompareFunction compare, std::optional<HashFunction> hash = std::nullopt,
      std::optional<IntToParamsFunction> int_to_params = std::nullopt,
      std::optional<ResolveParamsFunction> resolve_params = std::nullopt);

  // Disable copy (descriptors should not be copied)
  TypeDescriptor(const TypeDescriptor &) = delete;
  TypeDescriptor &operator=(const TypeDescriptor &) = delete;

  // Enable move (needed for SystemTableMap storage)
  TypeDescriptor(TypeDescriptor &&) = default;
  TypeDescriptor &operator=(TypeDescriptor &&) = default;

  ~TypeDescriptor() = default;

  // Key accessor (required by SystemTableMap)
  const TypeDescriptorKey &key() const { return key_; }

  // Accessors for key components (delegate to key)
  const std::string &type_name() const { return key_.type_name(); }
  const std::string &extension_name() const { return key_.extension_name(); }
  const std::string &extension_version() const {
    return key_.extension_version();
  }

  // Returns the qualified base name: "extension_name.type_name"
  // Does not include parameters. Use TypeContext::qualified_name() for the
  // full name including parameters (e.g. "vsql_tvector.TVECTOR(3)").
  std::string qualified_base_name() const {
    return make_qualified_base_name(extension_name(), type_name());
  }

  // Protocol version this type was registered with.
  vef_protocol_t protocol() const { return protocol_; }

  // Type implementation details
  unsigned char implementation_type() const { return implementation_type_; }
  int64_t persisted_length() const { return persisted_length_; }
  int64_t max_decode_buffer_length() const { return max_decode_buffer_length_; }
  // Upper bound on persisted_length across all valid parameterizations.
  // 0 for non-parameterized types (use persisted_length()) and for
  // parameterized types that have not declared the upper bound yet (these
  // will not be eligible for fix_fields constant-string inference).
  int64_t max_persisted_length() const { return max_persisted_length_; }

  // TypeFunction accessors.
  // encode_fn, decode_fn, compare_fn assert that the function is set
  // (required). hash_fn returns std::nullopt if no custom hash is registered.
  bool has_encode_fn() const { return encode_fn_.has_value(); }
  bool has_decode_fn() const { return decode_fn_.has_value(); }
  bool has_compare_fn() const { return compare_fn_.has_value(); }
  const EncodeFunction &encode_fn() const {
    assert(encode_fn_.has_value());
    return *encode_fn_;
  }
  const DecodeFunction &decode_fn() const {
    assert(decode_fn_.has_value());
    return *decode_fn_;
  }
  const CompareFunction &compare_fn() const {
    assert(compare_fn_.has_value());
    return *compare_fn_;
  }
  const std::optional<HashFunction> &hash_fn() const { return hash_fn_; }

  const std::optional<IntToParamsFunction> &int_to_params_fn() const {
    return int_to_params_fn_;
  }
  const std::optional<ResolveParamsFunction> &resolve_params_fn() const {
    return resolve_params_fn_;
  }

  // Returns true if this type accepts parameters (i.e., has a resolve_params
  // callback). Types like VECTOR(N) are parameterized; types like COMPLEX
  // are not.
  bool is_parameterized() const { return resolve_params_fn_.has_value(); }

  // Returns the intrinsic default function, or nullopt if not set.
  const std::optional<IntrinsicDefaultFunction> &intrinsic_default_fn() const {
    return intrinsic_default_fn_;
  }

  // Set the intrinsic default function after construction (used during
  // type registration).
  void set_intrinsic_default_fn(IntrinsicDefaultFunction fn) {
    intrinsic_default_fn_ = std::move(fn);
  }

  // Returns the intrinsic default string literal, or nullopt if not set.
  const std::optional<std::string> &intrinsic_default_str() const {
    return intrinsic_default_str_;
  }

  // Set the intrinsic default string literal after construction (used during
  // type registration).
  void set_intrinsic_default_str(std::string str) {
    intrinsic_default_str_ = std::move(str);
  }

  // Returns the storage interface for this type, or nullopt if the type does
  // not manage its own column storage.
  const std::optional<StorageInterface> &storage_intf() const {
    return storage_intf_;
  }

  // Wire in column storage after construction (called during extension loading
  // once all types and storage descriptors have been loaded).
  void set_storage_intf(StorageInterface intf) {
    storage_intf_ = std::move(intf);
  }

 private:
  TypeDescriptorKey key_;

  vef_protocol_t protocol_{VEF_PROTOCOL_1};

  // Type implementation details
  unsigned char implementation_type_{0};
  int64_t persisted_length_{0};
  int64_t max_decode_buffer_length_{0};
  int64_t max_persisted_length_{0};

  // Type functions (encode/decode/compare required; hash optional)
  std::optional<EncodeFunction> encode_fn_;
  std::optional<DecodeFunction> decode_fn_;
  std::optional<CompareFunction> compare_fn_;
  std::optional<HashFunction> hash_fn_;

  std::optional<IntToParamsFunction> int_to_params_fn_;
  std::optional<ResolveParamsFunction> resolve_params_fn_;

  std::optional<IntrinsicDefaultFunction> intrinsic_default_fn_;
  std::optional<std::string> intrinsic_default_str_;

  // Storage interface provided by the extension for managing column storage.
  // Empty if the type uses default InnoDB column storage.
  std::optional<StorageInterface> storage_intf_;
};

// TableTraits specialization for TypeDescriptor.
// Empty because TypeDescriptor doesn't have table-backed operations.
template <>
struct TableTraits<TypeDescriptor> {};

}  // namespace villagesql

#endif  // VILLAGESQL_SCHEMA_DESCRIPTOR_TYPE_DESCRIPTOR_H_
