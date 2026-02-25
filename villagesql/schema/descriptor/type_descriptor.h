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
#include <string>
#include <utility>

#include "villagesql/schema/systable/helpers.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"

namespace villagesql {

template <typename EntryType>
struct TableTraits;

// Prefix key for querying TypeDescriptors by type name (and optionally
// extension). Format: "normalized_type_name." or
// "normalized_type_name.normalized_ext_name."
struct TypeDescriptorKeyPrefix {
 public:
  // Query by type name only
  explicit TypeDescriptorKeyPrefix(std::string type_name)
      : type_name_(std::move(type_name)),
        normalized_prefix_(normalize_type_name(type_name_) + ".") {}

  // Query by type name + optional extension name
  TypeDescriptorKeyPrefix(std::string type_name, std::string extension_name)
      : type_name_(std::move(type_name)),
        extension_name_(std::move(extension_name)),
        normalized_prefix_(
            normalize_type_name(type_name_) + "." +
            (extension_name_.empty()
                 ? ""
                 : normalize_extension_name(extension_name_) + ".")) {}

  const std::string &str() const { return normalized_prefix_; }

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
// Holds direct function pointers for type operations.
class TypeDescriptor {
 public:
  using key_type = TypeDescriptorKey;
  using key_prefix_type = TypeDescriptorKeyPrefix;

  // Function pointer types for type operations
  // These match the signatures in CustomFunctionContainer for compatibility.

  // Encodes a string representation to binary storage format.
  // Returns false on success, true on error.
  // TODO(villagesql-ga): consolidate all of these typedefs so that they are in
  // one location.
  using EncodeFn = bool (*)(unsigned char *buffer, size_t buffer_size,
                            const char *from, size_t from_len, size_t *length);

  // Decodes binary storage format to string representation.
  // Returns false on success, true on error.
  using DecodeFn = bool (*)(const unsigned char *buffer, size_t buffer_size,
                            char *to, size_t to_buffer_size, size_t *to_length);

  // Compares two binary values. Returns <0, 0, or >0 like strcmp.
  // Comparison is always ascending; DESC is handled by callers.
  using CompareFn = int (*)(const unsigned char *data1, size_t len1,
                            const unsigned char *data2, size_t len2);

  // Optional hash function for deduplication/joins.
  // If nullptr, standard binary hash is used (requires encode to canonicalize
  // equivalent values like -0.0 to +0.0).
  using HashFn = size_t (*)(const unsigned char *data, size_t len);

  // Optional: Convert TYPE(N) integer to parameter key-value pairs.
  // If nullptr, TYPE(N) syntax is not supported for this type.
  using IntToParamsFn = vef_type_int_to_params_func_t;

  // Optional: Validate parameters and compute storage characteristics.
  // If nullptr, the type does not accept parameters.
  using ResolveParamsFn = vef_type_resolve_params_func_t;

  // Default constructor - creates an empty/invalid descriptor
  // Required for use with SystemTableMap's PendingOperation
  TypeDescriptor() = default;

  // Construct with key only, other fields can be set separately (useful for
  // testing)
  explicit TypeDescriptor(TypeDescriptorKey key) : key_(std::move(key)) {}

  // Full constructor with all fields (hash, int_to_params, resolve_params
  // may be nullptr)
  TypeDescriptor(TypeDescriptorKey key, unsigned char impl_type,
                 int64_t persisted_len, int64_t max_unpersisted_len,
                 EncodeFn encode, DecodeFn decode, CompareFn compare,
                 HashFn hash = nullptr, IntToParamsFn int_to_params = nullptr,
                 ResolveParamsFn resolve_params = nullptr);

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
    return extension_name() + "." + type_name();
  }

  // Type implementation details
  unsigned char implementation_type() const { return implementation_type_; }
  int64_t persisted_length() const { return persisted_length_; }
  int64_t max_decode_buffer_length() const { return max_decode_buffer_length_; }

  // Function pointer accessors
  EncodeFn encode() const { return encode_; }
  DecodeFn decode() const { return decode_; }
  CompareFn compare() const { return compare_; }
  HashFn hash() const { return hash_; }  // May be nullptr
  IntToParamsFn int_to_params() const { return int_to_params_; }
  ResolveParamsFn resolve_params() const { return resolve_params_; }

 private:
  TypeDescriptorKey key_;

  // Type implementation details
  unsigned char implementation_type_{0};
  int64_t persisted_length_{0};
  int64_t max_decode_buffer_length_{0};

  // Function pointers (nullptr for default-constructed descriptor)
  EncodeFn encode_{nullptr};
  DecodeFn decode_{nullptr};
  CompareFn compare_{nullptr};
  HashFn hash_{nullptr};
  IntToParamsFn int_to_params_{nullptr};
  ResolveParamsFn resolve_params_{nullptr};
};

// TableTraits specialization for TypeDescriptor.
// Empty because TypeDescriptor doesn't have table-backed operations.
template <>
struct TableTraits<TypeDescriptor> {};

}  // namespace villagesql

#endif  // VILLAGESQL_SCHEMA_DESCRIPTOR_TYPE_DESCRIPTOR_H_
