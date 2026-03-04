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

#ifndef VILLAGESQL_SCHEMA_DESCRIPTOR_TYPE_CONTEXT_H_
#define VILLAGESQL_SCHEMA_DESCRIPTOR_TYPE_CONTEXT_H_

#include <algorithm>
#include <cassert>
#include <cctype>
#include <memory>
#include <string>
#include <vector>

#include "villagesql/schema/descriptor/type_descriptor.h"

struct MEM_ROOT;

namespace villagesql {

struct ColumnEntry;

// TypeParameters holds the concrete instantiation parameters for a custom type
// as a canonical "key1=value1,key2=value2,..." string. The server treats this
// as an opaque string; only the extension's resolve_params callback interprets
// the key/value pairs.
//
// Canonical form: keys sorted alphabetically, all lowercased.
// Equality is just string comparison.
//
// Examples:
//   - COMPLEX with no parameters: empty string
//   - VECTOR(1536): "dimension=1536"
//   - VECTOR(1536, cosine): "dimension=1536,metric=cosine"
class TypeParameters {
 public:
  TypeParameters() = default;
  explicit TypeParameters(std::string canonical) : str_(std::move(canonical)) {}

  // Normalize a raw "k=v,k=v,..." string: split pairs, sort by lowercased
  // key, lowercase values, re-serialize. Used by TYPE('k=v,...') SQL parser
  // path.
  static TypeParameters from_raw(const std::string &raw);

  bool empty() const { return str_.empty(); }
  const std::string &str() const { return str_; }

  // JSON serialization for storage in the villagesql.custom_columns table
  // (which has a JSON column for type_parameters).
  // to_json(): "" → "{}", "k=v" → {"k":"v"}, "a=1,b=2" → {"a":"1","b":"2"}
  std::string to_json() const;
  // from_json(): inverse of to_json()
  static TypeParameters from_json(const std::string &json);

  bool operator==(const TypeParameters &other) const {
    return str_ == other.str_;
  }
  bool operator<(const TypeParameters &other) const {
    return str_ < other.str_;
  }

 private:
  std::string str_;
};

// Key for TypeContext entries in the VictionaryClient map.
// Combines the TypeDescriptorKey (type + extension + version) with
// TypeParameters to uniquely identify a concrete type instantiation.
// For example: VECTOR(1536) and VECTOR(3) would have the same TypeDescriptorKey
// but different parameters, thus different TypeContextKeys.
struct TypeContextKey {
 public:
  TypeContextKey() = default;

  TypeContextKey(TypeDescriptorKey desc_key, TypeParameters params)
      : descriptor_key_(std::move(desc_key)), parameters_(std::move(params)) {
    // Build the combined normalized key
    // Format: "descriptor_key.parameters_str" (or just "descriptor_key" if
    // params empty)
    if (parameters_.str().empty()) {
      normalized_key_ = descriptor_key_.str();
    } else {
      normalized_key_ = descriptor_key_.str() + "." + parameters_.str();
    }
  }

  // Convenience constructor from individual components
  TypeContextKey(std::string type_name, std::string extension_name,
                 std::string extension_version,
                 TypeParameters params = TypeParameters())
      : TypeContextKey(
            TypeDescriptorKey(std::move(type_name), std::move(extension_name),
                              std::move(extension_version)),
            std::move(params)) {}

  const std::string &str() const { return normalized_key_; }
  const TypeDescriptorKey &descriptor_key() const { return descriptor_key_; }
  const TypeParameters &parameters() const { return parameters_; }

  bool operator<(const TypeContextKey &other) const {
    return normalized_key_ < other.normalized_key_;
  }
  bool operator==(const TypeContextKey &other) const {
    return normalized_key_ == other.normalized_key_;
  }

 private:
  TypeDescriptorKey descriptor_key_;
  TypeParameters parameters_;
  std::string normalized_key_;
};

// TypeContext represents a concrete type, it contains:
// - Type Descriptor (metadata for this type)
// - Type Parameters (values for the instantiated type, e.g. dimension of a
//   vector)
//
// All uses of a type should use the TypeContext to ensure
// the type can be instantiated.
//
// TypeContext instances are shared and cached in the VictionaryClient
//
// Note: it is safe to hand out pointers to a TypeContext for the duration of a
// query, so long as it was acquired and tied to the lifetime of the query's
// memroot. If a longer reference guarantee is needed, then the TypeContext
// should be acquired with a longer-living memroot. We are assuming here that
// when invalidating a VictionaryClient entry, we grab an exclusive MDL lock
// that would be blocked by queries that are in-flight (that grab shared MDL
// locks). This is similar to how MySQL guarantees TABLE_SHARE pointers are
// valid for the lifetime of a query and freely copy these pointers to every
// TABLE struct created as part of the execution.
class TypeContext {
 public:
  using key_type = TypeContextKey;

  // Construct a TypeContext by using:
  // - the TypeContextKey, which has a TypeDescriptorKey and TypeParameters
  // - the TypeDescriptor from the victionary, which must have been obtained
  //   under the victionary lock and not via acquire.
  // Note: this must be constructed under the victionary lock
  TypeContext(const TypeContextKey &key, const TypeDescriptor *descriptor)
      : descriptor_(descriptor), key_(key) {
    assert(descriptor);
    assert(descriptor->key() == key.descriptor_key());
    resolve_cached_values();

    // Pre-encode the intrinsic default value using the type's
    // intrinsic_default_fn, if registered. This avoids repeated allocations and
    // encoding every time we need to store an intrinsic default in a NOT NULL
    // custom field.
    if (persisted_length_ > 0 &&
        descriptor_->intrinsic_default_op().has_value()) {
      const size_t storage_size = static_cast<size_t>(persisted_length_);
      std::vector<unsigned char> buffer(storage_size);

      char error_msg[VEF_MAX_ERROR_LEN] = {};
      size_t encoded_length = 0;
      bool encode_failed = descriptor_->intrinsic_default_op()->invoke(
          persisted_length_, buffer.data(), &encoded_length, error_msg);

      if (!encode_failed && encoded_length == storage_size) {
        intrinsic_default_buffer_ = std::move(buffer);
        intrinsic_default_size_ = encoded_length;
      }
      // If encoding failed, leave intrinsic_default_buffer_ empty.
    }
  }

  TypeContext() = delete;

  // Disable copy
  TypeContext(const TypeContext &) = delete;
  TypeContext &operator=(const TypeContext &) = delete;

  // Allow move
  TypeContext(TypeContext &&) = default;
  TypeContext &operator=(TypeContext &&) = default;

  ~TypeContext() = default;

  // ===== Accessors =====

  const TypeContextKey &key() const { return key_; }
  const TypeParameters &parameters() const { return key_.parameters(); }
  const TypeDescriptor *descriptor() const { return descriptor_; }

  // Convenience accessors for frequently used fields
  const std::string &extension_name() const {
    return descriptor_->extension_name();
  }
  const std::string &extension_version() const {
    return descriptor_->extension_version();
  }
  const std::string &type_name() const { return descriptor_->type_name(); }
  // Returns "extension_name.type_name" or "extension_name.type_name(v1,v2,...)"
  // when parameters are present (e.g. "vsql_tvector.TVECTOR(3)").
  const std::string &qualified_name() const { return qualified_name_; }
  // Returns "extension_name.type_name" without parameters
  const std::string &qualified_base_name() const {
    return qualified_base_name_;
  }

  // Storage characteristics for this type instantiation.
  // For fixed-length types, these are copied from the TypeDescriptor.
  // For variable-length types with parameters, these are computed by calling
  // the descriptor's resolve_params callback at construction time.
  int64_t persisted_length() const { return persisted_length_; }
  int64_t max_decode_buffer_length() const { return max_decode_buffer_length_; }

  // Get cached intrinsic default buffer. Returns nullptr if encoding failed
  // during construction.
  const unsigned char *intrinsic_default_buffer() const {
    return intrinsic_default_buffer_.empty() ? nullptr
                                            : intrinsic_default_buffer_.data();
  }

  // Get the size of the intrinsic default buffer.
  size_t intrinsic_default_size() const { return intrinsic_default_size_; }

 private:
  void resolve_cached_values();

  // Pointer to the TypeDescriptor in VictionaryClient
  // Not owned - protected by ref count inside the Victionary itself, and
  // MDL lock on the extension (to block new references during uninstall).
  const TypeDescriptor *descriptor_{nullptr};

  // Key for this TypeContext (used by ExtensionObjectMap)
  TypeContextKey key_;

  // Cached values (computed eagerly in resolve_cached_values())
  std::string qualified_name_;
  std::string qualified_base_name_;
  int64_t persisted_length_{0};
  int64_t max_decode_buffer_length_{0};

  // Cached intrinsic default value, pre-encoded during construction.
  // Empty if the type has no intrinsic_default_fn or encoding failed.
  std::vector<unsigned char> intrinsic_default_buffer_;
  size_t intrinsic_default_size_{0};
};

// Forward declaration of TableTraits (specialized per entry type)
template <typename EntryType>
struct TableTraits;

// TableTraits specialization for TypeContext
// TypeContext is a MEMORY_ONLY entry type - it's created on-demand rather than
// loaded from a backing table. The create() method is used by
// acquire_or_create().
template <>
struct TableTraits<TypeContext> {
  // Create a new TypeContext from its key and its required dependency (the
  // TypeDescriptor, which must come from the victionary under lock).
  // Called by SystemTableMap::acquire_or_create() when the entry doesn't exist.
  // Returns a shared_ptr to the new entry, or nullptr on error.
  static std::shared_ptr<TypeContext> create(const TypeContextKey &key,
                                             const TypeDescriptor *descriptor) {
    if (!descriptor) return std::shared_ptr<TypeContext>();
    return std::make_shared<TypeContext>(key, descriptor);
  }
};

}  // namespace villagesql

#endif  // VILLAGESQL_SCHEMA_DESCRIPTOR_TYPE_CONTEXT_H_
