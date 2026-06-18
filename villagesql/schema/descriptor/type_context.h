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
#include <string_view>
#include <vector>

#include "villagesql/include/error.h"
#include "villagesql/schema/descriptor/type_descriptor.h"
#include "villagesql/types/type_op.h"

struct MEM_ROOT;

// Forward declaration for test access to the private TypeContext constructor.
namespace villagesql_unittest {
class TypeContextTest;
}  // namespace villagesql_unittest

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
  explicit TypeParameters(std::string canonical) : str_(std::move(canonical)) {
    build_entries();
  }

  TypeParameters(const TypeParameters &other)
      : str_(other.str_), keys_(other.keys_), values_(other.values_) {
    rebuild_c_ptrs();
  }
  TypeParameters &operator=(const TypeParameters &other) {
    if (this != &other) {
      str_ = other.str_;
      keys_ = other.keys_;
      values_ = other.values_;
      rebuild_c_ptrs();
    }
    return *this;
  }
  TypeParameters(TypeParameters &&other) noexcept
      : str_(std::move(other.str_)),
        keys_(std::move(other.keys_)),
        values_(std::move(other.values_)) {
    rebuild_c_ptrs();
  }
  TypeParameters &operator=(TypeParameters &&other) noexcept {
    if (this != &other) {
      str_ = std::move(other.str_);
      keys_ = std::move(other.keys_);
      values_ = std::move(other.values_);
      rebuild_c_ptrs();
    }
    return *this;
  }

  // Normalize a raw "k=v,k=v,..." string: split pairs, sort by lowercased
  // key, lowercase values, re-serialize. Used by TYPE('k=v,...') SQL parser
  // path.
  static TypeParameters from_raw(const std::string_view raw);

  bool empty() const { return str_.empty(); }
  const std::string &str() const { return str_; }

  // ABI accessors: parallel key/value arrays for vef_type_params_t.
  // keys and values are in the same order (keys[i] pairs with values[i]),
  // sorted alphabetically by key.
  unsigned int count() const { return static_cast<unsigned int>(keys_.size()); }
  const char *const *key_data() const {
    return c_keys_.empty() ? nullptr : c_keys_.data();
  }
  const char *const *value_data() const {
    return c_values_.empty() ? nullptr : c_values_.data();
  }

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
  void build_entries();
  void rebuild_c_ptrs() {
    c_keys_.clear();
    c_values_.clear();
    c_keys_.reserve(keys_.size());
    for (const auto &k : keys_) c_keys_.push_back(k.c_str());
    c_values_.reserve(values_.size());
    for (const auto &v : values_) c_values_.push_back(v.c_str());
  }

  // The canonical string representation of the key/value pairs
  std::string str_;

  // Pre-parsed parallel key/value arrays (sorted by key).
  // We own the strings and keep const char* vectors for the ABI.
  std::vector<std::string> keys_;
  std::vector<std::string> values_;
  std::vector<const char *> c_keys_;
  std::vector<const char *> c_values_;
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
template <typename EntryType>
struct TableTraits;

class TypeContext {
 public:
  using key_type = TypeContextKey;

  TypeContext() = delete;

  // Disable copy
  TypeContext(const TypeContext &) = delete;
  TypeContext &operator=(const TypeContext &) = delete;

  // Allow move construction (needed for SystemTableMap storage).
  // Move assignment is deleted because Op members hold references to our
  // TypeParameters (which lives inside key_).
  TypeContext(TypeContext &&) = default;
  TypeContext &operator=(TypeContext &&) = delete;

  ~TypeContext() = default;

  // ===== Accessors =====

  const TypeContextKey &key() const { return key_; }
  const TypeParameters &parameters() const { return key_.parameters(); }
  const TypeDescriptor *descriptor() const { return descriptor_; }

  // Returns true when this TypeContext represents a parameterized type whose
  // parameters have not yet been resolved. A non-parameterized type with empty
  // parameters (e.g. COMPLEX) is fully known and returns false.
  bool is_unknown() const { return is_parameterized() && parameters().empty(); }

  // Types are compatible if they have the same key (type name, extension,
  // version, and parameters). This ensures e.g. TVECTOR(3) != TVECTOR(4).
  bool is_compatible_with(const TypeContext &other) const {
    return key() == other.key();
  }

  // This is assignable with other if they are either compatible with each
  // other, or this is unknown and other is known (of the same base type). So
  // TVECTOR is assignable with TVECTOR(3), but not the other way.
  bool is_assignable_with(const TypeContext &other) const {
    return is_compatible_with(other) ||
           (is_unknown() && !other.is_unknown() &&
            key().descriptor_key() == other.key().descriptor_key());
  }

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

  // True if the underlying type is variable-length (size decided per value).
  // Delegates to the descriptor's registration-time marker.
  bool is_variable_length() const {
    return descriptor_->length_kind() == LengthKind::Variable;
  }

  // True if this type accepts parameters (has a resolve_params callback),
  // independent of whether it is fixed- or variable-length.
  bool is_parameterized() const { return descriptor_->is_parameterized(); }

  // Declared length of the backing field for this type instantiation.
  // Fixed-length and parameter-resolved types store exactly persisted_length
  // bytes. A variable-length type length is decided per value, and the backing
  // field is sized to the type's max_persisted_length upper bound.
  int64_t field_buffer_length() const {
    return is_variable_length() ? descriptor_->max_persisted_length()
                                : persisted_length_;
  }

  // Bound type operations. These combine the TypeFunction from the descriptor
  // with this context's TypeParameters.
  // encode_op, decode_op, compare_op assert that the op is set (required ops).
  const EncodeOp &encode_op() const {
    assert(encode_op_.has_value());
    return *encode_op_;
  }
  const DecodeOp &decode_op() const {
    assert(decode_op_.has_value());
    return *decode_op_;
  }
  const CompareOp &compare_op() const {
    assert(compare_op_.has_value());
    return *compare_op_;
  }
  const std::optional<HashOp> &hash_op() const { return hash_op_; }

  // Returns the storage interface for this type, or nullopt if the type uses
  // default InnoDB column storage.
  const std::optional<StorageInterface> &storage_intf() const {
    return descriptor_->storage_intf();
  }

  // Get cached intrinsic default buffer, or nullptr if not set.
  // Protocol-1 types never populate this buffer; callers must check for
  // nullptr.
  const unsigned char *intrinsic_default_buffer() const {
    if (intrinsic_default_buffer_.empty()) return nullptr;
    return intrinsic_default_buffer_.data();
  }

  // Get the size of the intrinsic default buffer.
  size_t intrinsic_default_size() const { return intrinsic_default_size_; }

 private:
  friend struct TableTraits<TypeContext>;
  friend class villagesql_unittest::TypeContextTest;

  // Construct a TypeContext. Use TableTraits<TypeContext>::create() instead.
  // The TypeDescriptor must have been obtained under the victionary lock.
  TypeContext(const TypeContextKey &key, const TypeDescriptor *descriptor);

  // Pre-encode the intrinsic default value. Called once by
  // TableTraits<TypeContext>::create() after construction. Returns true on
  // failure. Sources tried in order: (1) intrinsic_default_fn,
  // (2) intrinsic_default_str, (3) encode(""). Skipped for variable-length
  // types where persisted_length_ <= 0 (no storage size known yet — these
  // types require parameters before use).
  bool init_intrinsic_default(std::string &error_out);

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

  // Bound type operations (constructed from TypeFunctions + TypeParameters).
  // Optional because key-only TypeDescriptors (used in tests) may lack
  // functions.
  std::optional<EncodeOp> encode_op_;
  std::optional<DecodeOp> decode_op_;
  std::optional<CompareOp> compare_op_;
  std::optional<HashOp> hash_op_;

  // Cached intrinsic default value, pre-encoded during construction.
  // Empty if the type has no intrinsic_default_fn or encoding failed.
  std::vector<unsigned char> intrinsic_default_buffer_;
  size_t intrinsic_default_size_{0};
};

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
    // Use new directly: make_shared constructs via the allocator which doesn't
    // have friend access to the private constructor.
    std::shared_ptr<TypeContext> tc(new TypeContext(key, descriptor));
    std::string error;
    if (tc->init_intrinsic_default(error)) {
      if (!error.empty()) {
        villagesql_error("Type '%s' failed to initialize: %s", MYF(0),
                         tc->qualified_name().c_str(), error.c_str());
      }
      return std::shared_ptr<TypeContext>();
    }
    return tc;
  }
};

}  // namespace villagesql

#endif  // VILLAGESQL_SCHEMA_DESCRIPTOR_TYPE_CONTEXT_H_
