// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, see <https://www.gnu.org/licenses/>.

#ifndef VILLAGESQL_SDK_TYPE_BUILDER_H
#define VILLAGESQL_SDK_TYPE_BUILDER_H

// This file provides the underlying templates for type definition.
// For full documentation and examples, see extension.h.

#include <villagesql/abi/types.h>

namespace villagesql {
namespace type_builder {

// =============================================================================
// TypeBuilder
// =============================================================================
//
// Fluent API for creating vef_type_desc_t. Protocol versioning is automatic.
//
// Usage:
//   make_type("mytype")
//     .persisted_length(8)
//     .max_decode_buffer_length(64)
//     .encode(&my_encode)
//     .decode(&my_decode)
//     .compare(&my_compare)
//     .hash(&my_hash)   // optional
//     .build()
//
// TODO(villagesql-beta): add type safe registration methods for the type
// operations.
// TODO(villagesql-beta): allow VDFs that aren't directly callable from SQL.

class TypeBuilder {
 public:
  constexpr explicit TypeBuilder(const char *name)
      : name_(name),
        persisted_length_(0),
        max_decode_buffer_length_(0),
        encode_(nullptr),
        decode_(nullptr),
        compare_(nullptr),
        hash_(nullptr),
        int_to_params_(nullptr),
        resolve_params_(nullptr),
        encode_vdf_name_(nullptr),
        decode_vdf_name_(nullptr),
        compare_vdf_name_(nullptr),
        hash_vdf_name_(nullptr) {}

  constexpr TypeBuilder persisted_length(int64_t len) const {
    TypeBuilder copy = *this;
    copy.persisted_length_ = len;
    return copy;
  }

  constexpr TypeBuilder max_decode_buffer_length(int64_t len) const {
    TypeBuilder copy = *this;
    copy.max_decode_buffer_length_ = len;
    return copy;
  }

  constexpr TypeBuilder encode(vef_encode_func_t f) const {
    TypeBuilder copy = *this;
    copy.encode_ = f;
    return copy;
  }

  constexpr TypeBuilder encode(const char *vdf_name) const {
    TypeBuilder copy = *this;
    copy.encode_vdf_name_ = vdf_name;
    return copy;
  }

  constexpr TypeBuilder decode(vef_decode_func_t f) const {
    TypeBuilder copy = *this;
    copy.decode_ = f;
    return copy;
  }

  constexpr TypeBuilder decode(const char *vdf_name) const {
    TypeBuilder copy = *this;
    copy.decode_vdf_name_ = vdf_name;
    return copy;
  }

  constexpr TypeBuilder compare(vef_compare_func_t f) const {
    TypeBuilder copy = *this;
    copy.compare_ = f;
    return copy;
  }

  constexpr TypeBuilder compare(const char *vdf_name) const {
    TypeBuilder copy = *this;
    copy.compare_vdf_name_ = vdf_name;
    return copy;
  }

  constexpr TypeBuilder hash(vef_hash_func_t f) const {
    TypeBuilder copy = *this;
    copy.hash_ = f;
    return copy;
  }

  constexpr TypeBuilder hash(const char *vdf_name) const {
    TypeBuilder copy = *this;
    copy.hash_vdf_name_ = vdf_name;
    return copy;
  }

  constexpr TypeBuilder int_to_params(vef_type_int_to_params_func_t f) const {
    TypeBuilder copy = *this;
    copy.int_to_params_ = f;
    return copy;
  }

  constexpr TypeBuilder resolve_params(vef_type_resolve_params_func_t f) const {
    TypeBuilder copy = *this;
    copy.resolve_params_ = f;
    return copy;
  }

  // Build the final vef_type_desc_t. Protocol is set automatically:
  // VEF_PROTOCOL_2 if any protocol-2 field is set, otherwise VEF_PROTOCOL_1.
  // ExtensionBuilder::type() propagates this up to the extension's
  // min_protocol, so the registration fails if the server offers a lower
  // protocol.
  constexpr vef_type_desc_t build() const {
    const bool needs_v2 =
        int_to_params_ != nullptr || resolve_params_ != nullptr ||
        encode_vdf_name_ != nullptr || decode_vdf_name_ != nullptr ||
        compare_vdf_name_ != nullptr || hash_vdf_name_ != nullptr;
    const vef_protocol_t protocol = needs_v2 ? VEF_PROTOCOL_2 : VEF_PROTOCOL_1;
    return vef_type_desc_t{
        protocol,          name_,
        persisted_length_, max_decode_buffer_length_,
        encode_,           decode_,
        compare_,          hash_,
        int_to_params_,    resolve_params_,
        encode_vdf_name_,  decode_vdf_name_,
        compare_vdf_name_, hash_vdf_name_,
    };
  }

 private:
  const char *name_;
  int64_t persisted_length_;
  int64_t max_decode_buffer_length_;
  vef_encode_func_t encode_;
  vef_decode_func_t decode_;
  vef_compare_func_t compare_;
  vef_hash_func_t hash_;
  vef_type_int_to_params_func_t int_to_params_;
  vef_type_resolve_params_func_t resolve_params_;
  const char *encode_vdf_name_;
  const char *decode_vdf_name_;
  const char *compare_vdf_name_;
  const char *hash_vdf_name_;
};

// Entry point: make_type("name")
constexpr TypeBuilder make_type(const char *name) { return TypeBuilder(name); }

}  // namespace type_builder
}  // namespace villagesql

#endif  // VILLAGESQL_SDK_TYPE_BUILDER_H
