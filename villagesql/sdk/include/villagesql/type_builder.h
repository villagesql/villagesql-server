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
#include <villagesql/type_params_cache.h>

namespace villagesql {
namespace type_builder {

template <typename P, auto ParseFunc>
void bind_params_cache() {
  villagesql::type_params_cache_for<P>().bind(ParseFunc);
}

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
//     .encode("mytype_encode")
//     .decode("mytype_decode")
//     .compare("mytype_compare")
//     .hash("mytype_hash")   // optional
//     .build()
//
// TODO(villagesql-beta): allow VDFs that aren't directly callable from SQL.

// Wrapper around the ABI type descriptor that owns the storage interface
// structure and ensures vef_desc.storage_intf points to the local copy
// after copy/move operations.
struct TypeDescriptor {
  vef_type_desc_t vef_desc{};
  vef_type_storage_intf_t storage_intf{};
  void (*params_init_fn)() = nullptr;

  constexpr TypeDescriptor() = default;

  constexpr TypeDescriptor(const TypeDescriptor &o)
      : vef_desc(o.vef_desc),
        storage_intf(o.storage_intf),
        params_init_fn(o.params_init_fn) {
    if (storage_intf.version != 0) vef_desc.storage_intf = &storage_intf;
  }

  constexpr TypeDescriptor(TypeDescriptor &&o)
      : vef_desc(o.vef_desc),
        storage_intf(o.storage_intf),
        params_init_fn(o.params_init_fn) {
    if (storage_intf.version != 0) vef_desc.storage_intf = &storage_intf;
  }

  constexpr TypeDescriptor &operator=(const TypeDescriptor &o) {
    vef_desc = o.vef_desc;
    storage_intf = o.storage_intf;
    params_init_fn = o.params_init_fn;
    if (storage_intf.version != 0) vef_desc.storage_intf = &storage_intf;
    return *this;
  }
};

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
        encode_vdf_name_(nullptr),
        decode_vdf_name_(nullptr),
        compare_vdf_name_(nullptr),
        hash_vdf_name_(nullptr),
        int_to_params_vdf_name_(nullptr),
        resolve_params_vdf_name_(nullptr),
        intrinsic_default_vdf_name_(nullptr),
        storage_intf_{},
        params_init_fn_(nullptr) {}

  constexpr TypeBuilder &persisted_length(int64_t len) {
    persisted_length_ = len;
    return *this;
  }

  constexpr TypeBuilder &max_decode_buffer_length(int64_t len) {
    max_decode_buffer_length_ = len;
    return *this;
  }

  constexpr TypeBuilder &encode(vef_encode_func_t f) {
    encode_ = f;
    return *this;
  }

  constexpr TypeBuilder &encode(const char *vdf_name) {
    encode_vdf_name_ = vdf_name;
    return *this;
  }

  constexpr TypeBuilder &decode(vef_decode_func_t f) {
    decode_ = f;
    return *this;
  }

  constexpr TypeBuilder &decode(const char *vdf_name) {
    decode_vdf_name_ = vdf_name;
    return *this;
  }

  constexpr TypeBuilder &compare(vef_compare_func_t f) {
    compare_ = f;
    return *this;
  }

  constexpr TypeBuilder &compare(const char *vdf_name) {
    compare_vdf_name_ = vdf_name;
    return *this;
  }

  constexpr TypeBuilder &hash(vef_hash_func_t f) {
    hash_ = f;
    return *this;
  }

  constexpr TypeBuilder &hash(const char *vdf_name) {
    hash_vdf_name_ = vdf_name;
    return *this;
  }

  constexpr TypeBuilder &intrinsic_default(const char *vdf_name) {
    intrinsic_default_vdf_name_ = vdf_name;
    return *this;
  }

  constexpr TypeBuilder &int_to_params(const char *vdf_name) {
    int_to_params_vdf_name_ = vdf_name;
    return *this;
  }

  constexpr TypeBuilder &resolve_params(const char *vdf_name) {
    resolve_params_vdf_name_ = vdf_name;
    return *this;
  }

  constexpr TypeBuilder &column_storage(const vef_type_storage_intf_t &intf) {
    storage_intf_ = intf;
    return *this;
  }

  // Registers the params type P and its parse function with the
  // TypeParamsCache. Called once during vef_register() before any VDF
  // invocations. After this, VDF wrappers and CustomArgWith<P> can call
  // type_params_cache_for<P>().get(raw) without passing the parse function.
  template <typename P, auto ParseFunc>
  constexpr TypeBuilder &params() {
    params_init_fn_ = &bind_params_cache<P, ParseFunc>;
    return *this;
  }

  // Build the final vef_type_desc_t. Protocol is set automatically:
  // VEF_PROTOCOL_2 if any protocol-2 field is set, otherwise VEF_PROTOCOL_1.
  // ExtensionBuilder::type() propagates this up to the extension's
  // min_protocol, so the registration fails if the server offers a lower
  // protocol.
  constexpr TypeDescriptor build() const {
    const bool needs_v2 =
        encode_vdf_name_ != nullptr || decode_vdf_name_ != nullptr ||
        compare_vdf_name_ != nullptr || hash_vdf_name_ != nullptr ||
        int_to_params_vdf_name_ != nullptr ||
        resolve_params_vdf_name_ != nullptr ||
        intrinsic_default_vdf_name_ != nullptr || storage_intf_.version != 0;
    const vef_protocol_t protocol = needs_v2 ? VEF_PROTOCOL_2 : VEF_PROTOCOL_1;
    TypeDescriptor desc{};
    desc.storage_intf = storage_intf_;
    desc.vef_desc = vef_type_desc_t{
        protocol,
        name_,
        persisted_length_,
        max_decode_buffer_length_,
        encode_,
        decode_,
        compare_,
        hash_,
        encode_vdf_name_,
        decode_vdf_name_,
        compare_vdf_name_,
        hash_vdf_name_,
        int_to_params_vdf_name_,
        resolve_params_vdf_name_,
        intrinsic_default_vdf_name_,
        storage_intf_.version != 0 ? &desc.storage_intf : nullptr,
    };
    desc.params_init_fn = params_init_fn_;
    return desc;
  }

 private:
  const char *name_;
  int64_t persisted_length_;
  int64_t max_decode_buffer_length_;
  vef_encode_func_t encode_;
  vef_decode_func_t decode_;
  vef_compare_func_t compare_;
  vef_hash_func_t hash_;
  const char *encode_vdf_name_;
  const char *decode_vdf_name_;
  const char *compare_vdf_name_;
  const char *hash_vdf_name_;
  const char *int_to_params_vdf_name_;
  const char *resolve_params_vdf_name_;
  const char *intrinsic_default_vdf_name_;
  vef_type_storage_intf_t storage_intf_;
  void (*params_init_fn_)();
};

// Entry point: make_type("name")
constexpr TypeBuilder make_type(const char *name) { return TypeBuilder(name); }

}  // namespace type_builder
}  // namespace villagesql

#endif  // VILLAGESQL_SDK_TYPE_BUILDER_H
