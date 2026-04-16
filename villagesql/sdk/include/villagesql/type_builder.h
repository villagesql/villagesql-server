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

// Type descriptor builder.
//
// TypeDescriptor and the VDF-name-based TypeBuilder are used internally by
// the vsql object-based type API (villagesql/vsql/type_builder.h) and are
// also available directly for extensions that reference type operations by
// VDF name string (VEF_PROTOCOL_2) or by raw function pointer (VEF_PROTOCOL_1
// backward compatibility).

#include <villagesql/abi/types.h>

namespace villagesql {
namespace type_builder {

// =============================================================================
// TypeDescriptor
// =============================================================================
//
// Wrapper around vef_type_desc_t that owns the storage interface structure
// and ensures vef_desc.storage_intf points to the local copy after copy/move.

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

// =============================================================================
// TypeBuilder
// =============================================================================
//
// Fluent builder for vef_type_desc_t. Type operations are referenced by VDF
// name (string), not by raw function pointer.
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
// For the object-based API that auto-generates VDF names, see
// villagesql/vsql/type_builder.h (included via villagesql/vsql.h).

class TypeBuilder {
 public:
  constexpr explicit TypeBuilder(const char *name)
      : name_(name),
        persisted_length_(0),
        max_decode_buffer_length_(0),
        encode_func_(nullptr),
        decode_func_(nullptr),
        compare_func_(nullptr),
        hash_func_(nullptr),
        encode_vdf_name_(nullptr),
        decode_vdf_name_(nullptr),
        compare_vdf_name_(nullptr),
        hash_vdf_name_(nullptr),
        int_to_params_vdf_name_(nullptr),
        resolve_params_vdf_name_(nullptr),
        intrinsic_default_vdf_name_(nullptr),
        intrinsic_default_str_(nullptr),
        storage_intf_{} {}

  constexpr TypeBuilder &persisted_length(int64_t len) {
    persisted_length_ = len;
    return *this;
  }

  constexpr TypeBuilder &max_decode_buffer_length(int64_t len) {
    max_decode_buffer_length_ = len;
    return *this;
  }

  constexpr TypeBuilder &encode(const char *vdf_name) {
    encode_vdf_name_ = vdf_name;
    return *this;
  }

  constexpr TypeBuilder &decode(const char *vdf_name) {
    decode_vdf_name_ = vdf_name;
    return *this;
  }

  constexpr TypeBuilder &compare(const char *vdf_name) {
    compare_vdf_name_ = vdf_name;
    return *this;
  }

  constexpr TypeBuilder &hash(const char *vdf_name) {
    hash_vdf_name_ = vdf_name;
    return *this;
  }

  // Raw function pointer overloads — V1 ABI backward compatibility.
  // When used, build() produces VEF_PROTOCOL_1 type registration.
  constexpr TypeBuilder &encode(vef_encode_func_t f) {
    encode_func_ = f;
    return *this;
  }

  constexpr TypeBuilder &decode(vef_decode_func_t f) {
    decode_func_ = f;
    return *this;
  }

  constexpr TypeBuilder &compare(vef_compare_func_t f) {
    compare_func_ = f;
    return *this;
  }

  constexpr TypeBuilder &hash(vef_hash_func_t f) {
    hash_func_ = f;
    return *this;
  }

  constexpr TypeBuilder &intrinsic_default(const char *vdf_name) {
    intrinsic_default_vdf_name_ = vdf_name;
    return *this;
  }

  constexpr TypeBuilder &intrinsic_default_str(const char *str) {
    intrinsic_default_str_ = str;
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

  constexpr TypeDescriptor build() const {
    TypeDescriptor desc{};
    desc.storage_intf = storage_intf_;
    const bool use_raw_ptrs = encode_func_ != nullptr ||
                              decode_func_ != nullptr ||
                              compare_func_ != nullptr;
    desc.vef_desc = vef_type_desc_t{
        use_raw_ptrs ? VEF_PROTOCOL_1 : VEF_PROTOCOL_2,
        name_,
        persisted_length_,
        max_decode_buffer_length_,
        encode_func_,
        decode_func_,
        compare_func_,
        hash_func_,
        use_raw_ptrs ? nullptr : encode_vdf_name_,
        use_raw_ptrs ? nullptr : decode_vdf_name_,
        use_raw_ptrs ? nullptr : compare_vdf_name_,
        use_raw_ptrs ? nullptr : hash_vdf_name_,
        int_to_params_vdf_name_,
        resolve_params_vdf_name_,
        intrinsic_default_vdf_name_,
        intrinsic_default_str_,
        storage_intf_.version != 0 ? &desc.storage_intf : nullptr,
    };
    return desc;
  }

 private:
  const char *name_;
  int64_t persisted_length_;
  int64_t max_decode_buffer_length_;
  vef_encode_func_t encode_func_;
  vef_decode_func_t decode_func_;
  vef_compare_func_t compare_func_;
  vef_hash_func_t hash_func_;
  const char *encode_vdf_name_;
  const char *decode_vdf_name_;
  const char *compare_vdf_name_;
  const char *hash_vdf_name_;
  const char *int_to_params_vdf_name_;
  const char *resolve_params_vdf_name_;
  const char *intrinsic_default_vdf_name_;
  const char *intrinsic_default_str_;
  vef_type_storage_intf_t storage_intf_;
};

// Entry point: make_type("name")
constexpr TypeBuilder make_type(const char *name) { return TypeBuilder(name); }

}  // namespace type_builder
}  // namespace villagesql

#endif  // VILLAGESQL_SDK_TYPE_BUILDER_H
