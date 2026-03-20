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
//
// TypeDescriptorWithParams<P> is the parameterized form used when a type
// registers a params type via .params<P, &fn>(). Code that doesn't care about
// the params type can hold or accept the base TypeDescriptor.
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

// TypeDescriptor parameterized on its params type. P = void means no params.
// Inherits from TypeDescriptor so all existing code accepting TypeDescriptor
// continues to work without changes.
template <typename P = void>
struct TypeDescriptorWithParams : TypeDescriptor {
  using params_type = P;
};

// Non-template storage for TypeBuilder fields. Extracting these into a plain
// struct lets TypeBuilder<P>'s cross-specialization copy constructor delegate
// to the compiler-generated copy instead of listing every field by hand.
struct TypeBuilderData {
  const char *name_;
  int64_t persisted_length_ = 0;
  int64_t max_decode_buffer_length_ = 0;
  vef_encode_func_t encode_ = nullptr;
  vef_decode_func_t decode_ = nullptr;
  vef_compare_func_t compare_ = nullptr;
  vef_hash_func_t hash_ = nullptr;
  const char *encode_vdf_name_ = nullptr;
  const char *decode_vdf_name_ = nullptr;
  const char *compare_vdf_name_ = nullptr;
  const char *hash_vdf_name_ = nullptr;
  const char *int_to_params_vdf_name_ = nullptr;
  const char *resolve_params_vdf_name_ = nullptr;
  const char *intrinsic_default_vdf_name_ = nullptr;
  vef_type_storage_intf_t storage_intf_ = {};
  void (*params_init_fn_)() = nullptr;

  constexpr explicit TypeBuilderData(const char *name) : name_(name) {}
};

template <typename P = void>
class TypeBuilder {
  // Allows TypeBuilder<Q>::params<P2>() to copy data_ into TypeBuilder<P2>.
  template <typename Q>
  friend class TypeBuilder;

 public:
  constexpr explicit TypeBuilder(const char *name) : data_(name) {}

  constexpr TypeBuilder &persisted_length(int64_t len) {
    data_.persisted_length_ = len;
    return *this;
  }

  constexpr TypeBuilder &max_decode_buffer_length(int64_t len) {
    data_.max_decode_buffer_length_ = len;
    return *this;
  }

  constexpr TypeBuilder &encode(vef_encode_func_t f) {
    data_.encode_ = f;
    return *this;
  }

  constexpr TypeBuilder &encode(const char *vdf_name) {
    data_.encode_vdf_name_ = vdf_name;
    return *this;
  }

  constexpr TypeBuilder &decode(vef_decode_func_t f) {
    data_.decode_ = f;
    return *this;
  }

  constexpr TypeBuilder &decode(const char *vdf_name) {
    data_.decode_vdf_name_ = vdf_name;
    return *this;
  }

  constexpr TypeBuilder &compare(vef_compare_func_t f) {
    data_.compare_ = f;
    return *this;
  }

  constexpr TypeBuilder &compare(const char *vdf_name) {
    data_.compare_vdf_name_ = vdf_name;
    return *this;
  }

  constexpr TypeBuilder &hash(vef_hash_func_t f) {
    data_.hash_ = f;
    return *this;
  }

  constexpr TypeBuilder &hash(const char *vdf_name) {
    data_.hash_vdf_name_ = vdf_name;
    return *this;
  }

  constexpr TypeBuilder &intrinsic_default(const char *vdf_name) {
    data_.intrinsic_default_vdf_name_ = vdf_name;
    return *this;
  }

  constexpr TypeBuilder &int_to_params(const char *vdf_name) {
    data_.int_to_params_vdf_name_ = vdf_name;
    return *this;
  }

  constexpr TypeBuilder &resolve_params(const char *vdf_name) {
    data_.resolve_params_vdf_name_ = vdf_name;
    return *this;
  }

  constexpr TypeBuilder &column_storage(const vef_type_storage_intf_t &intf) {
    data_.storage_intf_ = intf;
    return *this;
  }

  // Registers the params type P2 and its parse function. Returns a new
  // TypeBuilder<P2> with all fields copied, so the params type is reflected
  // in the return type of build(). Called once during vef_register() before
  // any VDF invocations.
  template <typename P2, auto ParseFunc>
  constexpr TypeBuilder<P2> params() const {
    TypeBuilder<P2> b(*this);
    b.data_.params_init_fn_ = &bind_params_cache<P2, ParseFunc>;
    return b;
  }

  // Build the final TypeDescriptorWithParams<P>. Protocol is set automatically:
  // VEF_PROTOCOL_2 if any protocol-2 field is set, otherwise VEF_PROTOCOL_1.
  // ExtensionBuilder::type() propagates this up to the extension's
  // min_protocol, so the registration fails if the server offers a lower
  // protocol.
  constexpr TypeDescriptorWithParams<P> build() const {
    const bool needs_v2 = data_.encode_vdf_name_ != nullptr ||
                          data_.decode_vdf_name_ != nullptr ||
                          data_.compare_vdf_name_ != nullptr ||
                          data_.hash_vdf_name_ != nullptr ||
                          data_.int_to_params_vdf_name_ != nullptr ||
                          data_.resolve_params_vdf_name_ != nullptr ||
                          data_.intrinsic_default_vdf_name_ != nullptr ||
                          data_.storage_intf_.version != 0;
    const vef_protocol_t protocol = needs_v2 ? VEF_PROTOCOL_2 : VEF_PROTOCOL_1;
    TypeDescriptorWithParams<P> desc{};
    desc.storage_intf = data_.storage_intf_;
    desc.vef_desc = vef_type_desc_t{
        protocol,
        data_.name_,
        data_.persisted_length_,
        data_.max_decode_buffer_length_,
        data_.encode_,
        data_.decode_,
        data_.compare_,
        data_.hash_,
        data_.encode_vdf_name_,
        data_.decode_vdf_name_,
        data_.compare_vdf_name_,
        data_.hash_vdf_name_,
        data_.int_to_params_vdf_name_,
        data_.resolve_params_vdf_name_,
        data_.intrinsic_default_vdf_name_,
        data_.storage_intf_.version != 0 ? &desc.storage_intf : nullptr,
    };
    desc.params_init_fn = data_.params_init_fn_;
    return desc;
  }

 private:
  // Converting copy constructor used only by params<P2, ParseFunc>() to
  // produce a TypeBuilder<P2> from a TypeBuilder<void>. Restricted to
  // Q = void so that calling .params<>() twice is a compile error.
  template <typename Q>
  constexpr explicit TypeBuilder(const TypeBuilder<Q> &o) : data_(o.data_) {
    static_assert(std::is_void_v<Q>,
                  ".params<P, &fn>() may only be called once on a TypeBuilder");
  }

  TypeBuilderData data_;
};

// Entry point: make_type("name") returns TypeBuilder<void>.
constexpr TypeBuilder<> make_type(const char *name) {
  return TypeBuilder<>(name);
}

}  // namespace type_builder
}  // namespace villagesql

#endif  // VILLAGESQL_SDK_TYPE_BUILDER_H
