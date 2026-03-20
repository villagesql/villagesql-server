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

#ifndef VILLAGESQL_SDK_EXTENSION_BUILDER_H
#define VILLAGESQL_SDK_EXTENSION_BUILDER_H

// =============================================================================
// Extension Builder - Registration via Fluent Builder API
// =============================================================================
//
// This file provides the ExtensionBuilder for registering functions and types.
// For the main extension authoring header with full documentation, see
// extension.h instead.
//

#include <cstddef>
#include <cstdio>
#include <string_view>
#include <tuple>
#include <utility>

#include <villagesql/func_builder.h>
#include <villagesql/sdk_version.h>
#include <villagesql/storage_builder.h>
#include <villagesql/type_builder.h>

namespace villagesql {

namespace detail {

// Compile-time validation helpers. Defined before ExtensionBuilder so that
// detail::validate_custom_params is visible when ExtensionBuilder::func() is
// parsed (qualified names are resolved at template definition time by Clang).

// Extracts params_type from the TypeTuple element at compile-time index I.
template <size_t I, typename TypeTuple>
using params_type_at = typename std::tuple_element_t<I, TypeTuple>::params_type;

// Returns true if P appears as the params_type of any element in TypeTuple.
// P = void is never considered registered.
template <typename P, typename TypeTuple, size_t I = 0>
constexpr bool is_params_type_registered() {
  if constexpr (I >= std::tuple_size_v<TypeTuple>) {
    return false;
  } else if constexpr (std::is_same_v<params_type_at<I, TypeTuple>, P>) {
    return true;
  } else {
    return is_params_type_registered<P, TypeTuple, I + 1>();
  }
}

// For each parameter of Func that is CustomArgWith<P> or CustomResultWith<P>,
// asserts at compile time that P is registered as a params_type in TypeTuple
// via .params<P, &fn>() on the corresponding type builder.
template <auto Func, typename TypeTuple, size_t I = 0>
constexpr void validate_custom_params() {
  using ParamTuple =
      typename func_builder::FuncParamTypes<decltype(Func)>::type;
  if constexpr (I < std::tuple_size_v<ParamTuple>) {
    using T = std::tuple_element_t<I, ParamTuple>;
    using P = typename func_builder::params_type_of<T>::type;
    if constexpr (!std::is_void_v<P>) {
      static_assert(is_params_type_registered<P, TypeTuple>(),
                    "VDF parameter uses CustomArgWith<P> or "
                    "CustomResultWith<P> but P is not registered via "
                    ".params<P, &fn>() on any type builder");
    }
    validate_custom_params<Func, TypeTuple, I + 1>();
  }
}

}  // namespace detail

namespace extension_builder {

using namespace func_builder;
using namespace storage_builder;
using namespace type_builder;

// =============================================================================
// ExtensionBuilder
// =============================================================================
//
// Stores functions and types by value using tuples, allowing inline definition
// without separate variable declarations.

template <typename FuncTuple, typename TypeTuple>
struct ExtensionBuilder {
  std::string_view name_;
  std::string_view version_;
  FuncTuple funcs_;
  TypeTuple types_;
  vef_protocol_t min_protocol_;

  // Add a function from a StaticFuncDesc. This is the terminal overload that
  // all other func() overloads delegate to after validation.
  template <typename F>
  constexpr auto func(F f) const {
    auto new_funcs = std::tuple_cat(funcs_, std::make_tuple(f));
    return ExtensionBuilder<decltype(new_funcs), TypeTuple>{
        name_, version_, new_funcs, types_, min_protocol_};
  }

  // Add a function from a TypedFuncDesc (the result of FuncBuilder::build() or
  // make_type_encode/decode/compare/hash/intrinsic_default).
  // Validates at compile time that:
  //   - every CustomArgWith<P> / CustomResultWith<P> parameter has P registered
  //     in this builder's TypeTuple via .params<P, &fn>(); and
  //   - for parameterized type operation VDFs (ParamsType != void), ParamsType
  //     is likewise registered.
  // Works whether the descriptor is inline or pre-built.
  template <auto Func, size_t N, typename ParamsType>
  constexpr auto func(const TypedFuncDesc<Func, N, ParamsType> &desc) const {
    detail::validate_custom_params<Func, TypeTuple>();
    if constexpr (!std::is_void_v<ParamsType>) {
      static_assert(
          detail::is_params_type_registered<ParamsType, TypeTuple>(),
          "type operation VDF uses a parameterized cache but its params "
          "type P is not registered via .params<P, &fn>() on any type builder");
    }
    return func(static_cast<const StaticFuncDesc<N> &>(desc));
  }

  // Convenience overload: accepts a FuncBuilder directly (without calling
  // .build() at the call site) by delegating to the TypedFuncDesc overload.
  template <auto Func, size_t N>
  constexpr auto func(const FuncBuilder<Func, N> &builder) const {
    return func(builder.build());
  }

  // Add a type (returns new builder with type appended).
  // Accepts TypeDescriptorWithParams<P> so the params type P is preserved in
  // the TypeTuple, enabling compile-time validation in later steps. If the type
  // requires a higher protocol than min_protocol_, min_protocol_ is raised
  // automatically.
  template <typename P>
  constexpr auto type(const TypeDescriptorWithParams<P> &type) const {
    auto new_types = std::tuple_cat(types_, std::make_tuple(type));
    const auto &t = type.vef_desc;
    const vef_protocol_t new_min =
        t.protocol > min_protocol_ ? t.protocol : min_protocol_;
    return ExtensionBuilder<FuncTuple, decltype(new_types)>{
        name_, version_, funcs_, new_types, new_min};
  }

  // This is here only for testing, please don't depend on it.
  // Require a minimum VEF protocol version from the server.
  // If the server offers a lower protocol, registration will fail with an
  // error message explaining the version requirement.
  constexpr auto test_only_require_protocol(vef_protocol_t p) const {
    return ExtensionBuilder<FuncTuple, TypeTuple>{name_, version_, funcs_,
                                                  types_, p};
  }

  // Accessors
  constexpr std::string_view name() const { return name_; }
  constexpr std::string_view version() const { return version_; }
  constexpr size_t func_count() const { return std::tuple_size_v<FuncTuple>; }
  constexpr size_t type_count() const { return std::tuple_size_v<TypeTuple>; }
  constexpr vef_protocol_t min_protocol() const { return min_protocol_; }

  template <size_t I>
  constexpr const auto &func_at() const {
    return std::get<I>(funcs_);
  }

  template <size_t I>
  constexpr const auto &type_at() const {
    return std::get<I>(types_);
  }
};

// Entry point to create an extension builder
constexpr auto make_extension(std::string_view name, std::string_view version) {
  return ExtensionBuilder<std::tuple<>, std::tuple<>>{
      name, version, {}, {}, VEF_PROTOCOL_1};
}

}  // namespace extension_builder

namespace detail {

// Implementation helpers used by VEF_GENERATE_ENTRY_POINTS. Not part of the
// public API.

// Fills arr[I] with the materialized vef_func_desc_t* for each function.
template <typename Ext, size_t... Is>
void vef_fill_func_ptrs(vef_func_desc_t **arr, const Ext &e,
                        std::index_sequence<Is...>) {
  using villagesql::func_builder::materialize_func_desc;
  ((arr[Is] = materialize_func_desc<decltype(e.template func_at<Is>()), Is>(
        e.template func_at<Is>())),
   ...);
}

// Fills arr[I] with the vef_type_desc_t* for each type.
template <typename Ext, size_t... Is>
void vef_fill_type_ptrs(vef_type_desc_t **arr, const Ext &e,
                        std::index_sequence<Is...>) {
  ((arr[Is] =
        const_cast<vef_type_desc_t *>(&e.template type_at<Is>().vef_desc)),
   ...);
}

// Calls params_init_fn() for each type that has one.
template <typename Ext, size_t... Is>
void vef_init_type_params(const Ext &e, std::index_sequence<Is...>) {
  ((e.template type_at<Is>().params_init_fn
        ? e.template type_at<Is>().params_init_fn()
        : void()),
   ...);
}

// Core registration logic called by VEF_GENERATE_ENTRY_POINTS.
// FuncCount and TypeCount are explicit template parameters so that array
// sizes are compile-time constants without relying on VLAs.
template <typename Ext, size_t FuncCount, size_t TypeCount>
vef_registration_t *vef_register_impl(vef_registration_t &reg,
                                      bool &initialized,
                                      vef_register_arg_t *arg, const Ext &ext) {
  if (initialized) return &reg;

  if (arg->protocol < ext.min_protocol()) {
    static char error_buf[128];
    snprintf(error_buf, sizeof(error_buf),
             "requires VEF protocol %u, server offered %u",
             static_cast<unsigned>(ext.min_protocol()),
             static_cast<unsigned>(arg->protocol));
    reg.protocol = arg->protocol;
    reg.error_msg = error_buf;
    return &reg;
  }

  static vef_func_desc_t *func_ptrs[FuncCount > 0 ? FuncCount : 1];
  static vef_type_desc_t *type_ptrs[TypeCount > 0 ? TypeCount : 1];

  if constexpr (FuncCount > 0) {
    vef_fill_func_ptrs(func_ptrs, ext, std::make_index_sequence<FuncCount>{});
  }
  if constexpr (TypeCount > 0) {
    vef_fill_type_ptrs(type_ptrs, ext, std::make_index_sequence<TypeCount>{});
    vef_init_type_params(ext, std::make_index_sequence<TypeCount>{});
  }

  reg.protocol = VEF_PROTOCOL_2;
  reg.error_msg = nullptr;
  reg.extension_name = ext.name().data();
  reg.extension_version = ext.version().data();
  reg.sdk_version = kSdkVersion;
  reg.func_count = FuncCount;
  reg.funcs = FuncCount > 0 ? func_ptrs : nullptr;
  reg.type_count = TypeCount;
  reg.types = TypeCount > 0 ? type_ptrs : nullptr;

  initialized = true;
  return &reg;
}

}  // namespace detail
}  // namespace villagesql

// VEF_GENERATE_ENTRY_POINTS
//
// Generates the extern "C" vef_register and vef_unregister functions.
// Must be called in a .cc file, not a header (defines functions/variables).
// Delegates registration logic to villagesql::detail::vef_register_impl.

#define VEF_GENERATE_ENTRY_POINTS(ext)                                   \
  namespace {                                                            \
  vef_registration_t vef_reg_;                                           \
  bool vef_reg_initialized_ = false;                                     \
  }                                                                      \
                                                                         \
  extern "C" vef_registration_t *vef_register(vef_register_arg_t *arg) { \
    using namespace villagesql::extension_builder;                       \
    static constexpr auto kExt = (ext);                                  \
    return villagesql::detail::vef_register_impl<                        \
        decltype(kExt), kExt.func_count(), kExt.type_count()>(           \
        vef_reg_, vef_reg_initialized_, arg, kExt);                      \
  }                                                                      \
                                                                         \
  extern "C" void vef_unregister(vef_unregister_arg_t *arg,              \
                                 vef_registration_t *reg) {              \
    (void)arg;                                                           \
    (void)reg;                                                           \
  }

#endif  // VILLAGESQL_SDK_EXTENSION_BUILDER_H
