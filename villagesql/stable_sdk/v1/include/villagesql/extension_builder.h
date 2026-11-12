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
#include <string_view>
#include <tuple>
#include <utility>

#include <villagesql/func_builder.h>
#include <villagesql/type_builder.h>

namespace villagesql {
namespace extension_builder {

using namespace func_builder;
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

  // Add a function (returns new builder with function appended)
  template <typename F>
  constexpr auto func(F f) const {
    auto new_funcs = std::tuple_cat(funcs_, std::make_tuple(f));
    return ExtensionBuilder<decltype(new_funcs), TypeTuple>{name_, version_,
                                                            new_funcs, types_};
  }

  // Add a type (returns new builder with type appended)
  constexpr auto type(const vef_type_desc_t &t) const {
    auto new_types = std::tuple_cat(types_, std::make_tuple(t));
    return ExtensionBuilder<FuncTuple, decltype(new_types)>{name_, version_,
                                                            funcs_, new_types};
  }

  // Accessors
  constexpr std::string_view name() const { return name_; }
  constexpr std::string_view version() const { return version_; }
  constexpr size_t func_count() const { return std::tuple_size_v<FuncTuple>; }
  constexpr size_t type_count() const { return std::tuple_size_v<TypeTuple>; }

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
  return ExtensionBuilder<std::tuple<>, std::tuple<>>{name, version, {}, {}};
}

}  // namespace extension_builder
}  // namespace villagesql

// =============================================================================
// VEF_GENERATE_ENTRY_POINTS
// =============================================================================
//
// Generates the extern "C" vef_register and vef_unregister functions.
// Must be called in a .cc file, not a header (defines functions/variables).
//

#define VEF_GENERATE_ENTRY_POINTS(ext)                                         \
  static vef_registration_t _vef_registration;                                 \
  static bool _vef_initialized = false;                                        \
                                                                               \
  template <typename Ext, size_t... Is>                                        \
  static void _vef_fill_func_ptrs_impl(vef_func_desc_t **arr, const Ext &e,    \
                                       std::index_sequence<Is...>) {           \
    using villagesql::func_builder::materialize_func_desc;                     \
    ((arr[Is] = materialize_func_desc<decltype(e.template func_at<Is>()), Is>( \
          e.template func_at<Is>())),                                          \
     ...);                                                                     \
  }                                                                            \
                                                                               \
  template <typename Ext, size_t... Is>                                        \
  static void _vef_fill_type_ptrs_impl(vef_type_desc_t **arr, const Ext &e,    \
                                       std::index_sequence<Is...>) {           \
    ((arr[Is] = const_cast<vef_type_desc_t *>(&e.template type_at<Is>())),     \
     ...);                                                                     \
  }                                                                            \
                                                                               \
  extern "C" vef_registration_t *vef_register(vef_register_arg_t *arg) {       \
    (void)arg;                                                                 \
    if (_vef_initialized) return &_vef_registration;                           \
                                                                               \
    using namespace villagesql::extension_builder;                             \
    static constexpr auto _ext = (ext);                                        \
    constexpr size_t func_count = _ext.func_count();                           \
    constexpr size_t type_count = _ext.type_count();                           \
                                                                               \
    static vef_func_desc_t *func_ptrs[func_count > 0 ? func_count : 1];        \
    static vef_type_desc_t *type_ptrs[type_count > 0 ? type_count : 1];        \
                                                                               \
    if constexpr (func_count > 0) {                                            \
      _vef_fill_func_ptrs_impl(func_ptrs, _ext,                                \
                               std::make_index_sequence<func_count>{});        \
    }                                                                          \
    if constexpr (type_count > 0) {                                            \
      _vef_fill_type_ptrs_impl(type_ptrs, _ext,                                \
                               std::make_index_sequence<type_count>{});        \
    }                                                                          \
                                                                               \
    _vef_registration.protocol = VEF_PROTOCOL_1;                               \
    _vef_registration.error_msg = nullptr;                                     \
    _vef_registration.extension_name = _ext.name().data();                     \
    _vef_registration.extension_version = _ext.version().data();               \
    _vef_registration.sdk_version = {1, 0, 0, nullptr};                        \
    _vef_registration.func_count = func_count;                                 \
    _vef_registration.funcs = func_count > 0 ? func_ptrs : nullptr;            \
    _vef_registration.type_count = type_count;                                 \
    _vef_registration.types = type_count > 0 ? type_ptrs : nullptr;            \
                                                                               \
    _vef_initialized = true;                                                   \
    return &_vef_registration;                                                 \
  }                                                                            \
                                                                               \
  extern "C" void vef_unregister(vef_unregister_arg_t *arg,                    \
                                 vef_registration_t *reg) {                    \
    (void)arg;                                                                 \
    (void)reg;                                                                 \
  }

#endif  // VILLAGESQL_SDK_EXTENSION_BUILDER_H
