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

#include <string_view>
#include <tuple>
#include <utility>

#include <villagesql/detail/vef_register.h>
#include <villagesql/func_builder.h>
#include <villagesql/type_builder.h>

namespace villagesql {

namespace extension_builder {

using type_builder::TypeDescriptor;

// Re-export func_builder and type_builder symbols so the VEF_GENERATE_*
// macros can resolve make_func, INT, STRING, make_type, etc. without
// additional using-declarations at the call site. Whichever func_builder
// header was included (v1 or vsql) determines which make_func is in scope.
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
  FuncTuple funcs_;
  TypeTuple types_;
  vef_protocol_t min_protocol_;

  // Add a function (returns new builder with function appended)
  template <typename F>
  constexpr auto func(F f) const {
    auto new_funcs = std::tuple_cat(funcs_, std::make_tuple(f));
    return ExtensionBuilder<decltype(new_funcs), TypeTuple>{new_funcs, types_,
                                                            min_protocol_};
  }

  // Add a type (returns new builder with type appended).
  // If the type requires a higher protocol than min_protocol_, min_protocol_
  // is raised automatically.
  constexpr auto type(const TypeDescriptor &td) const {
    auto new_types = std::tuple_cat(types_, std::make_tuple(td));
    return ExtensionBuilder<FuncTuple, decltype(new_types)>{
        funcs_, new_types, require_atleast_min(td.vef_desc.protocol)};
  }

  constexpr vef_protocol_t require_atleast_min(vef_protocol_t required) const {
    return required > min_protocol_ ? required : min_protocol_;
  }

  static constexpr size_t kFuncCount = std::tuple_size_v<FuncTuple>;
  static constexpr size_t kTypeCount = std::tuple_size_v<TypeTuple>;
  static constexpr size_t kSysVarCount = 0;
  static constexpr size_t kStatusVarCount = 0;
  static constexpr size_t kRequiredCapabilityCount = 0;
  static constexpr bool kHasVsqlGlobals = false;

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
constexpr auto make_extension() {
  return ExtensionBuilder<std::tuple<>, std::tuple<>>{{}, {}, VEF_PROTOCOL_1};
}

// Deprecated: name and version are now read from the VEB manifest file.
[[deprecated(
    "use make_extension() — name/version are now in the VEB manifest")]]
constexpr auto make_extension(std::string_view /*name*/,
                              std::string_view /*version*/) {
  return make_extension();
}

}  // namespace extension_builder
}  // namespace villagesql

// VEF_GENERATE_REGISTRATION
//
// Creates a _vef_do_register() helper that performs extension registration.
// Use this when you need to customize vef_register behavior (e.g., to patch
// descriptors after registration for testing). Otherwise use
// VEF_GENERATE_ENTRY_POINTS which generates the full extern "C" entry points.

#define VEF_GENERATE_REGISTRATION(ext)                                     \
  namespace {                                                              \
  vef_registration_t _vef_reg;                                             \
  bool _vef_reg_initialized = false;                                       \
  }                                                                        \
                                                                           \
  static vef_registration_t *_vef_do_register(vef_register_arg_t *arg) {   \
    using namespace villagesql::extension_builder;                         \
    static constexpr auto kExt = (ext);                                    \
    using ExtType = decltype(kExt);                                        \
    return villagesql::detail::vef_register_impl<                          \
        decltype(kExt), ExtType::kFuncCount, ExtType::kTypeCount,          \
        ExtType::kSysVarCount, ExtType::kStatusVarCount,                   \
        ExtType::kRequiredCapabilityCount>(_vef_reg, _vef_reg_initialized, \
                                           arg, kExt);                     \
  }

// VEF_GENERATE_ENTRY_POINTS
//
// Generates the extern "C" vef_register and vef_unregister functions.
// Must be called in a .cc file, not a header (defines functions/variables).

#define VEF_GENERATE_ENTRY_POINTS(ext)                                     \
  namespace {                                                              \
  vef_registration_t vef_reg_;                                             \
  bool vef_reg_initialized_ = false;                                       \
  }                                                                        \
                                                                           \
  extern "C" vef_registration_t *vef_register(vef_register_arg_t *arg) {   \
    using namespace villagesql::extension_builder;                         \
    static constexpr auto kExt = (ext);                                    \
    using ExtType = decltype(kExt);                                        \
    return villagesql::detail::vef_register_impl<                          \
        decltype(kExt), ExtType::kFuncCount, ExtType::kTypeCount,          \
        ExtType::kSysVarCount, ExtType::kStatusVarCount,                   \
        ExtType::kRequiredCapabilityCount>(vef_reg_, vef_reg_initialized_, \
                                           arg, kExt);                     \
  }                                                                        \
                                                                           \
  extern "C" void vef_unregister(vef_unregister_arg_t *arg,                \
                                 vef_registration_t *reg) {                \
    (void)arg;                                                             \
    (void)reg;                                                             \
  }

#endif  // VILLAGESQL_SDK_EXTENSION_BUILDER_H
