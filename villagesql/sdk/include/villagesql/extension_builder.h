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

#include <villagesql/sdk_version.h>
#include <villagesql/type_builder.h>
#include <villagesql/vsql/keyring.h>
#include <villagesql/vsql/sys_var_builder.h>

namespace villagesql {
namespace extension_builder {

using sys_var_builder::SysVarDescriptor;
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
// Stores functions, types, and config vars by value using tuples, allowing
// inline definition without separate variable declarations.

template <typename FuncTuple, typename TypeTuple, typename SysVarTuple>
struct ExtensionBuilder {
  std::string_view name_;
  std::string_view version_;
  FuncTuple funcs_;
  TypeTuple types_;
  SysVarTuple sys_vars_;
  vef_protocol_t min_protocol_;

  // Add a function (returns new builder with function appended)
  template <typename F>
  constexpr auto func(F f) const {
    auto new_funcs = std::tuple_cat(funcs_, std::make_tuple(f));
    return ExtensionBuilder<decltype(new_funcs), TypeTuple, SysVarTuple>{
        name_, version_, new_funcs, types_, sys_vars_, min_protocol_};
  }

  // Add a type (returns new builder with type appended).
  // If the type requires a higher protocol than min_protocol_, min_protocol_
  // is raised automatically.
  constexpr auto type(const TypeDescriptor &td) const {
    auto new_types = std::tuple_cat(types_, std::make_tuple(td));
    const auto &t = td.vef_desc;
    const vef_protocol_t new_min =
        t.protocol > min_protocol_ ? t.protocol : min_protocol_;
    return ExtensionBuilder<FuncTuple, decltype(new_types), SysVarTuple>{
        name_, version_, funcs_, new_types, sys_vars_, new_min};
  }

  // Add a system variable. System variables require at least VEF_PROTOCOL_2.
  constexpr auto sys_var(const SysVarDescriptor &cv) const {
    auto new_cvs = std::tuple_cat(sys_vars_, std::make_tuple(cv));
    const vef_protocol_t new_min =
        VEF_PROTOCOL_2 > min_protocol_ ? VEF_PROTOCOL_2 : min_protocol_;
    return ExtensionBuilder<FuncTuple, TypeTuple, decltype(new_cvs)>{
        name_, version_, funcs_, types_, new_cvs, new_min};
  }

  // Add a type object that carries embedded SQL-callable VDFs (e.g. the
  // vsql::TypeObject<EFT> produced by vsql::make_type().build()). The type
  // descriptor and its embedded VDFs are both registered in one call.
  // Selected by overload resolution when the argument has an embedded_funcs
  // member (preferred over the TypeDescriptor overload as an exact match).
  template <typename TypeObj,
            typename = decltype(std::declval<TypeObj>().embedded_funcs)>
  constexpr auto type(const TypeObj &t) const {
    auto new_types = std::tuple_cat(types_, std::make_tuple(t.descriptor));
    auto new_funcs = std::tuple_cat(funcs_, t.embedded_funcs);
    const vef_protocol_t new_min =
        t.descriptor.vef_desc.protocol > min_protocol_
            ? t.descriptor.vef_desc.protocol
            : min_protocol_;
    return ExtensionBuilder<decltype(new_funcs), decltype(new_types),
                            SysVarTuple>{name_,     version_,  new_funcs,
                                         new_types, sys_vars_, new_min};
  }

  // This is here only for testing, please don't depend on it.
  // Require a minimum VEF protocol version from the server.
  // If the server offers a lower protocol, registration will fail with an
  // error message explaining the version requirement.
  constexpr auto test_only_require_protocol(vef_protocol_t p) const {
    return ExtensionBuilder<FuncTuple, TypeTuple, SysVarTuple>{
        name_, version_, funcs_, types_, sys_vars_, p};
  }

  // Compile-time counts
  static constexpr size_t kFuncCount = std::tuple_size_v<FuncTuple>;
  static constexpr size_t kTypeCount = std::tuple_size_v<TypeTuple>;
  static constexpr size_t kSysVarCount = std::tuple_size_v<SysVarTuple>;

  // Accessors
  constexpr std::string_view name() const { return name_; }
  constexpr std::string_view version() const { return version_; }
  constexpr vef_protocol_t min_protocol() const { return min_protocol_; }

  template <size_t I>
  constexpr const auto &func_at() const {
    return std::get<I>(funcs_);
  }

  template <size_t I>
  constexpr const auto &type_at() const {
    return std::get<I>(types_);
  }

  template <size_t I>
  constexpr const auto &sys_var_at() const {
    return std::get<I>(sys_vars_);
  }
};

// Entry point to create an extension builder
constexpr auto make_extension(std::string_view name, std::string_view version) {
  return ExtensionBuilder<std::tuple<>, std::tuple<>, std::tuple<>>{
      name, version, {}, {}, {}, VEF_PROTOCOL_1};
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

// Fills arr[I] with the vef_sys_var_desc_t* for each system variable.
template <typename Ext, size_t... Is>
void vef_fill_sys_var_ptrs(vef_sys_var_desc_t **arr, const Ext &e,
                           std::index_sequence<Is...>) {
  ((arr[Is] =
        const_cast<vef_sys_var_desc_t *>(&e.template sys_var_at<Is>().desc)),
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

// Calls init_name() on any func that has it (auto-named VDFs from the ::vsql
// API). Must be called before vef_fill_func_ptrs() so that name buffers are
// populated before materialize_func_desc reads them.
template <typename T, typename = void>
struct has_init_name : std::false_type {};
template <typename T>
struct has_init_name<
    T, std::void_t<decltype(std::declval<const T &>().init_name())>>
    : std::true_type {};

template <typename Ext, size_t... Is>
void vef_init_auto_names(const Ext &e, std::index_sequence<Is...>) {
  auto init_one = [](const auto &f) {
    using F = std::decay_t<decltype(f)>;
    if constexpr (has_init_name<F>::value) f.init_name();
  };
  (init_one(e.template func_at<Is>()), ...);
}

// Returns the name of the first VDF that requires a bound params cache but
// whose cache was not bound (i.e., .params<P, &parse_fn>() was omitted from
// the type builder). Must be called after vef_init_type_params().
template <typename Ext, size_t... Is>
const char *vef_check_params_cache(const Ext &e, std::index_sequence<Is...>) {
  const char *unbound = nullptr;
  auto check_one = [&unbound](const auto &func) {
    if (unbound) return;
    auto check_fn = func.check_params_cache_bound();
    if (check_fn && !check_fn()) unbound = func.name();
  };
  (check_one(e.template func_at<Is>()), ...);
  return unbound;
}

// Core registration logic called by VEF_GENERATE_ENTRY_POINTS.
// FuncCount, TypeCount, and SysVarCount are explicit template parameters
// so that array sizes are compile-time constants without relying on VLAs.
template <typename Ext, size_t FuncCount, size_t TypeCount, size_t SysVarCount>
vef_registration_t *vef_register_impl(vef_registration_t &reg,
                                      bool &initialized,
                                      vef_register_arg_t *arg, const Ext &ext) {
  if (initialized) return &reg;

  // Capture server-provided function pointers for protocol >= 2.
  if (arg->protocol >= VEF_PROTOCOL_2) {
    villagesql::sys_var::g_get_variable = arg->get_variable;
    villagesql::sys_var::g_set_variable = arg->set_variable;
    villagesql::keyring::g_read_keyring = arg->read_keyring;
    villagesql::keyring::g_write_keyring = arg->write_keyring;
  }

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
  static vef_sys_var_desc_t *sys_var_ptrs[SysVarCount > 0 ? SysVarCount : 1];

  if constexpr (FuncCount > 0) {
    vef_init_auto_names(ext, std::make_index_sequence<FuncCount>{});
    vef_fill_func_ptrs(func_ptrs, ext, std::make_index_sequence<FuncCount>{});
  }
  if constexpr (TypeCount > 0) {
    vef_fill_type_ptrs(type_ptrs, ext, std::make_index_sequence<TypeCount>{});
    vef_init_type_params(ext, std::make_index_sequence<TypeCount>{});
  }
  if constexpr (SysVarCount > 0) {
    vef_fill_sys_var_ptrs(sys_var_ptrs, ext,
                          std::make_index_sequence<SysVarCount>{});
  }

  if constexpr (FuncCount > 0) {
    const char *unbound_vdf =
        vef_check_params_cache(ext, std::make_index_sequence<FuncCount>{});
    if (unbound_vdf) {
      static char error_buf[256];
      snprintf(error_buf, sizeof(error_buf),
               "VDF '%s' uses a parameterized type cache but no "
               ".params<P, &parse_fn>() was registered for that params type; "
               "add .params<P, &parse_fn>() to the type builder",
               unbound_vdf);
      reg.protocol = arg->protocol;
      reg.error_msg = error_buf;
      return &reg;
    }
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
  reg.sys_var_count = SysVarCount;
  reg.sys_vars = SysVarCount > 0 ? sys_var_ptrs : nullptr;

  initialized = true;
  return &reg;
}

}  // namespace detail
}  // namespace villagesql

// VEF_GENERATE_REGISTRATION
//
// Creates a _vef_do_register() helper that performs extension registration.
// Use this when you need to customize vef_register behavior (e.g., to patch
// descriptors after registration for testing). Otherwise use
// VEF_GENERATE_ENTRY_POINTS which generates the full extern "C" entry points.

#define VEF_GENERATE_REGISTRATION(ext)                                       \
  namespace {                                                                \
  vef_registration_t _vef_reg;                                               \
  bool _vef_reg_initialized = false;                                         \
  }                                                                          \
                                                                             \
  static vef_registration_t *_vef_do_register(vef_register_arg_t *arg) {     \
    using namespace villagesql::extension_builder;                           \
    static constexpr auto kExt = (ext);                                      \
    using ExtType = decltype(kExt);                                          \
    return villagesql::detail::vef_register_impl<                            \
        decltype(kExt), ExtType::kFuncCount, ExtType::kTypeCount,            \
        ExtType::kSysVarCount>(                                              \
        _vef_reg, _vef_reg_initialized, arg, kExt);                          \
  }

// VEF_GENERATE_ENTRY_POINTS
//
// Generates the extern "C" vef_register and vef_unregister functions.
// Must be called in a .cc file, not a header (defines functions/variables).
// Delegates registration logic to villagesql::detail::vef_register_impl.

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
        ExtType::kSysVarCount>(vef_reg_, vef_reg_initialized_, arg, kExt); \
  }                                                                        \
                                                                           \
  extern "C" void vef_unregister(vef_unregister_arg_t *arg,                \
                                 vef_registration_t *reg) {                \
    (void)arg;                                                             \
    (void)reg;                                                             \
  }

#endif  // VILLAGESQL_SDK_EXTENSION_BUILDER_H
