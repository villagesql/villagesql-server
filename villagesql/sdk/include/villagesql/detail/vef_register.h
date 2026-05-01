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

#ifndef VILLAGESQL_DETAIL_VEF_REGISTER_H
#define VILLAGESQL_DETAIL_VEF_REGISTER_H

// Internal implementation shared by extension_builder.h and
// vsql/extension_builder.h. Not part of the public API.

#include <cstddef>
#include <cstdio>
#include <tuple>
#include <type_traits>
#include <utility>

#include <villagesql/abi/types.h>
#include <villagesql/sdk_version.h>

namespace villagesql {

// Forward-declare the vsql globals so that name lookup succeeds inside the
// `if constexpr (Ext::kHasVsqlGlobals)` block even when only the base
// extension_builder.h (not vsql.h) is included. Clang resolves non-dependent
// names at parse time regardless of whether the branch is discarded.
namespace sys_var {
extern vef_get_variable_fn g_get_variable;
extern vef_set_variable_fn g_set_variable;
}  // namespace sys_var
namespace keyring {
extern vef_read_keyring_fn g_read_keyring;
extern vef_write_keyring_fn g_write_keyring;
}  // namespace keyring

// Forward-declare materialize_func_desc so the helpers below can reference it
// without requiring func_builder.h to be included first.
namespace func_builder {
template <typename FuncData, size_t Index>
vef_func_desc_t *materialize_func_desc(const FuncData &func_data);
}  // namespace func_builder

namespace detail {

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

// Fills arr[I] with the vef_status_var_desc_t* for each status variable.
template <typename Ext, size_t... Is>
void vef_fill_status_var_ptrs(vef_status_var_desc_t **arr, const Ext &e,
                              std::index_sequence<Is...>) {
  ((arr[Is] = const_cast<vef_status_var_desc_t *>(
        &e.template status_var_at<Is>().desc)),
   ...);
}

// Fills arr[I] with a copy of each vef_required_capability_t.
template <typename Ext, size_t... Is>
void vef_fill_required_capability_reqs(vef_required_capability_t *arr,
                                       const Ext &e,
                                       std::index_sequence<Is...>) {
  ((arr[Is] = e.template required_capability_at<Is>()), ...);
}

// Calls params_init_fn() for each type that has one.
template <typename Ext, size_t... Is>
void vef_init_type_params(const Ext &e, std::index_sequence<Is...>) {
  ((e.template type_at<Is>().params_init_fn
        ? e.template type_at<Is>().params_init_fn()
        : void()),
   ...);
}

// Calls init_name() on any func that has it (auto-named VDFs from the vsql
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
// FuncCount, TypeCount, SysVarCount, StatusVarCount, and
// RequiredCapabilityCount are explicit template parameters so that array sizes
// are compile-time constants without relying on VLAs.
template <typename Ext, size_t FuncCount, size_t TypeCount, size_t SysVarCount,
          size_t StatusVarCount, size_t RequiredCapabilityCount>
vef_registration_t *vef_register_impl(vef_registration_t &reg,
                                      bool &initialized,
                                      vef_register_arg_t *arg, const Ext &ext) {
  if (initialized) return &reg;

  // Ext::kHasVsqlGlobals is true for vsql::ExtensionBuilder (which always
  // includes keyring.h and sys_var_builder.h) and false for the base
  // extension_builder::ExtensionBuilder (which does not). The if constexpr
  // guard prunes this block for base extensions so those TUs don't need the
  // vsql headers in scope.
  if constexpr (Ext::kHasVsqlGlobals) {
    if (arg->protocol >= VEF_PROTOCOL_2) {
      villagesql::sys_var::g_get_variable = arg->get_variable;
      villagesql::sys_var::g_set_variable = arg->set_variable;
      villagesql::keyring::g_read_keyring = arg->read_keyring;
      villagesql::keyring::g_write_keyring = arg->write_keyring;
    }
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
  static vef_status_var_desc_t
      *status_var_ptrs[StatusVarCount > 0 ? StatusVarCount : 1];
  static vef_required_capability_t required_capability_reqs
      [RequiredCapabilityCount > 0 ? RequiredCapabilityCount : 1];

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
  if constexpr (StatusVarCount > 0) {
    vef_fill_status_var_ptrs(status_var_ptrs, ext,
                             std::make_index_sequence<StatusVarCount>{});
  }
  if constexpr (RequiredCapabilityCount > 0) {
    vef_fill_required_capability_reqs(
        required_capability_reqs, ext,
        std::make_index_sequence<RequiredCapabilityCount>{});
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
  reg.deprecated_extension_name = nullptr;
  reg.deprecated_extension_version = nullptr;
  reg.sdk_version = kSdkVersion;
  reg.func_count = FuncCount;
  reg.funcs = FuncCount > 0 ? func_ptrs : nullptr;
  reg.type_count = TypeCount;
  reg.types = TypeCount > 0 ? type_ptrs : nullptr;
  reg.sys_var_count = SysVarCount;
  reg.sys_vars = SysVarCount > 0 ? sys_var_ptrs : nullptr;
  reg.status_var_count = StatusVarCount;
  reg.status_vars = StatusVarCount > 0 ? status_var_ptrs : nullptr;
  reg.required_capability_count = RequiredCapabilityCount;
  reg.required_capabilities =
      RequiredCapabilityCount > 0 ? required_capability_reqs : nullptr;

  initialized = true;
  return &reg;
}

}  // namespace detail
}  // namespace villagesql

#endif  // VILLAGESQL_DETAIL_VEF_REGISTER_H
