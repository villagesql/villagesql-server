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

#ifndef VILLAGESQL_VSQL_EXTENSION_BUILDER_H
#define VILLAGESQL_VSQL_EXTENSION_BUILDER_H

#include <cstddef>
#include <tuple>
#include <utility>

#include <villagesql/detail/cap_receive.h>
#include <villagesql/vsql/capability_traits.h>

#include <villagesql/detail/vef_register.h>
#include <villagesql/vsql/status_var_builder.h>
#include <villagesql/vsql/sys_var_builder.h>

namespace vsql {

// Re-export func_builder symbols so the VEF_GENERATE_* macros can resolve
// make_func, INT, STRING, etc. without additional using-declarations at the
// call site.
using namespace func_builder;

// ExtensionBuilder is the vsql-API extension builder. It is a standalone type
// (not a wrapper around villagesql::extension_builder::ExtensionBuilder) so it
// can evolve independently. It satisfies the same duck-typed interface required
// by VEF_GENERATE_ENTRY_POINTS.
template <typename FuncTuple, typename TypeTuple, typename SysVarTuple,
          typename StatusVarTuple,
          typename RequiredCapabilityTuple = std::tuple<>>
struct ExtensionBuilder {
  FuncTuple funcs_;
  TypeTuple types_;
  SysVarTuple sys_vars_;
  StatusVarTuple status_vars_;
  RequiredCapabilityTuple required_capabilities_;
  vef_protocol_t min_protocol_;

  static constexpr size_t kFuncCount = std::tuple_size_v<FuncTuple>;
  static constexpr size_t kTypeCount = std::tuple_size_v<TypeTuple>;
  static constexpr size_t kSysVarCount = std::tuple_size_v<SysVarTuple>;
  static constexpr size_t kStatusVarCount = std::tuple_size_v<StatusVarTuple>;
  static constexpr size_t kRequiredCapabilityCount =
      std::tuple_size_v<RequiredCapabilityTuple>;
  static constexpr bool kHasVsqlGlobals = true;

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

  template <size_t I>
  constexpr const auto &status_var_at() const {
    return std::get<I>(status_vars_);
  }

  template <size_t I>
  constexpr const auto &required_capability_at() const {
    return std::get<I>(required_capabilities_);
  }

  constexpr vef_protocol_t require_atleast_min(vef_protocol_t required) const {
    return required > min_protocol_ ? required : min_protocol_;
  }

  template <typename F>
  constexpr auto func(F f) const {
    auto new_funcs = std::tuple_cat(funcs_, std::make_tuple(f));
    return ExtensionBuilder<decltype(new_funcs), TypeTuple, SysVarTuple,
                            StatusVarTuple, RequiredCapabilityTuple>{
        new_funcs,    types_, sys_vars_, status_vars_, required_capabilities_,
        min_protocol_};
  }

  // Accepts a TypeObject (from vsql::make_type().build()) that carries embedded
  // VDFs alongside the type descriptor, registering both in one call.
  template <typename TypeObj,
            typename = decltype(std::declval<TypeObj>().embedded_funcs)>
  constexpr auto type(const TypeObj &t) const {
    auto new_types = std::tuple_cat(types_, std::make_tuple(t.descriptor));
    auto new_funcs = std::tuple_cat(funcs_, t.embedded_funcs);
    return ExtensionBuilder<decltype(new_funcs), decltype(new_types),
                            SysVarTuple, StatusVarTuple,
                            RequiredCapabilityTuple>{
        new_funcs,
        new_types,
        sys_vars_,
        status_vars_,
        required_capabilities_,
        require_atleast_min(t.descriptor.vef_desc.protocol)};
  }

  constexpr auto sys_var(const sys_var_builder::SysVarDescriptor &sv) const {
    auto new_svs = std::tuple_cat(sys_vars_, std::make_tuple(sv));
    return ExtensionBuilder<FuncTuple, TypeTuple, decltype(new_svs),
                            StatusVarTuple, RequiredCapabilityTuple>{
        funcs_,
        types_,
        new_svs,
        status_vars_,
        required_capabilities_,
        require_atleast_min(VEF_PROTOCOL_2)};
  }

  constexpr auto status_var(
      const status_var_builder::StatusVarDescriptor &sv) const {
    auto new_svs = std::tuple_cat(status_vars_, std::make_tuple(sv));
    return ExtensionBuilder<FuncTuple, TypeTuple, SysVarTuple,
                            decltype(new_svs), RequiredCapabilityTuple>{
        funcs_,
        types_,
        sys_vars_,
        new_svs,
        required_capabilities_,
        require_atleast_min(VEF_PROTOCOL_2)};
  }

  // No-op. Preview capability builders that need runtime initialization before
  // vef_register_impl() runs should wrap ExtensionBuilder and override this.
  void init() const {}

  // Capability registration. The user's wrapper is captured into the
  // builder's tuple as a typed Capability*. At registration time
  // vef_register_impl publishes the pointer into
  // CapReceiveSlot<Capability> and emits a wire entry whose receive
  // callback is the slot's captureless `receive` function.
  template <typename Capability>
  constexpr auto with(Capability &cap) const {
    auto new_caps =
        std::tuple_cat(required_capabilities_, std::make_tuple(&cap));
    return ExtensionBuilder<FuncTuple, TypeTuple, SysVarTuple, StatusVarTuple,
                            decltype(new_caps)>{
        funcs_,       types_,   sys_vars_,
        status_vars_, new_caps, require_atleast_min(VEF_PROTOCOL_2)};
  }

  // For testing only — forces the extension to require protocol p regardless
  // of which features are registered.
  constexpr auto test_only_require_protocol(vef_protocol_t p) const {
    return ExtensionBuilder<FuncTuple, TypeTuple, SysVarTuple, StatusVarTuple,
                            RequiredCapabilityTuple>{
        funcs_, types_, sys_vars_, status_vars_, required_capabilities_, p};
  }
};

constexpr auto make_extension() {
  return ExtensionBuilder<std::tuple<>, std::tuple<>, std::tuple<>,
                          std::tuple<>, std::tuple<>>{{}, {}, {},
                                                      {}, {}, VEF_PROTOCOL_1};
}
}  // namespace vsql

// VEF_GENERATE_REGISTRATION (vsql variant)
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
    using namespace vsql;                                                  \
    static constexpr auto kExt = (ext);                                    \
    using ExtType = decltype(kExt);                                        \
    return villagesql::detail::vef_register_impl<                          \
        decltype(kExt), ExtType::kFuncCount, ExtType::kTypeCount,          \
        ExtType::kSysVarCount, ExtType::kStatusVarCount,                   \
        ExtType::kRequiredCapabilityCount>(_vef_reg, _vef_reg_initialized, \
                                           arg, kExt);                     \
  }

// VEF_GENERATE_ENTRY_POINTS (vsql variant)
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
    using namespace vsql;                                                  \
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

#endif  // VILLAGESQL_VSQL_EXTENSION_BUILDER_H
