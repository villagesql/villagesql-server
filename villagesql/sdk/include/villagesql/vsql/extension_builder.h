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

#include <villagesql/detail/capability_traits.h>

#include <villagesql/detail/vef_register.h>

namespace vsql {

// Re-export func_builder symbols so the VEF_GENERATE_* macros can resolve
// make_func, INT, STRING, etc. without additional using-declarations at the
// call site.
using namespace func_builder;

// ExtensionBuilder is the vsql-API extension builder. It is a standalone type
// (not a wrapper around villagesql::extension_builder::ExtensionBuilder) so it
// can evolve independently. It satisfies the same duck-typed interface required
// by VEF_GENERATE_ENTRY_POINTS.
template <typename FuncTuple, typename TypeTuple,
          typename RequiredCapabilityTuple = std::tuple<>,
          void (*InitFn)() = nullptr, void (*DeinitFn)() = nullptr>
struct ExtensionBuilder {
  FuncTuple funcs_;
  TypeTuple types_;
  RequiredCapabilityTuple required_capabilities_;
  vef_protocol_t min_protocol_;

  static constexpr size_t kFuncCount = std::tuple_size_v<FuncTuple>;
  static constexpr size_t kTypeCount = std::tuple_size_v<TypeTuple>;
  static constexpr size_t kRequiredCapabilityCount =
      std::tuple_size_v<RequiredCapabilityTuple>;

  // Extension-side init / deinit callbacks (null if unset). Invoked by
  // vef_register_impl / vef_unregister — see on_init() / on_deinit() below.
  static constexpr void (*kInitFn)() = InitFn;
  static constexpr void (*kDeinitFn)() = DeinitFn;

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
  constexpr const auto &required_capability_at() const {
    return std::get<I>(required_capabilities_);
  }

  constexpr vef_protocol_t require_atleast_min(vef_protocol_t required) const {
    return required > min_protocol_ ? required : min_protocol_;
  }

  template <typename F>
  constexpr auto func(F f) const {
    auto new_funcs = std::tuple_cat(funcs_, std::make_tuple(f));
    return ExtensionBuilder<decltype(new_funcs), TypeTuple,
                            RequiredCapabilityTuple, InitFn, DeinitFn>{
        new_funcs, types_, required_capabilities_,
        require_atleast_min(f.required_protocol())};
  }

  // Accepts a TypeObject (from vsql::make_type().build()) that carries embedded
  // VDFs and params-cache init fns alongside the type descriptor, registering
  // all of them in one call. The whole TypeObject is stored in types_ so that
  // detail/vef_register.h can reach the init fns at registration time.
  template <typename TypeObj,
            typename = decltype(std::declval<TypeObj>().embedded_funcs)>
  constexpr auto type(const TypeObj &t) const {
    auto new_types = std::tuple_cat(types_, std::make_tuple(t));
    auto new_funcs = std::tuple_cat(funcs_, t.embedded_funcs);
    return ExtensionBuilder<decltype(new_funcs), decltype(new_types),
                            RequiredCapabilityTuple, InitFn, DeinitFn>{
        new_funcs, new_types, required_capabilities_,
        require_atleast_min(t.descriptor.vef_desc.protocol)};
  }

  // No-op. Preview capability builders that need runtime initialization before
  // vef_register_impl() runs should wrap ExtensionBuilder and override this.
  void init() const {}

  // Registers a capability requirement. Pass a capability object declared in
  // your extension (e.g., a PingCapability or StorageCapability) by reference.
  // If the server does not support the capability, the extension will fail to
  // load.
  template <typename Capability>
  constexpr auto with(Capability &cap) const {
    auto new_caps =
        std::tuple_cat(required_capabilities_, std::make_tuple(&cap));
    return ExtensionBuilder<FuncTuple, TypeTuple, decltype(new_caps), InitFn,
                            DeinitFn>{funcs_, types_, new_caps,
                                      require_atleast_min(VEF_PROTOCOL_3)};
  }

  // Registers a function to run once, extension-side, at extension load (every
  // server startup and at install), after the extension is validated and
  // accepted. The function runs in the extension process with no server access;
  // for server-interacting setup use a capability's on_populate instead. Use
  // this for local one-time init such as choosing CPU/ISA-specific function
  // pointers or allocating extension-owned state.
  template <void (*Fn)()>
  constexpr auto on_init() const {
    return ExtensionBuilder<FuncTuple, TypeTuple, RequiredCapabilityTuple, Fn,
                            DeinitFn>{funcs_, types_, required_capabilities_,
                                      min_protocol_};
  }

  // Registers a function to run at extension unload (server shutdown or
  // uninstall). Like on_init(), it runs extension-side with no server access.
  template <void (*Fn)()>
  constexpr auto on_deinit() const {
    return ExtensionBuilder<FuncTuple, TypeTuple, RequiredCapabilityTuple,
                            InitFn, Fn>{funcs_, types_, required_capabilities_,
                                        min_protocol_};
  }

  // For testing only — forces the extension to require protocol p regardless
  // of which features are registered.
  constexpr auto test_only_require_protocol(vef_protocol_t p) const {
    return ExtensionBuilder<FuncTuple, TypeTuple, RequiredCapabilityTuple,
                            InitFn, DeinitFn>{funcs_, types_,
                                              required_capabilities_, p};
  }
};

// make_extension() — creates an empty ExtensionBuilder. Chain .type(), .func(),
// and .with() calls to register types, functions, and capabilities, then pass
// the result to VEF_GENERATE_ENTRY_POINTS.
constexpr auto make_extension() {
  return ExtensionBuilder<std::tuple<>, std::tuple<>, std::tuple<>>{
      {}, {}, {}, VEF_PROTOCOL_1};
}
}  // namespace vsql

// VEF_GENERATE_REGISTRATION (vsql variant)
//
// Creates a _vef_do_register() helper that performs extension registration.
// Use this when you need to customize vef_register behavior (e.g., to patch
// descriptors after registration for testing). Otherwise use
// VEF_GENERATE_ENTRY_POINTS which generates the full extern "C" entry points.

#define VEF_GENERATE_REGISTRATION(ext)                                   \
  namespace {                                                            \
  vef_registration_t _vef_reg;                                           \
  bool _vef_reg_initialized = false;                                     \
  }                                                                      \
                                                                         \
  static vef_registration_t *_vef_do_register(vef_register_arg_t *arg) { \
    using namespace vsql;                                                \
    static constexpr auto kExt = (ext);                                  \
    using ExtType = decltype(kExt);                                      \
    static vef_func_desc_t                                               \
        *func_ptrs[ExtType::kFuncCount > 0 ? ExtType::kFuncCount : 1];   \
    static vef_type_desc_t                                               \
        *type_ptrs[ExtType::kTypeCount > 0 ? ExtType::kTypeCount : 1];   \
    static vef_required_capability_t                                     \
        required_capability_reqs[ExtType::kRequiredCapabilityCount > 0   \
                                     ? ExtType::kRequiredCapabilityCount \
                                     : 1];                               \
    return villagesql::detail::vef_register_impl<                        \
        decltype(kExt), ExtType::kFuncCount, ExtType::kTypeCount,        \
        ExtType::kRequiredCapabilityCount>(                              \
        _vef_reg, _vef_reg_initialized, func_ptrs, type_ptrs,            \
        required_capability_reqs, arg, kExt);                            \
  }

// VEF_GENERATE_ENTRY_POINTS (vsql variant)
//
// Generates the extern "C" vef_register and vef_unregister functions.
// Must be called in a .cc file, not a header (defines functions/variables).

#define VEF_GENERATE_ENTRY_POINTS(ext)                                   \
  namespace {                                                            \
  vef_registration_t vef_reg_;                                           \
  bool vef_reg_initialized_ = false;                                     \
  }                                                                      \
                                                                         \
  extern "C" vef_registration_t *vef_register(vef_register_arg_t *arg) { \
    using namespace vsql;                                                \
    static constexpr auto kExt = (ext);                                  \
    using ExtType = decltype(kExt);                                      \
    static vef_func_desc_t                                               \
        *func_ptrs[ExtType::kFuncCount > 0 ? ExtType::kFuncCount : 1];   \
    static vef_type_desc_t                                               \
        *type_ptrs[ExtType::kTypeCount > 0 ? ExtType::kTypeCount : 1];   \
    static vef_required_capability_t                                     \
        required_capability_reqs[ExtType::kRequiredCapabilityCount > 0   \
                                     ? ExtType::kRequiredCapabilityCount \
                                     : 1];                               \
    return villagesql::detail::vef_register_impl<                        \
        decltype(kExt), ExtType::kFuncCount, ExtType::kTypeCount,        \
        ExtType::kRequiredCapabilityCount>(                              \
        vef_reg_, vef_reg_initialized_, func_ptrs, type_ptrs,            \
        required_capability_reqs, arg, kExt);                            \
  }                                                                      \
                                                                         \
  extern "C" void vef_unregister(vef_unregister_arg_t *arg,              \
                                 vef_registration_t *reg) {              \
    (void)arg;                                                           \
    (void)reg;                                                           \
    using namespace vsql;                                                \
    static constexpr auto kExt = (ext);                                  \
    if constexpr (decltype(kExt)::kDeinitFn != nullptr) {                \
      decltype(kExt)::kDeinitFn();                                       \
    }                                                                    \
  }

#endif  // VILLAGESQL_VSQL_EXTENSION_BUILDER_H
