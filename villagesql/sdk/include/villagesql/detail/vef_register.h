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
#include <villagesql/detail/capability_hash.h>
#include <villagesql/sdk_version.h>
#include <villagesql/vsql/capability_traits.h>

namespace vsql {

// Define materialize_func_desc here so it is available to both old
// (villagesql::func_builder) and new (vsql::func_builder) API users without
// requiring an additional include.  villagesql/func_builder.h re-exports this
// as villagesql::func_builder::materialize_func_desc via a using-declaration,
// so ADL on old-API types and the explicit using in vef_fill_func_ptrs both
// resolve to the same entity — eliminating overload ambiguity.
namespace func_builder {
template <typename FuncData, size_t Index>
__attribute__((visibility("hidden"))) vef_func_desc_t *materialize_func_desc(
    const FuncData &func_data) {
  static vef_signature_t signature;
  static vef_func_desc_t desc;

  signature.param_count = static_cast<unsigned int>(func_data.num_params());
  signature.params = func_data.num_params() > 0 ? func_data.params() : nullptr;
  signature.return_type = func_data.return_type();

  desc.protocol = VEF_PROTOCOL_2;
  desc.name = func_data.name();
  desc.signature = &signature;
  desc.vdf = func_data.vdf();
  desc.prerun = func_data.prerun();
  desc.postrun = func_data.postrun();
  desc.buffer_size = func_data.buffer_size();
  desc.deterministic = func_data.deterministic();
  desc.clear = func_data.clear();
  desc.accumulate = func_data.accumulate();

  return &desc;
}
}  // namespace func_builder

}  // namespace vsql

namespace villagesql {
namespace detail {

// has_descriptor<T>: true when T has a .descriptor member (the vsql TypeObject
// shape produced by vsql::make_type<>().build()). False for the low-level
// type_builder::TypeDescriptor which stores vef_desc directly.
template <typename T, typename = void>
struct has_descriptor : std::false_type {};
template <typename T>
struct has_descriptor<T, std::void_t<decltype(std::declval<T>().descriptor)>>
    : std::true_type {};

// Uniform accessor for the underlying vef_type_desc_t regardless of which
// shape (TypeObject or TypeDescriptor) the typed-API or low-level extension
// builder stored in its types_ tuple.
template <typename T>
inline const vef_type_desc_t &get_vef_desc(const T &t) {
  if constexpr (has_descriptor<T>::value) {
    return t.descriptor.vef_desc;
  } else {
    return t.vef_desc;
  }
}

// Fills arr[I] with the materialized vef_func_desc_t* for each function.
template <typename Ext, size_t... Is>
void vef_fill_func_ptrs(vef_func_desc_t **arr, const Ext &e,
                        std::index_sequence<Is...>) {
  using vsql::func_builder::materialize_func_desc;
  ((arr[Is] = materialize_func_desc<decltype(e.template func_at<Is>()), Is>(
        e.template func_at<Is>())),
   ...);
}

// Fills arr[I] with the vef_type_desc_t* for each type.
template <typename Ext, size_t... Is>
void vef_fill_type_ptrs(vef_type_desc_t **arr, const Ext &e,
                        std::index_sequence<Is...>) {
  ((arr[Is] =
        const_cast<vef_type_desc_t *>(&get_vef_desc(e.template type_at<Is>()))),
   ...);
}

// has_extension_data<T>: true when CapabilityTraits<T> provides extension_data.
template <typename Traits, typename Cap, typename = void>
struct has_extension_data : std::false_type {};
template <typename Traits, typename Cap>
struct has_extension_data<
    Traits, Cap,
    std::void_t<decltype(Traits::extension_data(std::declval<Cap *>()))>>
    : std::true_type {};

// Detection idiom: true if CapabilityTraits has a DescriptorType member.
template <typename T, typename = void>
struct has_capability_descriptor : std::false_type {};
template <typename T>
struct has_capability_descriptor<T, std::void_t<typename T::DescriptorType>>
    : std::true_type {};

// Fill arr[I] with the wire entry for this Capability. The vtable_dest
// points at the abi-pointer slot inside the extension's wrapper, as
// determined by the trait specialization; the server writes the vtable
// pointer there at registration time if all compat checks pass.
template <size_t I, typename Ext>
void vef_fill_one_capability_req(vef_required_capability_t *arr, const Ext &e) {
  auto *cap_ptr = e.template required_capability_at<I>();
  using Capability = std::remove_cv_t<std::remove_pointer_t<decltype(cap_ptr)>>;
  using Traits = ::vsql::detail::CapabilityTraits<Capability>;

  arr[I].name = Traits::kName;
  arr[I].vtable_dest =
      static_cast<void **>(Traits::vtable_destination(cap_ptr));
  arr[I].abi_type_hash =
      villagesql::detail::abi_type_hash<typename Traits::AbiType>();
  arr[I].min_version = Traits::kAbiVersion;
  if constexpr (has_extension_data<Traits, Capability>::value) {
    arr[I].extension_data = Traits::extension_data(cap_ptr);
  } else {
    arr[I].extension_data = nullptr;
  }
  if constexpr (has_capability_descriptor<Traits>::value) {
    arr[I].descriptor_abi_hash =
        villagesql::detail::abi_type_hash<typename Traits::DescriptorType>();
  } else {
    arr[I].descriptor_abi_hash = 0;
  }
}

template <typename Ext, size_t... Is>
void vef_fill_required_capability_reqs(vef_required_capability_t *arr,
                                       const Ext &e,
                                       std::index_sequence<Is...>) {
  (vef_fill_one_capability_req<Is>(arr, e), ...);
}

// Calls params_init_fn() and params_to_strings_init_fn() for each type that
// has one. These fields live on the vsql TypeObject only (parameterized types
// flow through the typed C++ API); low-level type_builder::TypeDescriptor has
// no init fns. has_descriptor<> gates the access so low-level extensions
// compile and run with a no-op loop body.
template <typename T>
inline void call_type_init_fns(const T &t) {
  if constexpr (has_descriptor<T>::value) {
    if (t.params_init_fn) t.params_init_fn();
    if (t.params_to_strings_init_fn) t.params_to_strings_init_fn();
  }
}

template <typename Ext, size_t... Is>
void vef_init_type_params(const Ext &e, std::index_sequence<Is...>) {
  (call_type_init_fns(e.template type_at<Is>()), ...);
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
// The counts are explicit template parameters so that array sizes are
// compile-time constants without relying on VLAs.
template <typename Ext, size_t FuncCount, size_t TypeCount,
          size_t RequiredCapabilityCount>
vef_registration_t *vef_register_impl(
    vef_registration_t &reg, bool &initialized, vef_func_desc_t **func_ptrs,
    vef_type_desc_t **type_ptrs,
    vef_required_capability_t *required_capability_reqs,
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

  if constexpr (FuncCount > 0) {
    vef_init_auto_names(ext, std::make_index_sequence<FuncCount>{});
    vef_fill_func_ptrs(func_ptrs, ext, std::make_index_sequence<FuncCount>{});
  }
  if constexpr (TypeCount > 0) {
    vef_fill_type_ptrs(type_ptrs, ext, std::make_index_sequence<TypeCount>{});
    vef_init_type_params(ext, std::make_index_sequence<TypeCount>{});
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
  reg.required_capability_count = RequiredCapabilityCount;
  reg.required_capabilities =
      RequiredCapabilityCount > 0 ? required_capability_reqs : nullptr;

  initialized = true;
  return &reg;
}

}  // namespace detail
}  // namespace villagesql

#endif  // VILLAGESQL_DETAIL_VEF_REGISTER_H
