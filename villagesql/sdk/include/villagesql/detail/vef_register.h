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
#include <villagesql/detail/capability_base.h>
#include <villagesql/detail/capability_traits.h>
#include <villagesql/sdk_version.h>

namespace vsql {

// Define materialize_func_desc here so it is available to both old
// (villagesql::func_builder) and new (vsql::func_builder) API users without
// requiring an additional include.  villagesql/func_builder.h re-exports this
// as villagesql::func_builder::materialize_func_desc via a using-declaration,
// so ADL on old-API types and the explicit using in vef_fill_func_ptrs both
// resolve to the same entity — eliminating overload ambiguity.
namespace func_builder {

// Detection idiom: true when FuncData exposes max_result_length() (the typed
// builder does; older stable raw builders do not). Allows materialize_func_desc
// to stay generic over old and new without changing the previous stable
// APIs/ABIs.
template <typename T, typename = void>
struct has_max_result_length : std::false_type {};
template <typename T>
struct has_max_result_length<
    T, std::void_t<decltype(std::declval<const T &>().max_result_length())>>
    : std::true_type {};

template <typename FuncData, size_t Index>
__attribute__((visibility("hidden"))) vef_func_desc_t *materialize_func_desc(
    const FuncData &func_data) {
  static vef_signature_t signature;
  static vef_func_desc_t desc;

  signature.param_count = static_cast<unsigned int>(func_data.num_params());
  // For varargs functions num_params() returns the VEF_PARAM_VARARGS sentinel
  // and params() is meaningless; null it out so the server reads
  // param_count alone.
  signature.params = (func_data.num_params() > 0 &&
                      func_data.num_params() != VEF_PARAM_VARARGS)
                         ? func_data.params()
                         : nullptr;
  signature.return_type = func_data.return_type();

  desc.protocol = VEF_PROTOCOL_4;
  desc.name = func_data.name();
  desc.signature = &signature;
  desc.vdf = func_data.vdf();
  desc.prerun = func_data.prerun();
  desc.postrun = func_data.postrun();
  desc.buffer_size = func_data.buffer_size();
  // max_result_length is only present on the typed builder; the older stable
  // raw builders do not expose it, so default to 0 (fall back to argument
  // width).
  if constexpr (has_max_result_length<FuncData>::value) {
    desc.max_result_length = func_data.max_result_length();
  } else {
    desc.max_result_length = 0;
  }
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

template <typename T, typename = void>
struct has_sql_callable : std::false_type {};
template <typename T>
struct has_sql_callable<
    T, std::void_t<decltype(std::declval<const T &>().sql_callable())>>
    : std::true_type {};

template <typename Func>
constexpr bool get_sql_callable(const Func &func) {
  if constexpr (has_sql_callable<Func>::value) {
    return func.sql_callable();
  }
  return true;
}

template <typename Ext, size_t... Is>
void vef_fill_func_sql_callable(bool *arr, const Ext &e,
                                std::index_sequence<Is...>) {
  ((arr[Is] = get_sql_callable(e.template func_at<Is>())), ...);
}

// Fills arr[I] with the vef_type_desc_t* for each type.
template <typename Ext, size_t... Is>
void vef_fill_type_ptrs(vef_type_desc_t **arr, const Ext &e,
                        std::index_sequence<Is...>) {
  ((arr[Is] =
        const_cast<vef_type_desc_t *>(&get_vef_desc(e.template type_at<Is>()))),
   ...);
}

// Detection idiom: true if CapabilityTraits has a CapabilityConfigType
// member -- present only on traits whose capability carries a
// configuration struct alongside the vtable.
template <typename T, typename = void>
struct has_capability_config : std::false_type {};
template <typename T>
struct has_capability_config<T, std::void_t<typename T::CapabilityConfigType>>
    : std::true_type {};

// Fill arr[I] with the wire entry for this Capability. The vtable_dest
// points at the abi-pointer slot inside the extension's wrapper, as
// determined by the trait specialization; the server writes the vtable
// pointer there at registration time if the (vtable_hash,
// capability_config_hash) tuple matches a registered capability version.
template <size_t I, typename Ext>
void vef_fill_one_capability_req(vef_required_capability_t *arr, const Ext &e) {
  auto *cap_ptr = e.template required_capability_at<I>();
  using Capability = std::remove_cv_t<std::remove_pointer_t<decltype(cap_ptr)>>;
  using Traits = ::vsql::detail::CapabilityTraits<Capability>;

  arr[I].name = Traits::kName;
  arr[I].vtable_dest =
      static_cast<void **>(Traits::vtable_destination(cap_ptr));
  arr[I].vtable_hash = Traits::kVtableHash;
  if constexpr (has_capability_config<Traits>::value) {
    arr[I].capability_config = Traits::capability_config(cap_ptr);
    arr[I].capability_config_hash = Traits::kCapabilityConfigHash;
  } else {
    arr[I].capability_config = nullptr;
    arr[I].capability_config_hash = nullptr;
  }
}

template <typename Ext, size_t... Is>
void vef_fill_required_capability_reqs(vef_required_capability_t *arr,
                                       const Ext &e,
                                       std::index_sequence<Is...>) {
  (vef_fill_one_capability_req<Is>(arr, e), ...);
}

// Walk the builder's RequiredCapabilityTuple and consume the matching
// PendingCapability registry entry for each. Returns a const char* error
// (a string literal or static-buffer pointer) on the first problem:
//   * pointer passed to .with() is not a CapabilityBase (or is from
//     another .so) — registry lookup misses.
//   * same instance passed to .with() more than once — registry entry is
//     already consumed.
// Returns nullptr on success.
template <size_t I, typename Ext>
const char *vef_consume_one_capability(const Ext &e) {
  const void *cap_ptr =
      static_cast<const void *>(e.template required_capability_at<I>());
  ::vsql::detail::PendingCapability *node =
      ::vsql::detail::find_pending(cap_ptr);
  static char error_buf[192];
  if (node == nullptr) {
    snprintf(error_buf, sizeof(error_buf),
             ".with() received an object that does not inherit "
             "vsql::detail::CapabilityBase; not a registered capability");
    return error_buf;
  }
  if (node->consumed) {
    snprintf(error_buf, sizeof(error_buf),
             "capability '%s' passed to .with() more than once",
             node->cpp_type_name ? node->cpp_type_name : "(unknown)");
    return error_buf;
  }
  node->consumed = true;
  return nullptr;
}

template <typename Ext, size_t... Is>
const char *vef_consume_required_capabilities(const Ext &e,
                                              std::index_sequence<Is...>) {
  const char *err = nullptr;
  // Short-circuit on first error using fold expression.
  (void)((err = vef_consume_one_capability<Is>(e), err != nullptr) || ...);
  return err;
}

// After every .with() has marked its matching entry consumed, any entry
// still unconsumed is a declared-but-never-registered capability.
// Returns the C++ type name of the first such entry, or nullptr if all
// consumed.
__attribute__((visibility("hidden"))) inline const char *
vef_first_unconsumed_capability() {
  for (::vsql::detail::PendingCapability *node =
           ::vsql::detail::pending_capabilities_head();
       node != nullptr; node = node->next) {
    if (!node->consumed) {
      return node->cpp_type_name ? node->cpp_type_name : "(unknown)";
    }
  }
  return nullptr;
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

template <typename T, typename = void>
struct has_check_signature : std::false_type {};
template <typename T>
struct has_check_signature<
    T, std::void_t<decltype(std::declval<const T &>().check_signature())>>
    : std::true_type {};

template <typename Ext, size_t... Is>
const char *vef_check_func_signatures(const Ext &e,
                                      std::index_sequence<Is...>) {
  static char error_buf[512];
  const char *invalid = nullptr;
  auto check_one = [&invalid](const auto &func) {
    if (invalid) return;
    using F = std::decay_t<decltype(func)>;
    if constexpr (has_check_signature<F>::value) {
      auto check_fn = func.check_signature();
      if (check_fn == nullptr) return;
      if (const char *err =
              check_fn(func.params(), func.num_params(), func.return_type())) {
        snprintf(error_buf, sizeof(error_buf), "VDF '%s': %s", func.name(),
                 err);
        invalid = error_buf;
      }
    }
  };
  (check_one(e.template func_at<Is>()), ...);
  return invalid;
}

// Core registration logic called by VEF_GENERATE_ENTRY_POINTS.
// The counts are explicit template parameters so that array sizes are
// compile-time constants without relying on VLAs.
template <typename Ext, size_t FuncCount, size_t TypeCount,
          size_t RequiredCapabilityCount>
vef_registration_t *vef_register_impl(
    vef_registration_t &reg, bool &initialized, vef_func_desc_t **func_ptrs,
    vef_type_desc_t **type_ptrs, bool *func_sql_callable_ptrs,
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
    vef_fill_func_sql_callable(func_sql_callable_ptrs, ext,
                               std::make_index_sequence<FuncCount>{});
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

  // Reset all CapabilityBase entries for this registration attempt. The
  // capability objects are static, so a repeated vef_register invocation for
  // the same loaded object must not inherit consumed=true from an earlier
  // invocation.
  ::vsql::detail::reset_pending_consumed();

  if constexpr (RequiredCapabilityCount > 0) {
    if (const char *err = vef_consume_required_capabilities(
            ext, std::make_index_sequence<RequiredCapabilityCount>{})) {
      reg.protocol = arg->protocol;
      reg.error_msg = const_cast<char *>(err);
      return &reg;
    }
  }

  if (const char *unregistered = vef_first_unconsumed_capability()) {
    static char unreg_error_buf[192];
    snprintf(unreg_error_buf, sizeof(unreg_error_buf),
             "capability '%s' was declared but never passed to .with(); "
             "every CapabilityBase-derived static must be registered via "
             ".with(cap) in the extension builder",
             unregistered);
    reg.protocol = arg->protocol;
    reg.error_msg = unreg_error_buf;
    return &reg;
  }

  if constexpr (FuncCount > 0) {
    const char *invalid_signature =
        vef_check_func_signatures(ext, std::make_index_sequence<FuncCount>{});
    if (invalid_signature) {
      static char error_buf[256];
      snprintf(error_buf, sizeof(error_buf), "%s", invalid_signature);
      reg.protocol = arg->protocol;
      reg.error_msg = error_buf;
      return &reg;
    }

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

  reg.protocol = VEF_PROTOCOL_4;
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
