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

#ifndef VILLAGESQL_SDK_FUNC_BUILDER_H
#define VILLAGESQL_SDK_FUNC_BUILDER_H

// Do not write new code against this header. It is included only for backward
// compatibility and will be removed before Beta. Use the C++ API in
// <villagesql/vsql.h> instead.

// V1 function builder — supports raw ABI (vef_vdf_func_t) VDFs only.
//
// For the typed C++ API (IntArg, RealArg, Span<>, aggregate support, etc.)
// use villagesql/vsql/func_builder.h instead, which is included automatically
// via villagesql/vsql.h.

#include <array>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <string_view>
#include <type_traits>

#include <villagesql/abi/types.h>

// Forward-declare materialize_func_desc in vsql::func_builder.  The definition
// lives in vef_register.h.  The using-declaration below makes
// villagesql::func_builder::materialize_func_desc an alias to the same entity,
// so ADL on old-API types and the explicit using in vef_fill_func_ptrs both
// resolve to one function — eliminating overload ambiguity.
namespace vsql {
namespace func_builder {
template <typename FuncData, size_t Index>
vef_func_desc_t *materialize_func_desc(const FuncData &func_data);
}  // namespace func_builder
}  // namespace vsql

namespace villagesql {
namespace func_builder {

template <size_t NumParams>
struct StaticFuncDesc;

// =============================================================================
// Type Definitions
// =============================================================================

constexpr size_t kMaxParams = 8;

using ExtFunc = vef_vdf_func_t;

// =============================================================================
// Type Constants
// =============================================================================

constexpr const char *STRING = "STRING";
constexpr const char *INT = "INT";
constexpr const char *REAL = "REAL";

constexpr vef_type_t to_vef_type(const char *name);

// =============================================================================
// Protocol Helpers
// =============================================================================

inline vef_invalue_t promote_v1(const vef_invalue_v1_t &v) {
  vef_invalue_t out{};
  memcpy(&out, &v, sizeof(vef_invalue_v1_t));
  return out;
}

inline vef_invalue_t get_invalue(vef_context_t *ctx, vef_vdf_args_t *args,
                                 unsigned int i) {
  if (ctx->protocol >= VEF_PROTOCOL_2) return *args->values[i];
  return promote_v1(args->values_v1[i]);
}

// =============================================================================
// FuncWithMetadata
// =============================================================================

struct FuncWithMetadata {
  constexpr FuncWithMetadata()
      : f(nullptr),
        prerun(nullptr),
        postrun(nullptr),
        clear(nullptr),
        accumulate(nullptr),
        return_type{},
        param_types{},
        num_params(0),
        buffer_size(0),
        deterministic(false),
        check_params_cache_bound(nullptr) {}

  ExtFunc f;
  vef_prerun_func_t prerun;
  vef_postrun_func_t postrun;
  vef_vdf_clear_func_t clear;
  vef_vdf_accumulate_func_t accumulate;
  vef_type_t return_type;
  std::array<vef_type_t, kMaxParams> param_types;
  size_t num_params;
  size_t buffer_size;
  bool deterministic;
  bool (*check_params_cache_bound)();
};

// =============================================================================
// Raw Encode/Decode Function Types (V1 style)
// =============================================================================

// FROM_STRING raw function: converts string to binary representation.
// Returns false on success, true on error. Set *length = SIZE_MAX for NULL.
using RawFromStringFunc = bool (*)(unsigned char *buffer, size_t buffer_size,
                                   const char *from, size_t from_len,
                                   size_t *length);

// TO_STRING raw function: converts binary representation to string.
// Returns false on success, true on error.
using RawToStringFunc = bool (*)(const unsigned char *buffer,
                                 size_t buffer_size, char *to, size_t to_size,
                                 size_t *to_length);

// =============================================================================
// V1PerArgWrapper — V1 per-argument function style backward compatibility
// =============================================================================
//
// Wraps functions with the old per-argument signature:
//   void func(vef_context_t*, vef_invalue_t* arg0, ..., vef_vdf_result_t*)
// into the current vef_vdf_func_t signature. Used by make_func<> when the
// template argument is not exactly vef_vdf_func_t.

template <auto Func, size_t NumParams>
struct V1PerArgWrapper {
  static void invoke(vef_context_t *ctx, vef_vdf_args_t *args,
                     vef_vdf_result_t *result) {
    invoke_impl(ctx, args, result, std::make_index_sequence<NumParams>{});
  }

 private:
  template <size_t... Is>
  static void invoke_impl(vef_context_t *ctx, vef_vdf_args_t *args,
                          vef_vdf_result_t *result,
                          std::index_sequence<Is...>) {
    std::array<vef_invalue_t, NumParams> vals = {
        get_invalue(ctx, args, static_cast<unsigned>(Is))...};
    Func(ctx, &vals[Is]..., result);
  }
};

// =============================================================================
// FromStringWrapper / ToStringWrapper (V1 raw pointer wrappers)
// =============================================================================

template <RawFromStringFunc Func>
struct FromStringWrapper {
  static void invoke(vef_context_t *ctx, vef_vdf_args_t *args,
                     vef_vdf_result_t *result) {
    vef_invalue_t arg = get_invalue(ctx, args, 0);

    if (arg.is_null) {
      result->type = VEF_RESULT_NULL;
      return;
    }

    size_t length;
    bool failed = Func(result->bin_buf, result->max_bin_len, arg.str_value,
                       arg.str_len, &length);

    if (failed) {
      result->type = VEF_RESULT_WARNING;
      constexpr size_t kMaxInputDisplay = 64;
      size_t display_len = arg.str_len;
      const char *ellipsis = "";
      if (display_len > kMaxInputDisplay) {
        display_len = kMaxInputDisplay;
        ellipsis = "...";
      }
      snprintf(result->error_msg, VEF_MAX_ERROR_LEN,
               "failed to parse string '%.*s%s'", static_cast<int>(display_len),
               arg.str_value, ellipsis);
      return;
    }

    if (length == SIZE_MAX) {
      result->type = VEF_RESULT_NULL;
      return;
    }

    result->type = VEF_RESULT_VALUE;
    result->actual_len = length;
  }
};

template <RawToStringFunc Func>
struct ToStringWrapper {
  static void invoke(vef_context_t *ctx, vef_vdf_args_t *args,
                     vef_vdf_result_t *result) {
    vef_invalue_t arg = get_invalue(ctx, args, 0);

    if (arg.is_null) {
      result->type = VEF_RESULT_NULL;
      return;
    }

    size_t to_length;
    bool failed = Func(arg.bin_value, arg.bin_len, result->str_buf,
                       result->max_str_len, &to_length);

    if (failed) {
      result->type = VEF_RESULT_ERROR;
      return;
    }

    result->type = VEF_RESULT_VALUE;
    result->actual_len = to_length;
  }
};

// =============================================================================
// StaticFuncDesc
// =============================================================================

template <size_t NumParams>
struct StaticFuncDesc {
  const char *name_;
  vef_type_t params_[NumParams > 0 ? NumParams : 1];
  vef_type_t return_type_;
  ExtFunc vdf_;
  vef_prerun_func_t prerun_;
  vef_postrun_func_t postrun_;
  vef_vdf_clear_func_t clear_;
  vef_vdf_accumulate_func_t accumulate_;
  size_t buffer_size_;
  bool deterministic_;
  bool (*check_params_cache_bound_)();

  constexpr StaticFuncDesc(const char *name, const FuncWithMetadata &meta)
      : name_(name),
        params_{},
        return_type_(meta.return_type),
        vdf_(meta.f),
        prerun_(meta.prerun),
        postrun_(meta.postrun),
        clear_(meta.clear),
        accumulate_(meta.accumulate),
        buffer_size_(meta.buffer_size),
        deterministic_(meta.deterministic),
        check_params_cache_bound_(meta.check_params_cache_bound) {
    for (size_t i = 0; i < NumParams && i < meta.num_params; ++i) {
      params_[i] = meta.param_types[i];
    }
  }

  constexpr const char *name() const { return name_; }
  constexpr size_t num_params() const { return NumParams; }
  constexpr const vef_type_t *params() const { return params_; }
  constexpr vef_type_t return_type() const { return return_type_; }
  constexpr ExtFunc vdf() const { return vdf_; }
  constexpr vef_prerun_func_t prerun() const { return prerun_; }
  constexpr vef_postrun_func_t postrun() const { return postrun_; }
  constexpr vef_vdf_clear_func_t clear() const { return clear_; }
  constexpr vef_vdf_accumulate_func_t accumulate() const { return accumulate_; }
  constexpr size_t buffer_size() const { return buffer_size_; }
  constexpr bool deterministic() const { return deterministic_; }
  constexpr auto check_params_cache_bound() const -> bool (*)() {
    return check_params_cache_bound_;
  }

  constexpr void init_name() const {}
};

using vsql::func_builder::materialize_func_desc;

// =============================================================================
// FuncBuilder (V1 — raw vef_vdf_func_t only)
// =============================================================================

template <auto Func, size_t NumParams>
struct FuncBuilder {
  constexpr FuncBuilder()
      : name_(nullptr),
        return_type_(nullptr),
        param_types_{},
        buffer_size_(0),
        prerun_(nullptr),
        postrun_(nullptr),
        deterministic_(false) {}

  const char *name_;
  const char *return_type_;
  std::array<const char *, NumParams> param_types_;
  size_t buffer_size_;
  vef_prerun_func_t prerun_;
  vef_postrun_func_t postrun_;
  bool deterministic_;

  constexpr FuncBuilder<Func, NumParams> &returns(const char *t) {
    return_type_ = t;
    return *this;
  }

  constexpr FuncBuilder<Func, NumParams + 1> param(const char *t) const {
    FuncBuilder<Func, NumParams + 1> next;
    next.name_ = name_;
    next.return_type_ = return_type_;
    next.buffer_size_ = buffer_size_;
    next.prerun_ = prerun_;
    next.postrun_ = postrun_;
    next.deterministic_ = deterministic_;
    for (size_t i = 0; i < NumParams; ++i) {
      next.param_types_[i] = param_types_[i];
    }
    next.param_types_[NumParams] = t;
    return next;
  }

  constexpr FuncBuilder<Func, NumParams> &buffer_size(size_t s) {
    buffer_size_ = s;
    return *this;
  }

  constexpr FuncBuilder<Func, NumParams> &deterministic(bool d = true) {
    deterministic_ = d;
    return *this;
  }

  template <vef_prerun_func_t Hook>
  constexpr FuncBuilder<Func, NumParams> &prerun() {
    prerun_ = Hook;
    return *this;
  }

  template <vef_postrun_func_t Hook>
  constexpr FuncBuilder<Func, NumParams> &postrun() {
    postrun_ = Hook;
    return *this;
  }

  constexpr StaticFuncDesc<NumParams> build() const {
    static_assert(NumParams <= kMaxParams,
                  "Too many parameters (max is kMaxParams)");
    FuncWithMetadata meta{};
    if constexpr (std::is_same_v<decltype(Func), vef_vdf_func_t>) {
      meta.f = Func;
    } else {
      meta.f = &V1PerArgWrapper<Func, NumParams>::invoke;
    }
    meta.prerun = prerun_;
    meta.postrun = postrun_;
    meta.return_type = to_vef_type(return_type_);
    meta.num_params = NumParams;
    meta.buffer_size = buffer_size_;
    meta.deterministic = deterministic_;
    for (size_t i = 0; i < NumParams; ++i) {
      meta.param_types[i] = to_vef_type(param_types_[i]);
    }
    return StaticFuncDesc<NumParams>(name_, meta);
  }
};

// =============================================================================
// FuncBuilderNoImpl (V1 type conversion functions)
// =============================================================================

template <size_t NumParams>
struct FuncBuilderNoImpl {
  constexpr FuncBuilderNoImpl() : name_(nullptr) {}

  const char *name_;

  // from_string: STRING -> custom type (raw pointer signature)
  template <RawFromStringFunc Func>
  constexpr StaticFuncDesc<1> from_string(const char *type_name) const {
    FuncWithMetadata meta{};
    meta.f = &FromStringWrapper<Func>::invoke;
    meta.return_type = to_vef_type(type_name);
    meta.param_types[0] = to_vef_type(STRING);
    meta.num_params = 1;
    meta.buffer_size = 0;
    return StaticFuncDesc<1>(name_, meta);
  }

  // to_string: custom type -> STRING (raw pointer signature)
  template <RawToStringFunc Func>
  constexpr StaticFuncDesc<1> to_string(const char *type_name) const {
    FuncWithMetadata meta{};
    meta.f = &ToStringWrapper<Func>::invoke;
    meta.return_type = to_vef_type(STRING);
    meta.param_types[0] = to_vef_type(type_name);
    meta.num_params = 1;
    meta.buffer_size = 0;
    return StaticFuncDesc<1>(name_, meta);
  }
};

// =============================================================================
// Entry Points
// =============================================================================

// make_func<&impl>("name") — raw functions (vef_vdf_func_t or per-argument
// vef_invalue_t* style). For typed C++ functions use villagesql/vsql.h.
template <auto Func>
constexpr FuncBuilder<Func, 0> make_func(const char *name) {
  FuncBuilder<Func, 0> builder;
  builder.name_ = name;
  return builder;
}

// make_func("name") — entry point for raw type conversion functions.
constexpr FuncBuilderNoImpl<0> make_func(const char *name) {
  FuncBuilderNoImpl<0> builder;
  builder.name_ = name;
  return builder;
}

// =============================================================================
// Internal Implementation
// =============================================================================

constexpr vef_type_t to_vef_type(const char *name) {
  std::string_view sv(name);
  if (sv == "STRING") return vef_type_t{VEF_TYPE_STRING, nullptr};
  if (sv == "INT") return vef_type_t{VEF_TYPE_INT, nullptr};
  if (sv == "REAL") return vef_type_t{VEF_TYPE_REAL, nullptr};
  return vef_type_t{VEF_TYPE_CUSTOM, name};
}

}  // namespace func_builder
}  // namespace villagesql

#endif  // VILLAGESQL_SDK_FUNC_BUILDER_H
