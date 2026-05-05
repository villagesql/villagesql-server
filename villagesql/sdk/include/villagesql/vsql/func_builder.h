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

#ifndef VILLAGESQL_VSQL_FUNC_BUILDER_H
#define VILLAGESQL_VSQL_FUNC_BUILDER_H

// New-style (vsql) function builder.
//
// This file provides the typed C++ function registration API. Functions must
// use typed argument and result wrappers (IntArg, RealArg, StringArg,
// CustomArg, etc.) rather than raw ABI types (vef_context_t*, vef_invalue_t*,
// vef_vdf_result_t*).
//
// Raw vef_vdf_func_t function pointers and the deprecated vef_context_t* first
// parameter style are rejected at compile time. Use villagesql/func_builder.h
// (the v1 API) if you need to register raw ABI functions.
//
// For full usage documentation see villagesql/vsql.h.

#include <algorithm>
#include <array>
#include <cinttypes>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

#include <villagesql/abi/types.h>
#include <villagesql/vsql/func_types.h>
#include <villagesql/vsql/type_params_cache.h>

namespace vsql {

// Storage characteristics resolved from type parameters.
struct ResolvedTypeParams {
  int64_t persisted_length;
  int64_t max_decode_buffer_length;
};

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

// Deliberately unimplemented — produces a compile error with a descriptive
// name when build() detects an invalid aggregate configuration.
void config_error__aggregate_must_set_both_clear_and_accumulate();

// Auto-generated prerun/postrun for aggregate state management.
template <typename State>
void auto_prerun(vef_context_t *, vef_prerun_args_t *,
                 vef_prerun_result_t *result) {
  result->user_data = new State{};
  result->type = VEF_RESULT_VALUE;
}

template <typename State>
void auto_postrun(vef_context_t *, vef_postrun_args_t *args,
                  vef_postrun_result_t *) {
  delete static_cast<State *>(args->user_data);
}

// =============================================================================
// Internal Protocol Helpers
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
// FuncParamTypes / is_context_param
// =============================================================================

template <typename F>
struct FuncParamTypes;

template <typename R, typename... Args>
struct FuncParamTypes<R (*)(Args...)> {
  using type = std::tuple<Args...>;
};

// True for the deprecated ABI-style context parameter. Used by make_func to
// reject functions that still have vef_context_t* as their first parameter.
template <typename T>
struct is_context_param : std::false_type {};

template <>
struct is_context_param<vef_context_t *> : std::true_type {};

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
// Extension Author Function Signatures
// =============================================================================

// Encode: string -> binary. false=success, true=error. SIZE_MAX length = NULL.
using TypeEncodeFunc = bool (*)(std::string_view from, Span<unsigned char> buf,
                                size_t *length);
template <typename P>
using TypeEncodeWithParamsFunc = bool (*)(const P &, std::string_view from,
                                          Span<unsigned char> buf,
                                          size_t *length);

// Decode: binary -> string. false=success, true=error.
using TypeDecodeFunc = bool (*)(Span<const unsigned char> data, Span<char> out,
                                size_t *out_len);
template <typename P>
using TypeDecodeWithParamsFunc = bool (*)(const P &,
                                          Span<const unsigned char> data,
                                          Span<char> out, size_t *out_len);

// Compare: returns <0, 0, or >0.
using TypeCompareFunc = int (*)(Span<const unsigned char> a,
                                Span<const unsigned char> b);
template <typename P>
using TypeCompareWithParamsFunc = int (*)(const P &,
                                          Span<const unsigned char> a,
                                          Span<const unsigned char> b);

// Hash: returns hash code.
using TypeHashFunc = size_t (*)(Span<const unsigned char> data);
template <typename P>
using TypeHashWithParamsFunc = size_t (*)(const P &,
                                          Span<const unsigned char> data);

// intrinsic_default: returns string representation of the default value.
using IntrinsicDefaultFunc = std::string (*)(char *error_msg);
template <typename P>
using IntrinsicDefaultWithParamsFunc = std::string (*)(const P &,
                                                       char *error_msg);

// int_to_params: converts MYTYPE(N) integer to parameter key-value pairs.
using IntToTypeParamsFunc = bool (*)(int64_t value,
                                     std::map<std::string, std::string> &params,
                                     char *error_msg);

// resolve_params: validates type parameters and computes storage sizes.
using ResolveTypeParamsFunc =
    bool (*)(const std::map<std::string, std::string> &params,
             vsql::ResolvedTypeParams *result, char *error_msg);

// =============================================================================
// Aggregate Callback Wrappers
// =============================================================================

// Wraps void(State&) -> vef_vdf_clear_func_t
template <typename State, auto Func>
void agg_clear_wrapper(vef_context_t *, vef_vdf_args_t *args) {
  Func(*static_cast<State *>(args->user_data));
}

// Wraps void(State&, TypedArgs...) -> vef_vdf_accumulate_func_t
template <typename State, auto Func, size_t NumParams>
struct AggAccumulateWrapper {
  static void invoke(vef_context_t *ctx, vef_vdf_args_t *args,
                     vef_vdf_result_t *result) {
    invoke_impl(ctx, args, result, std::make_index_sequence<NumParams>{});
  }

 private:
  using Params = typename FuncParamTypes<decltype(Func)>::type;

  template <typename T>
  static T make_arg(vef_invalue_t *v) {
    return T(v);
  }

  template <size_t... Is>
  static void invoke_impl(vef_context_t *ctx, vef_vdf_args_t *args,
                          vef_vdf_result_t *, std::index_sequence<Is...>) {
    auto &state = *static_cast<State *>(args->user_data);
    std::array<vef_invalue_t, NumParams> vals{
        get_invalue(ctx, args, static_cast<unsigned int>(Is))...};
    Func(state, make_arg<std::tuple_element_t<1 + Is, Params>>(&vals[Is])...);
  }
};

// Wraps T(const State&) or optional<T>(const State&) -> vef_vdf_func_t
template <typename State, auto Func>
struct AggResultWrapper {
  static void invoke(vef_context_t *, vef_vdf_args_t *args,
                     vef_vdf_result_t *result) {
    const auto &state = *static_cast<State *>(args->user_data);
    write_result(Func(state), result);
  }

 private:
  template <typename T>
  static void write_result(const std::optional<T> &val,
                           vef_vdf_result_t *result) {
    if (!val.has_value()) {
      result->type = VEF_RESULT_NULL;
    } else {
      write_scalar(*val, result);
    }
  }

  template <typename T>
  static void write_result(const T &val, vef_vdf_result_t *result) {
    write_scalar(val, result);
  }

  static void write_scalar(long long v, vef_vdf_result_t *r) {
    r->int_value = v;
    r->type = VEF_RESULT_VALUE;
  }

  static void write_scalar(double v, vef_vdf_result_t *r) {
    r->real_value = v;
    r->type = VEF_RESULT_VALUE;
  }

  static void write_scalar(const std::string &v, vef_vdf_result_t *r) {
    if (v.size() > r->max_str_len) {
      r->type = VEF_RESULT_ERROR;
      snprintf(r->error_msg, VEF_MAX_ERROR_LEN,
               "aggregate result (%zu bytes) exceeds buffer (%zu bytes)",
               v.size(), r->max_str_len);
      return;
    }
    memcpy(r->str_buf, v.data(), v.size());
    r->actual_len = v.size();
    r->type = VEF_RESULT_VALUE;
  }
};

// =============================================================================
// Wrapper Template
// =============================================================================
//
// Generates a vef_vdf_func_t that unpacks vef_vdf_args_t and adapts each
// argument and result to the declared typed wrapper parameter of Func.
//
// Only typed C++ style is accepted:
//   void func(IntArg arg0, ..., IntResult result)
//
// Raw ABI style (vef_context_t* first param) is rejected at compile time.

template <auto Func, size_t NumParams>
struct Wrapper {
  static void invoke(vef_context_t *ctx, vef_vdf_args_t *args,
                     vef_vdf_result_t *result) {
    invoke_impl(ctx, args, result, std::make_index_sequence<NumParams>{});
  }

 private:
  template <size_t... Is>
  static void invoke_impl(vef_context_t *ctx, vef_vdf_args_t *args,
                          vef_vdf_result_t *result,
                          std::index_sequence<Is...>) {
    using Params = typename FuncParamTypes<decltype(Func)>::type;
    std::array<vef_invalue_t, NumParams> vals{
        get_invalue(ctx, args, static_cast<unsigned int>(Is))...};
    Func(make_arg<std::tuple_element_t<Is, Params>>(&vals[Is])...,
         make_result<std::tuple_element_t<NumParams, Params>>(result));
  }

  template <typename T>
  static T make_arg(vef_invalue_t *v) {
    return T(v);
  }

  template <typename T>
  static T make_result(vef_vdf_result_t *r) {
    return T(r);
  }
};

// =============================================================================
// Type Operation VDF Wrappers
// =============================================================================

// TypeEncodeVdfWrapper: wraps TypeEncodeFunc into a VDF.
// VDF signature: (STRING) -> CUSTOM(type).
template <auto Func>
struct TypeEncodeVdfWrapper {
  static void invoke(vef_context_t *ctx, vef_vdf_args_t *args,
                     vef_vdf_result_t *result) {
    vef_invalue_t arg = get_invalue(ctx, args, 0);
    if (arg.is_null) {
      result->type = VEF_RESULT_NULL;
      return;
    }
    size_t length;
    bool failed = Func({arg.str_value, arg.str_len},
                       {result->bin_buf, result->max_bin_len}, &length);
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
               "failed to encode '%.*s%s'", static_cast<int>(display_len),
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

// TypeDecodeVdfWrapper: wraps TypeDecodeFunc into a VDF.
// VDF signature: (CUSTOM(type)) -> STRING.
template <auto Func>
struct TypeDecodeVdfWrapper {
  static void invoke(vef_context_t *ctx, vef_vdf_args_t *args,
                     vef_vdf_result_t *result) {
    vef_invalue_t arg = get_invalue(ctx, args, 0);
    if (arg.is_null) {
      result->type = VEF_RESULT_NULL;
      return;
    }
    size_t out_len;
    bool failed = Func({arg.bin_value, arg.bin_len},
                       {result->str_buf, result->max_str_len}, &out_len);
    if (failed) {
      result->type = VEF_RESULT_ERROR;
      snprintf(result->error_msg, VEF_MAX_ERROR_LEN, "failed to decode value");
      return;
    }
    result->type = VEF_RESULT_VALUE;
    result->actual_len = out_len;
  }
};

// TypeCompareVdfWrapper: wraps TypeCompareFunc into a VDF.
// VDF signature: (CUSTOM(type), CUSTOM(type)) -> INT.
template <auto Func>
struct TypeCompareVdfWrapper {
  static void invoke(vef_context_t *ctx, vef_vdf_args_t *args,
                     vef_vdf_result_t *result) {
    vef_invalue_t a = get_invalue(ctx, args, 0);
    vef_invalue_t b = get_invalue(ctx, args, 1);
    if (a.is_null || b.is_null) {
      result->type = VEF_RESULT_NULL;
      return;
    }
    result->int_value =
        Func({a.bin_value, a.bin_len}, {b.bin_value, b.bin_len});
    result->type = VEF_RESULT_VALUE;
  }
};

// TypeHashVdfWrapper: wraps TypeHashFunc into a VDF.
// VDF signature: (CUSTOM(type)) -> INT.
template <auto Func>
struct TypeHashVdfWrapper {
  static void invoke(vef_context_t *ctx, vef_vdf_args_t *args,
                     vef_vdf_result_t *result) {
    vef_invalue_t arg = get_invalue(ctx, args, 0);
    if (arg.is_null) {
      result->type = VEF_RESULT_NULL;
      return;
    }
    result->int_value =
        static_cast<long long>(Func({arg.bin_value, arg.bin_len}));
    result->type = VEF_RESULT_VALUE;
  }
};

// Cache-aware wrappers for parameterized types.

template <auto Func>
struct TypeEncodeWithCacheVdfWrapper {
  using P = std::remove_cv_t<std::remove_reference_t<
      std::tuple_element_t<0, typename FuncParamTypes<decltype(Func)>::type>>>;

  static void invoke(vef_context_t *ctx, vef_vdf_args_t *args,
                     vef_vdf_result_t *result) {
    vef_invalue_t arg = get_invalue(ctx, args, 0);
    if (arg.is_null) {
      result->type = VEF_RESULT_NULL;
      return;
    }
    const P &p = type_params_cache_for<P>().get(result->type_params);
    size_t length;
    bool failed = Func(p, {arg.str_value, arg.str_len},
                       {result->bin_buf, result->max_bin_len}, &length);
    if (failed) {
      result->type = VEF_RESULT_ERROR;
      constexpr size_t kMaxInputDisplay = 64;
      size_t display_len = arg.str_len;
      const char *ellipsis = "";
      if (display_len > kMaxInputDisplay) {
        display_len = kMaxInputDisplay;
        ellipsis = "...";
      }
      snprintf(result->error_msg, VEF_MAX_ERROR_LEN,
               "failed to encode '%.*s%s'", static_cast<int>(display_len),
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

template <auto Func>
struct TypeDecodeWithCacheVdfWrapper {
  using P = std::remove_cv_t<std::remove_reference_t<
      std::tuple_element_t<0, typename FuncParamTypes<decltype(Func)>::type>>>;

  static void invoke(vef_context_t *ctx, vef_vdf_args_t *args,
                     vef_vdf_result_t *result) {
    vef_invalue_t arg = get_invalue(ctx, args, 0);
    if (arg.is_null) {
      result->type = VEF_RESULT_NULL;
      return;
    }
    const P &p = type_params_cache_for<P>().get(arg.type_params);
    size_t out_len;
    bool failed = Func(p, {arg.bin_value, arg.bin_len},
                       {result->str_buf, result->max_str_len}, &out_len);
    if (failed) {
      result->type = VEF_RESULT_ERROR;
      snprintf(result->error_msg, VEF_MAX_ERROR_LEN, "failed to decode value");
      return;
    }
    result->type = VEF_RESULT_VALUE;
    result->actual_len = out_len;
  }
};

template <auto Func>
struct TypeCompareWithCacheVdfWrapper {
  using P = std::remove_cv_t<std::remove_reference_t<
      std::tuple_element_t<0, typename FuncParamTypes<decltype(Func)>::type>>>;

  static void invoke(vef_context_t *ctx, vef_vdf_args_t *args,
                     vef_vdf_result_t *result) {
    vef_invalue_t a = get_invalue(ctx, args, 0);
    vef_invalue_t b = get_invalue(ctx, args, 1);
    if (a.is_null || b.is_null) {
      result->type = VEF_RESULT_NULL;
      return;
    }
    const P &p = type_params_cache_for<P>().get(a.type_params);
    result->int_value =
        Func(p, {a.bin_value, a.bin_len}, {b.bin_value, b.bin_len});
    result->type = VEF_RESULT_VALUE;
  }
};

template <auto Func>
struct TypeHashWithCacheVdfWrapper {
  using P = std::remove_cv_t<std::remove_reference_t<
      std::tuple_element_t<0, typename FuncParamTypes<decltype(Func)>::type>>>;

  static void invoke(vef_context_t *ctx, vef_vdf_args_t *args,
                     vef_vdf_result_t *result) {
    vef_invalue_t arg = get_invalue(ctx, args, 0);
    if (arg.is_null) {
      result->type = VEF_RESULT_NULL;
      return;
    }
    const P &p = type_params_cache_for<P>().get(arg.type_params);
    result->int_value =
        static_cast<long long>(Func(p, {arg.bin_value, arg.bin_len}));
    result->type = VEF_RESULT_VALUE;
  }
};

// IntrinsicDefaultWrapper: wraps IntrinsicDefaultFunc into a VDF.
// VDF signature: () -> STRING.
template <auto Func>
struct IntrinsicDefaultWrapper {
  static void invoke(vef_context_t * /*ctx*/, vef_vdf_args_t * /*args*/,
                     vef_vdf_result_t *result) {
    static thread_local std::string buf;
    buf = Func(result->error_msg);
    if (result->error_msg[0] != '\0') {
      result->type = VEF_RESULT_ERROR;
      return;
    }
    *result->alt_str_buf = buf.data();
    result->actual_len = buf.size();
    result->type = VEF_RESULT_VALUE;
  }
};

template <auto Func>
struct IntrinsicDefaultWithCacheWrapper {
  using P = std::remove_cv_t<std::remove_reference_t<
      std::tuple_element_t<0, typename FuncParamTypes<decltype(Func)>::type>>>;

  static void invoke(vef_context_t * /*ctx*/, vef_vdf_args_t * /*args*/,
                     vef_vdf_result_t *result) {
    static thread_local std::string buf;
    const P &p = type_params_cache_for<P>().get(result->type_params);
    buf = Func(p, result->error_msg);
    if (result->error_msg[0] != '\0') {
      result->type = VEF_RESULT_ERROR;
      return;
    }
    *result->alt_str_buf = buf.data();
    result->actual_len = buf.size();
    result->type = VEF_RESULT_VALUE;
  }
};

// IntToParamsWrapper: wraps IntToTypeParamsFunc into a VDF.
// VDF signature: (INT) -> STRING.
template <IntToTypeParamsFunc Func>
struct IntToParamsWrapper {
  static void invoke(vef_context_t *ctx, vef_vdf_args_t *args,
                     vef_vdf_result_t *result) {
    vef_invalue_t arg = get_invalue(ctx, args, 0);

    if (arg.is_null) {
      result->type = VEF_RESULT_NULL;
      return;
    }

    std::map<std::string, std::string> params;
    if (Func(arg.int_value, params, result->error_msg)) {
      result->type = VEF_RESULT_ERROR;
      return;
    }

    // TODO(villagesql-beta): decide on a broader character set policy.
    std::string serialized;
    for (const auto &[key, value] : params) {
      if (key.empty() || key.find_first_of(",=") != std::string::npos) {
        result->type = VEF_RESULT_ERROR;
        snprintf(result->error_msg, VEF_MAX_ERROR_LEN,
                 "int_to_params: key '%s' is empty or contains ',' or '='",
                 key.c_str());
        return;
      }
      if (value.find_first_of(",=") != std::string::npos) {
        result->type = VEF_RESULT_ERROR;
        snprintf(result->error_msg, VEF_MAX_ERROR_LEN,
                 "int_to_params: value '%s' contains ',' or '='",
                 value.c_str());
        return;
      }
      if (!serialized.empty()) serialized += ',';
      serialized += key;
      serialized += '=';
      serialized += value;
    }

    if (serialized.size() > result->max_str_len) {
      result->type = VEF_RESULT_ERROR;
      snprintf(result->error_msg, VEF_MAX_ERROR_LEN,
               "int_to_params result too large for buffer");
      return;
    }

    memcpy(result->str_buf, serialized.data(), serialized.size());
    result->type = VEF_RESULT_VALUE;
    result->actual_len = serialized.size();
  }
};

// ResolveParamsWrapper: wraps ResolveTypeParamsFunc into a VDF.
// VDF signature: (STRING) -> STRING.
template <ResolveTypeParamsFunc Func>
struct ResolveParamsWrapper {
  static void invoke(vef_context_t *ctx, vef_vdf_args_t *args,
                     vef_vdf_result_t *result) {
    vef_invalue_t arg = get_invalue(ctx, args, 0);

    if (arg.is_null) {
      result->type = VEF_RESULT_NULL;
      return;
    }

    std::string_view input(arg.str_value, arg.str_len);
    std::map<std::string, std::string> params;
    size_t start = 0;
    while (start < input.size()) {
      size_t comma = input.find(',', start);
      if (comma == std::string_view::npos) comma = input.size();
      size_t eq = input.find('=', start);
      if (eq == std::string_view::npos || eq >= comma) {
        result->type = VEF_RESULT_ERROR;
        snprintf(result->error_msg, VEF_MAX_ERROR_LEN,
                 "resolve_params: invalid input format");
        return;
      }
      params.emplace(input.substr(start, eq - start),
                     input.substr(eq + 1, comma - eq - 1));
      start = comma + 1;
    }

    vsql::ResolvedTypeParams resolved = {};
    if (Func(params, &resolved, result->error_msg)) {
      result->type = VEF_RESULT_ERROR;
      return;
    }

    int written =
        snprintf(result->str_buf, result->max_str_len, "%" PRId64 ",%" PRId64,
                 resolved.persisted_length, resolved.max_decode_buffer_length);
    if (written < 0 || static_cast<size_t>(written) >= result->max_str_len) {
      result->type = VEF_RESULT_ERROR;
      snprintf(result->error_msg, VEF_MAX_ERROR_LEN,
               "resolve_params result too large for buffer");
      return;
    }

    result->type = VEF_RESULT_VALUE;
    result->actual_len = static_cast<size_t>(written);
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

  // init_name() is a no-op for regular StaticFuncDesc; present so that
  // vef_init_auto_names() in extension_builder.h compiles for any func type.
  constexpr void init_name() const {}
};

// =============================================================================
// params_type_of / unique_params_types
// =============================================================================

template <typename T>
struct params_type_of {
  using type = void;
};
template <typename P>
struct params_type_of<CustomArgWith<P>> {
  using type = P;
};
template <typename P>
struct params_type_of<CustomResultWith<P>> {
  using type = P;
};

template <typename T, typename AccumTuple>
struct append_if_absent;
template <typename T, typename... Existing>
struct append_if_absent<T, std::tuple<Existing...>> {
  using type =
      std::conditional_t<(std::is_same_v<T, Existing> || ...),
                         std::tuple<Existing...>, std::tuple<Existing..., T>>;
};

template <typename InputTuple, size_t I, size_t N, typename AccumTuple>
struct unique_params_types_impl {
  using P = typename params_type_of<std::tuple_element_t<I, InputTuple>>::type;
  using NextAccum =
      std::conditional_t<std::is_void_v<P>, AccumTuple,
                         typename append_if_absent<P, AccumTuple>::type>;
  using type =
      typename unique_params_types_impl<InputTuple, I + 1, N, NextAccum>::type;
};
template <typename InputTuple, size_t N, typename AccumTuple>
struct unique_params_types_impl<InputTuple, N, N, AccumTuple> {
  using type = AccumTuple;
};

template <typename Tuple>
struct unique_params_types {
  using type =
      typename unique_params_types_impl<Tuple, 0, std::tuple_size_v<Tuple>,
                                        std::tuple<>>::type;
};

template <typename... Ps>
struct params_cache_checker {
  static bool check() { return (is_params_cache_bound<Ps>() && ...); }
};
template <>
struct params_cache_checker<> {
  static bool check() { return true; }
};

template <typename Tuple>
struct apply_params_cache_checker;
template <typename... Ps>
struct apply_params_cache_checker<std::tuple<Ps...>>
    : params_cache_checker<Ps...> {};

// =============================================================================
// FuncBuilder
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
        clear_(nullptr),
        accumulate_(nullptr),
        deterministic_(false) {}

  const char *name_;
  const char *return_type_;
  std::array<const char *, NumParams> param_types_;
  size_t buffer_size_;
  vef_prerun_func_t prerun_;
  vef_postrun_func_t postrun_;
  vef_vdf_clear_func_t clear_;
  vef_vdf_accumulate_func_t accumulate_;
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
    next.clear_ = clear_;
    next.accumulate_ = accumulate_;
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

  // Aggregate callbacks — typed C++ functions only.
  //
  //   .clear<&my_clear>()       // void(MyState&)
  //   .accumulate<&my_acc>()    // void(MyState&, IntArg, ...)
  //
  // Use with .state<T>() for automatic prerun/postrun.
  template <auto Fn>
  constexpr FuncBuilder<Func, NumParams> &clear() {
    if constexpr (std::is_same_v<decltype(Fn), vef_vdf_clear_func_t>) {
      // TODO(villagesql-beta): raw vef_vdf_clear_func_t should be converted to
      // typed void(State&). For now pass through directly.
      clear_ = Fn;
    } else {
      using Params = typename FuncParamTypes<decltype(Fn)>::type;
      using State = std::remove_reference_t<std::tuple_element_t<0, Params>>;
      clear_ = &agg_clear_wrapper<State, Fn>;
    }
    return *this;
  }

  template <auto Fn>
  constexpr FuncBuilder<Func, NumParams> &accumulate() {
    if constexpr (std::is_same_v<decltype(Fn), vef_vdf_accumulate_func_t>) {
      // TODO(villagesql-beta): raw vef_vdf_accumulate_func_t should be
      // converted to typed void(State&, TypedArgs...). For now pass through.
      accumulate_ = Fn;
    } else {
      using Params = typename FuncParamTypes<decltype(Fn)>::type;
      using State = std::remove_reference_t<std::tuple_element_t<0, Params>>;
      accumulate_ = &AggAccumulateWrapper<State, Fn, NumParams>::invoke;
    }
    return *this;
  }

  template <typename State>
  constexpr FuncBuilder<Func, NumParams> &state() {
    prerun_ = &auto_prerun<State>;
    postrun_ = &auto_postrun<State>;
    return *this;
  }

  constexpr StaticFuncDesc<NumParams> build() const {
    static_assert(NumParams <= kMaxParams,
                  "Too many parameters (max is kMaxParams)");
    if ((clear_ == nullptr) != (accumulate_ == nullptr)) {
      config_error__aggregate_must_set_both_clear_and_accumulate();
    }

    using AllParams = typename FuncParamTypes<decltype(Func)>::type;
    using UniquePTuple = typename unique_params_types<AllParams>::type;

    FuncWithMetadata meta{};
    if constexpr (std::is_same_v<decltype(Func), vef_vdf_func_t>) {
      // TODO(villagesql-beta): raw vef_vdf_func_t aggregate result functions
      // should be converted to typed style: void(const State&, ResultWrapper).
      // For now pass through directly.
      meta.f = Func;
    } else if constexpr (std::tuple_size_v<AllParams> == 1 &&
                         std::is_lvalue_reference_v<
                             std::tuple_element_t<0, AllParams>> &&
                         std::is_const_v<std::remove_reference_t<
                             std::tuple_element_t<0, AllParams>>>) {
      // Typed aggregate result: T(const State&) or optional<T>(const State&).
      using State = std::remove_const_t<
          std::remove_reference_t<std::tuple_element_t<0, AllParams>>>;
      meta.f = &AggResultWrapper<State, Func>::invoke;
    } else {
      // Typed scalar VDF.
      meta.f = &Wrapper<Func, NumParams>::invoke;
    }
    meta.prerun = prerun_;
    meta.postrun = postrun_;
    meta.clear = clear_;
    meta.accumulate = accumulate_;
    meta.return_type = to_vef_type(return_type_);
    meta.num_params = NumParams;
    meta.buffer_size = buffer_size_;
    meta.deterministic = deterministic_;
    for (size_t i = 0; i < NumParams; ++i) {
      meta.param_types[i] = to_vef_type(param_types_[i]);
    }
    if constexpr (std::tuple_size_v<UniquePTuple> > 0) {
      meta.check_params_cache_bound =
          &apply_params_cache_checker<UniquePTuple>::check;
    }

    return StaticFuncDesc<NumParams>(name_, meta);
  }
};

// =============================================================================
// Entry Points
// =============================================================================

// make_func<&impl>("name") — typed functions preferred.
// TODO(villagesql-beta): reject raw vef_vdf_func_t once all aggregate result
// functions have been converted to typed style (void(const State&,
// ResultWrapper)). The deprecated vef_context_t* first param is still rejected.
template <auto Func>
constexpr FuncBuilder<Func, 0> make_func(const char *name) {
  using AllParams = typename FuncParamTypes<decltype(Func)>::type;
  static_assert(
      std::is_same_v<decltype(Func), vef_vdf_func_t> ||
          std::tuple_size_v<AllParams> == 0 ||
          !is_context_param<std::tuple_element_t<0, AllParams>>::value,
      "vsql make_func: deprecated vef_context_t* first parameter not "
      "supported; write a typed function without the context parameter");
  FuncBuilder<Func, 0> builder;
  builder.name_ = name;
  return builder;
}

// make_type_encode<&fn>("name", TYPE) — (STRING) -> CUSTOM(type).
template <auto Func>
constexpr StaticFuncDesc<1> make_type_encode(const char *name,
                                             const char *type_name) {
  FuncWithMetadata meta{};
  if constexpr (std::is_same_v<decltype(Func), TypeEncodeFunc>) {
    meta.f = &TypeEncodeVdfWrapper<Func>::invoke;
  } else {
    meta.f = &TypeEncodeWithCacheVdfWrapper<Func>::invoke;
    meta.check_params_cache_bound =
        &is_params_cache_bound<typename TypeEncodeWithCacheVdfWrapper<Func>::P>;
  }
  meta.return_type = to_vef_type(type_name);
  meta.param_types[0] = to_vef_type(STRING);
  meta.num_params = 1;
  meta.buffer_size = 0;
  return StaticFuncDesc<1>(name, meta);
}

// make_type_decode<&fn>("name", TYPE) — (CUSTOM(type)) -> STRING.
template <auto Func>
constexpr StaticFuncDesc<1> make_type_decode(const char *name,
                                             const char *type_name) {
  FuncWithMetadata meta{};
  if constexpr (std::is_same_v<decltype(Func), TypeDecodeFunc>) {
    meta.f = &TypeDecodeVdfWrapper<Func>::invoke;
  } else {
    meta.f = &TypeDecodeWithCacheVdfWrapper<Func>::invoke;
    meta.check_params_cache_bound =
        &is_params_cache_bound<typename TypeDecodeWithCacheVdfWrapper<Func>::P>;
  }
  meta.return_type = to_vef_type(STRING);
  meta.param_types[0] = to_vef_type(type_name);
  meta.num_params = 1;
  meta.buffer_size = 0;
  return StaticFuncDesc<1>(name, meta);
}

// make_type_compare<&fn>("name", TYPE) — (CUSTOM, CUSTOM) -> INT.
template <auto Func>
constexpr StaticFuncDesc<2> make_type_compare(const char *name,
                                              const char *type_name) {
  FuncWithMetadata meta{};
  if constexpr (std::is_same_v<decltype(Func), TypeCompareFunc>) {
    meta.f = &TypeCompareVdfWrapper<Func>::invoke;
  } else {
    meta.f = &TypeCompareWithCacheVdfWrapper<Func>::invoke;
    meta.check_params_cache_bound = &is_params_cache_bound<
        typename TypeCompareWithCacheVdfWrapper<Func>::P>;
  }
  meta.return_type = to_vef_type(INT);
  meta.param_types[0] = to_vef_type(type_name);
  meta.param_types[1] = to_vef_type(type_name);
  meta.num_params = 2;
  meta.buffer_size = 0;
  return StaticFuncDesc<2>(name, meta);
}

// make_type_hash<&fn>("name", TYPE) — (CUSTOM(type)) -> INT.
template <auto Func>
constexpr StaticFuncDesc<1> make_type_hash(const char *name,
                                           const char *type_name) {
  FuncWithMetadata meta{};
  if constexpr (std::is_same_v<decltype(Func), TypeHashFunc>) {
    meta.f = &TypeHashVdfWrapper<Func>::invoke;
  } else {
    meta.f = &TypeHashWithCacheVdfWrapper<Func>::invoke;
    meta.check_params_cache_bound =
        &is_params_cache_bound<typename TypeHashWithCacheVdfWrapper<Func>::P>;
  }
  meta.return_type = to_vef_type(INT);
  meta.param_types[0] = to_vef_type(type_name);
  meta.num_params = 1;
  meta.buffer_size = 0;
  return StaticFuncDesc<1>(name, meta);
}

// make_int_to_params<&fn>("name") — (INT) -> STRING.
template <IntToTypeParamsFunc Func>
constexpr StaticFuncDesc<1> make_int_to_params(const char *name) {
  FuncWithMetadata meta{};
  meta.f = &IntToParamsWrapper<Func>::invoke;
  meta.return_type = to_vef_type(STRING);
  meta.param_types[0] = to_vef_type(INT);
  meta.num_params = 1;
  meta.buffer_size = VEF_MAX_TYPE_PARAMS_STRING_LEN;
  return StaticFuncDesc<1>(name, meta);
}

// make_resolve_params<&fn>("name") — (STRING) -> STRING.
template <ResolveTypeParamsFunc Func>
constexpr StaticFuncDesc<1> make_resolve_params(const char *name) {
  FuncWithMetadata meta{};
  meta.f = &ResolveParamsWrapper<Func>::invoke;
  meta.return_type = to_vef_type(STRING);
  meta.param_types[0] = to_vef_type(STRING);
  meta.num_params = 1;
  meta.buffer_size = VEF_MAX_TYPE_PARAMS_STRING_LEN;
  return StaticFuncDesc<1>(name, meta);
}

// make_intrinsic_default<&fn>("name") — () -> STRING.
template <auto Func>
constexpr StaticFuncDesc<0> make_intrinsic_default(const char *name) {
  FuncWithMetadata meta{};
  if constexpr (std::is_same_v<decltype(Func), IntrinsicDefaultFunc>) {
    meta.f = &IntrinsicDefaultWrapper<Func>::invoke;
  } else {
    meta.f = &IntrinsicDefaultWithCacheWrapper<Func>::invoke;
    meta.check_params_cache_bound = &is_params_cache_bound<
        typename IntrinsicDefaultWithCacheWrapper<Func>::P>;
  }
  meta.return_type = to_vef_type(STRING);
  meta.num_params = 0;
  meta.buffer_size = VEF_MAX_TYPE_PARAMS_STRING_LEN;
  return StaticFuncDesc<0>(name, meta);
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
}  // namespace vsql

#endif  // VILLAGESQL_VSQL_FUNC_BUILDER_H
