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

// This file provides the underlying templates for function definition.
// For full documentation, see extension.h.
//
// =============================================================================
// Examples
// =============================================================================
//
// Basic function returning an INT:
//
//   make_func<&add_impl>("add")
//     .returns(INT)
//     .param(INT)
//     .param(INT)
//     .build()
//
// Function with custom type (define a constant to avoid typos):
//
//   constexpr const char* BYTEARRAY = "bytearray";
//
//   make_func<&rot13_impl>("rot13")
//     .returns(BYTEARRAY)
//     .param(BYTEARRAY)
//     .build()
//
// Type conversion functions:
//
//   make_func("bytearray_from_string")
//     .from_string<&encode_func>(BYTEARRAY)
//
//   make_func("bytearray_to_string")
//     .to_string<&decode_func>(BYTEARRAY)
//

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
#include <villagesql/func_types.h>
#include <villagesql/type_params_cache.h>

namespace villagesql {

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

// Maximum number of parameters supported
constexpr size_t kMaxParams = 8;

// The function pointer type that the framework calls - matches ABI
using ExtFunc = vef_vdf_func_t;

// =============================================================================
// Type Constants
// =============================================================================

// Built-in type names for use with .returns() and .param()
// For custom types, define your own constant: constexpr const char* MYTYPE =
// "mytype";
constexpr const char *STRING = "STRING";
constexpr const char *INT = "INT";
constexpr const char *REAL = "REAL";

// Forward declaration for internal helper (defined at end of file)
constexpr vef_type_t to_vef_type(const char *name);

// Deliberately unimplemented function used to produce a compile error. When
// build() runs in a constexpr context (as it does inside VEF_GENERATE_ENTRY_POINTS),
// calling a non-constexpr function is ill-formed. So if the aggregate
// configuration is invalid (e.g., clear set without accumulate), the build()
// method calls this function, which forces a compile error with a name that
// explains what went wrong.
void config_error__aggregate_must_set_both_clear_and_accumulate();

// Auto-generated prerun/postrun for aggregate state management.
// Use with .state<T>() on FuncBuilder to avoid writing boilerplate
// prerun/postrun callbacks for simple aggregate state types.
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
// Aggregate Callback Wrappers
// =============================================================================
//
// These wrappers let extension authors write aggregate callbacks using typed
// C++ signatures instead of raw ABI types. The State type is extracted from
// user_data and passed as a reference.
//
// Clear: void my_clear(State &state)
// Accumulate: void my_acc(State &state, IntArg val, ...)
// Result: ReturnType my_result(const State &state)
//      or std::optional<ReturnType> my_result(const State &state)
//
// std::optional results map nullopt to SQL NULL.

// Wraps void(State&) -> vef_vdf_clear_func_t
template <typename State, auto Func>
void agg_clear_wrapper(vef_context_t *, vef_vdf_args_t *args) {
  Func(*static_cast<State *>(args->user_data));
}

// =============================================================================
// FuncWithMetadata
// =============================================================================

// All the information about this function we'll pass to the
// the VillageSQL Extension Framework (VEF)
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
  // Non-null for VDFs that use a parameterized type cache. Points to a
  // function that returns true if the cache has been bound. Set by the cache
  // wrapper selection in make_type_encode/decode/compare/intrinsic_default.
  bool (*check_params_cache_bound)();
};

// =============================================================================
// Raw Function Types
// =============================================================================

// FROM_STRING raw function: converts string to binary representation
// Returns false on success, true on error. Set *length = SIZE_MAX for NULL.
using RawFromStringFunc = bool (*)(unsigned char *buffer, size_t buffer_size,
                                   const char *from, size_t from_len,
                                   size_t *length);

// TO_STRING raw function: converts binary representation to string
// Returns false on success, true on error.
using RawToStringFunc = bool (*)(const unsigned char *buffer,
                                 size_t buffer_size, char *to, size_t to_size,
                                 size_t *to_length);

// Extracts the parameter types of a function pointer as a std::tuple.
// Used by Wrapper to deduce whether each argument is a raw ABI type or a
// typed wrapper (IntArg, CustomResult, etc.) and adapt accordingly.
template <typename F>
struct FuncParamTypes;

template <typename R, typename... Args>
struct FuncParamTypes<R (*)(Args...)> {
  using type = std::tuple<Args...>;
};

// True for types recognized as the VDF execution context. Used by Wrapper to
// detect the deprecated ABI-style context parameter and handle it during the
// transition period.
//
template <typename T>
struct is_context_param : std::false_type {};

template <>
struct is_context_param<vef_context_t *> : std::true_type {};

// Promotes a vef_invalue_v1_t to vef_invalue_t, zero-initializing any fields
// added in VEF_PROTOCOL_2 and later.
inline vef_invalue_t promote_v1(const vef_invalue_v1_t &v) {
  vef_invalue_t out{};
  memcpy(&out, &v, sizeof(vef_invalue_v1_t));
  return out;
}

// Returns the i-th input value as a vef_invalue_t, handling both protocol
// versions. Extensions built against the v2 SDK may be loaded by v1 servers
// (protocol negotiated down), so callers must use this rather than reading
// args->values or args->values_v1 directly.
inline vef_invalue_t get_invalue(vef_context_t *ctx, vef_vdf_args_t *args,
                                 unsigned int i) {
  if (ctx->protocol >= VEF_PROTOCOL_2) return *args->values[i];
  return promote_v1(args->values_v1[i]);
}

// =============================================================================
// Wrapper Template
// =============================================================================

// Wrapper generates a function with the vef_vdf_func_t signature that unpacks
// vef_vdf_args_t and adapts each argument and result to the declared parameter
// type of Func.
//
// Preferred C++ style (no context parameter):
//   void func(IntArg arg0, ..., IntResult result)
//
// Deprecated ABI style (context as first parameter, to be removed):
//   void func(vef_context_t* ctx, vef_invalue_t* arg0, ...,
//             vef_vdf_result_t* result)
//
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
    constexpr bool kHasCtx =
        is_context_param<std::tuple_element_t<0, Params>>::value;
    std::array<vef_invalue_t, NumParams> vals{
        get_invalue(ctx, args, static_cast<unsigned int>(Is))...};
    if constexpr (kHasCtx) {
      // Deprecated style: passes ABI C types as arguments to function.
      // TODO(villagesql): Remove once all callers have migrated off
      // vef_context_t*.
      Func(ctx, make_arg<std::tuple_element_t<1 + Is, Params>>(&vals[Is])...,
           make_result<std::tuple_element_t<1 + NumParams, Params>>(result));
    } else {
      Func(make_arg<std::tuple_element_t<Is, Params>>(&vals[Is])...,
           make_result<std::tuple_element_t<NumParams, Params>>(result));
    }
  }

  // Converts vef_invalue_t* to T. If T is vef_invalue_t* the pointer passes
  // through; otherwise T is constructed from the pointer (e.g. IntArg(v)).
  template <typename T>
  static T make_arg(vef_invalue_t *v) {
    if constexpr (std::is_same_v<T, vef_invalue_t *>) {
      return v;
    } else {
      return T(v);
    }
  }

  // Converts vef_vdf_result_t* to T. Same pass-through / construct logic.
  template <typename T>
  static T make_result(vef_vdf_result_t *r) {
    if constexpr (std::is_same_v<T, vef_vdf_result_t *>) {
      return r;
    } else {
      return T(r);
    }
  }
};

// =============================================================================
// Aggregate Typed Wrappers
// =============================================================================
//
// These wrappers let extension authors write aggregate callbacks using typed
// C++ signatures instead of raw ABI types. The State type is extracted from
// user_data and passed as a reference.
//
// Accumulate: void my_acc(State &state, IntArg val, ...)
// Result: ReturnType my_result(const State &state)
//      or std::optional<ReturnType> my_result(const State &state)
//
// std::optional results map nullopt to SQL NULL.
//
// (agg_clear_wrapper is defined earlier since it has no dependencies.)

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
    if constexpr (std::is_same_v<T, vef_invalue_t *>) {
      return v;
    } else {
      return T(v);
    }
  }

  template <size_t... Is>
  static void invoke_impl(vef_context_t *ctx, vef_vdf_args_t *args,
                          vef_vdf_result_t *,
                          std::index_sequence<Is...>) {
    auto &state = *static_cast<State *>(args->user_data);
    std::array<vef_invalue_t, NumParams> vals{
        get_invalue(ctx, args, static_cast<unsigned int>(Is))...};
    Func(state, make_arg<std::tuple_element_t<1 + Is, Params>>(&vals[Is])...);
  }
};

// Wraps T(const State&) or std::optional<T>(const State&) -> vef_vdf_func_t
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
// FromStringWrapper / ToStringWrapper
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
// C++ Type Operation Builders
// =============================================================================
//
// Extension authors must implement functions with the following signatures
// to enable a new type to be used in VillageSQL, and use the builders
// make_type_encode, make_type_decode, make_type_compare, and make_type_hash to
// and register the names with the type builder.
//   TypeEncodeFunc  -> VDF: (STRING) -> CUSTOM(type)
//   TypeDecodeFunc  -> VDF: (CUSTOM(type)) -> STRING
//   TypeCompareFunc -> VDF: (CUSTOM(type), CUSTOM(type)) -> INT
//   TypeHashFunc    -> VDF: (CUSTOM(type)) -> INT

// Extension author signatures for encode.
// Returns false on success, true on error. Set *length = SIZE_MAX for NULL.
using TypeEncodeFunc = bool (*)(std::string_view from, Span<unsigned char> buf,
                                size_t *length);
template <typename P>
using TypeEncodeWithParamsFunc = bool (*)(const P &, std::string_view from,
                                          Span<unsigned char> buf,
                                          size_t *length);

// Extension author signatures for decode.
// Returns false on success, true on error.
using TypeDecodeFunc = bool (*)(Span<const unsigned char> data, Span<char> out,
                                size_t *out_len);
template <typename P>
using TypeDecodeWithParamsFunc = bool (*)(const P &,
                                          Span<const unsigned char> data,
                                          Span<char> out, size_t *out_len);

// Extension author signatures for compare.
// Returns <0, 0, or >0.
using TypeCompareFunc = int (*)(Span<const unsigned char> a,
                                Span<const unsigned char> b);
template <typename P>
using TypeCompareWithParamsFunc = int (*)(const P &,
                                          Span<const unsigned char> a,
                                          Span<const unsigned char> b);

// Extension author signatures for hash.
// Returns a hash code.
using TypeHashFunc = size_t (*)(Span<const unsigned char> data);
template <typename P>
using TypeHashWithParamsFunc = size_t (*)(const P &,
                                          Span<const unsigned char> data);

// Extension author signature for intrinsic_default.
// Called when a NOT NULL custom column is set to NULL with IGNORE (e.g.
// INSERT IGNORE or UPDATE IGNORE). Returns the string representation of the
// default value (e.g. "(0,0)" for COMPLEX). The server converts this string
// using the type's from_string function to produce the binary default.
// Returns an empty string on error (writes to error_msg).
using IntrinsicDefaultFunc = std::string (*)(char *error_msg);
template <typename P>
using IntrinsicDefaultWithParamsFunc = std::string (*)(const P &,
                                                       char *error_msg);

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

// TypeHashWithCacheVdfWrapper: wraps size_t Func(const P&,
// Span<const unsigned char>). VDF signature: (CUSTOM(type)) -> INT.
// P is deduced from Func's first parameter. The TypeParamsCache for P must
// have been bound via .params<P, &parse_fn>() in the type builder.
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

// Cache-aware type operation wrappers for parameterized types.
//
// Used when a type operation function takes const P& as its first parameter.
// The parse result P is cached per unique canonical params string, so parsing
// runs at most once per unique type instantiation (e.g., TVECTOR(3)).
//
// type_params_cache_for<P>() (from type_params_cache.h) is marked
// visibility("hidden") so each extension DSO gets its own cache instance, and
// all wrappers for the same P share a single cache within a DSO.

// TypeEncodeWithCacheVdfWrapper: wraps bool Func(const P&, string_view,
// Span<unsigned char>, size_t*). VDF signature: (STRING) -> CUSTOM(type).
// P is deduced from Func's first parameter. The TypeParamsCache for P must
// have been bound via .params<P, &parse_fn>() in the type builder.
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

// TypeDecodeWithCacheVdfWrapper: wraps bool Func(const P&,
// Span<const unsigned char>, Span<char>, size_t*).
// VDF signature: (CUSTOM(type)) -> STRING.
// P is deduced from Func's first parameter. The TypeParamsCache for P must
// have been bound via .params<P, &parse_fn>() in the type builder.
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

// IntrinsicDefaultWithCacheWrapper: wraps std::string Func(const P&, char*).
// VDF signature: () -> STRING.
// P is deduced from Func's first parameter. The TypeParamsCache for P must
// have been bound via .params<P, &parse_fn>() in the type builder.
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

// TypeCompareWithCacheVdfWrapper: wraps int Func(const P&,
// Span<const unsigned char>, Span<const unsigned char>).
// VDF signature: (CUSTOM(type), CUSTOM(type)) -> INT.
// P is deduced from Func's first parameter. The TypeParamsCache for P must
// have been bound via .params<P, &parse_fn>() in the type builder.
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

// =============================================================================
// Extension Author Function Signatures (for parameterized types)
// =============================================================================
//
// Extension authors write against these clean C++ signatures using std::map.
// The SDK wrapper templates handle serialization to/from the canonical
// "key=value,key=value,..." wire format.

// Convert TYPE(N) integer to parameter key-value pairs.
// Populates params map (e.g., params["dimension"] = "1536").
// Returns false on success, true on error (writes to error_msg).
// TODO(villagesql-beta): revisit this convention for the client (something like
// Rust's Result).
using IntToTypeParamsFunc = bool (*)(int64_t value,
                                     std::map<std::string, std::string> &params,
                                     char *error_msg);

// Validate type parameters and compute storage characteristics.
// Reads from params map, populates *result with storage sizes.
// Returns false on success, true on error (writes to error_msg).
// TODO(villagesql-beta): generate C++ object for parameterized types as part of
// resolve_params call.
using ResolveTypeParamsFunc =
    bool (*)(const std::map<std::string, std::string> &params,
             villagesql::ResolvedTypeParams *result, char *error_msg);

// =============================================================================
// IntToParamsWrapper / ResolveParamsWrapper
// =============================================================================

// IntToParamsWrapper: wraps an IntToTypeParamsFunc into a VDF.
// VDF signature: (INT) -> STRING, returning "key1=value1,key2=value2,...".
// The map is serialized with keys in sorted order (via std::map iteration).
// The server normalizes casing and sort order via TypeParameters::from_raw(),
// which re-parses the string.
// TODO(villagesql-performance): The map→string→parse→string round-trip could
// be avoided by changing IntToParamsFunction to return a map directly instead
// of going through the VDF string ABI. Not expected to matter since this only
// runs during DDL.
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

    // Validate and serialize map to "key1=value1,key2=value2,..." string.
    // std::map iterates in sorted key order. The server normalizes
    // casing and sort order via TypeParameters::from_raw().
    // TODO(villagesql-beta): decide on a broader character set policy for
    // keys and values (e.g., restrict to ASCII alphanumeric + underscore).
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

// ResolveParamsWrapper: wraps a ResolveTypeParamsFunc into a VDF.
// VDF signature: (STRING) -> STRING.
// Input: "key1=value1,key2=value2,...".
// Output: "persisted_length,max_decode_buffer_length".
template <ResolveTypeParamsFunc Func>
struct ResolveParamsWrapper {
  static void invoke(vef_context_t *ctx, vef_vdf_args_t *args,
                     vef_vdf_result_t *result) {
    vef_invalue_t arg = get_invalue(ctx, args, 0);

    if (arg.is_null) {
      result->type = VEF_RESULT_NULL;
      return;
    }

    // Parse "key1=value1,key2=value2,..." into std::map.
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

    villagesql::ResolvedTypeParams resolved = {};
    if (Func(params, &resolved, result->error_msg)) {
      result->type = VEF_RESULT_ERROR;
      return;
    }

    // Serialize to "persisted_length,max_decode_buffer_length".
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

// Holds function metadata for constexpr builder chain.
// Does NOT contain self-referential pointers - those are created at
// registration time by materialize_func_desc().
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

  // Accessors for use at registration time
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
};

// Materializes the ABI descriptor structures at registration time.
// Uses template parameters to ensure each function gets unique static storage.
// FuncData is the StaticFuncDesc type, Index ensures uniqueness per function.
//
// Hidden visibility prevents the dynamic linker from coalescing identical
// template instantiations across different extension .so files. Without this,
// two extensions with functions of the same signature and index would share
// the same static desc/signature objects, causing use-after-free when one
// extension is unloaded.
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

// =============================================================================
// params_type_of / first_params_type_in / count_different_params_types
// =============================================================================

// Extracts the params type P if T is CustomArgWith<P> or CustomResultWith<P>.
// Yields void for all other types.
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

// Appends T to Tuple<Existing...> only if T is not already present.
// Uses a fold expression over Existing to check membership.
template <typename T, typename AccumTuple>
struct append_if_absent;
template <typename T, typename... Existing>
struct append_if_absent<T, std::tuple<Existing...>> {
  using type =
      std::conditional_t<(std::is_same_v<T, Existing> || ...),
                         std::tuple<Existing...>, std::tuple<Existing..., T>>;
};

// Collects the distinct non-void params types from InputTuple into a
// std::tuple<P1, P2, ...>, preserving order of first appearance.
// N is passed explicitly to avoid ill-formed partial specialization on
// std::tuple_size_v<InputTuple>.
template <typename InputTuple, size_t I, size_t N, typename AccumTuple>
struct unique_params_types_impl {
  using P =
      typename params_type_of<std::tuple_element_t<I, InputTuple>>::type;
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

// Public entry point: unique_params_types<Tuple>::type is a std::tuple of the
// distinct P types used by CustomArgWith<P> or CustomResultWith<P> in Tuple.
template <typename Tuple>
struct unique_params_types {
  using type = typename unique_params_types_impl<
      Tuple, 0, std::tuple_size_v<Tuple>, std::tuple<>>::type;
};

// Checks that all TypeParamsCaches for Ps... have been bound. Used as the
// check_params_cache_bound function pointer for VDFs that use parameterized
// custom types via CustomArgWith<P> or CustomResultWith<P>.
template <typename... Ps>
struct params_cache_checker {
  static bool check() { return (is_params_cache_bound<Ps>() && ...); }
};
// Specialization for the no-params case (vacuously true, never stored).
template <>
struct params_cache_checker<> {
  static bool check() { return true; }
};

// Unpacks a std::tuple<Ps...> into params_cache_checker<Ps...>.
template <typename Tuple>
struct apply_params_cache_checker;
template <typename... Ps>
struct apply_params_cache_checker<std::tuple<Ps...>>
    : params_cache_checker<Ps...> {};

// =============================================================================
// FuncBuilder
// =============================================================================

// Builder for defining functions. Start with make_func<&impl>("name"), chain
// configuration methods, and end with .build().
//
// Example:
//   make_func<&add_impl>("add")
//     .returns(INT)
//     .param(INT)
//     .param(INT)
//     .build()
//
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

  // Aggregate callbacks. Accepts either raw ABI function pointers or typed
  // C++ functions. Typed functions have their State deduced from the first
  // parameter.
  //
  // Raw ABI:
  //   .clear<&my_raw_clear>()       // void(vef_context_t*, vef_vdf_args_t*)
  //   .accumulate<&my_raw_acc>()    // void(vef_context_t*, vef_vdf_args_t*,
  //                                 //      vef_vdf_result_t*)
  //
  // Typed (use with .state<T>()):
  //   .clear<&my_clear>()           // void(MyState&)
  //   .accumulate<&my_acc>()        // void(MyState&, IntArg, ...)
  //
  // The result function is the Func template parameter of make_func<>:
  //   make_func<&my_result>("name") // T(const MyState&) or
  //                                 // optional<T>(const MyState&)
  //
  // Example:
  //   void my_clear(MyState &s) { s = {}; }
  //   void my_acc(MyState &s, IntArg val) { ... }
  //   std::optional<long long> my_result(const MyState &s) { return s.val; }
  //
  //   make_func<&my_result>("my_agg")
  //       .returns(INT).param(INT)
  //       .state<MyState>()
  //       .clear<&my_clear>()
  //       .accumulate<&my_acc>()
  //       .build()

  template <auto Fn>
  constexpr FuncBuilder<Func, NumParams> &clear() {
    if constexpr (std::is_same_v<decltype(Fn), vef_vdf_clear_func_t>) {
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
      accumulate_ = Fn;
    } else {
      using Params = typename FuncParamTypes<decltype(Fn)>::type;
      using State = std::remove_reference_t<std::tuple_element_t<0, Params>>;
      accumulate_ = &AggAccumulateWrapper<State, Fn, NumParams>::invoke;
    }
    return *this;
  }

  // Set the aggregate state type. Automatically generates prerun (allocates
  // State via value-initialization) and postrun (deletes State).
  template <typename State>
  constexpr FuncBuilder<Func, NumParams> &state() {
    prerun_ = &auto_prerun<State>;
    postrun_ = &auto_postrun<State>;
    return *this;
  }

  // Finalize the function definition and produce the StaticFuncDesc
  constexpr StaticFuncDesc<NumParams> build() const {
    static_assert(NumParams <= kMaxParams,
                  "Too many parameters (max is kMaxParams)");
    // Catch mismatched clear/accumulate at compile time. build() is evaluated
    // in a constexpr context by VEF_GENERATE_ENTRY_POINTS, so reaching a
    // non-constexpr call produces a compile error. Also validated at
    // registration time as a safety net.
    if ((clear_ == nullptr) != (accumulate_ == nullptr)) {
      config_error__aggregate_must_set_both_clear_and_accumulate();
    }

    using AllParams = typename FuncParamTypes<decltype(Func)>::type;
    using UniquePTuple = typename unique_params_types<AllParams>::type;

    FuncWithMetadata meta{};
    if constexpr (std::is_same_v<decltype(Func), vef_vdf_func_t>) {
      // Raw ABI signature — use directly.
      meta.f = Func;
    } else if constexpr (std::tuple_size_v<AllParams> == 1 &&
                         std::is_lvalue_reference_v<
                             std::tuple_element_t<0, AllParams>> &&
                         std::is_const_v<std::remove_reference_t<
                             std::tuple_element_t<0, AllParams>>>) {
      // Typed aggregate result: T(const State&) or optional<T>(const State&).
      // Wrap with AggResultWrapper to extract user_data and write the return
      // value to the ABI result struct.
      using State =
          std::remove_const_t<std::remove_reference_t<
              std::tuple_element_t<0, AllParams>>>;
      meta.f = &AggResultWrapper<State, Func>::invoke;
    } else {
      // Typed scalar VDF — wrap arguments and result.
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

// Specialization for type conversion functions (no Func template parameter)
template <size_t NumParams>
struct FuncBuilderNoImpl {
  constexpr FuncBuilderNoImpl() : name_(nullptr) {}

  const char *name_;

  // from_string: STRING -> custom type
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

  // to_string: custom type -> STRING
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

// Entry point for regular functions: make_func<&impl>("name")
template <auto Func>
constexpr FuncBuilder<Func, 0> make_func(const char *name) {
  FuncBuilder<Func, 0> builder;
  builder.name_ = name;
  return builder;
}

// Entry point for type conversion functions: make_func("name")
constexpr FuncBuilderNoImpl<0> make_func(const char *name) {
  FuncBuilderNoImpl<0> builder;
  builder.name_ = name;
  return builder;
}

// Entry point for int_to_params functions:
//   make_int_to_params<&my_func>("my_func")
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

// Entry point for resolve_params functions:
//   make_resolve_params<&my_func>("my_func")
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

// Entry point for intrinsic_default functions:
//   make_intrinsic_default<&my_func>("my_func")
// my_func returns std::string (string representation of the default value).
// The server encodes this string to produce the binary default.
// If my_func's first parameter is const P&, the cache is used automatically
// (parse function must be bound via .params<P, &parse_fn>() in the type
// builder).
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

// Entry point for encode VDFs: (STRING) -> CUSTOM(type_name).
//   make_type_encode<&my_func>("func_name", TYPE)
// Accepts TypeEncodeFunc or a function whose first parameter is const P&,
// in which case the SDK routes through the params cache (parse function must
// be bound via .params<P, &parse_fn>() in the type builder).
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
  meta.buffer_size = 0;  // server provides bin_buf sized to persisted_length
  return StaticFuncDesc<1>(name, meta);
}

// Entry point for decode VDFs: (CUSTOM(type_name)) -> STRING.
//   make_type_decode<&my_func>("func_name", TYPE)
// Accepts TypeDecodeFunc or a function whose first parameter is const P&,
// in which case the SDK routes through the params cache (parse function must
// be bound via .params<P, &parse_fn>() in the type builder).
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

// Entry point for compare VDFs: (CUSTOM(type_name), CUSTOM(type_name)) -> INT.
//   make_type_compare<&my_func>("func_name", TYPE)
// Register with .func() and reference in type via .compare("func_name").
// Accepts TypeCompareFunc or a function whose first parameter is const P&,
// in which case the SDK routes through the params cache (parse function must
// be bound via .params<P, &parse_fn>() in the type builder).
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

// Entry point for hash VDFs: (CUSTOM(type_name)) -> INT.
//   make_type_hash<&my_func>("func_name", TYPE)
// Register with .func() and reference in type via .hash("func_name").
// Accepts TypeHashFunc or a function whose first parameter is const P&,
// in which case the SDK routes through the params cache (parse function must
// be bound via .params<P, &parse_fn>() in the type builder).
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

// =============================================================================
// Internal Implementation
// =============================================================================

// Converts type name string to vef_type_t ABI struct
constexpr vef_type_t to_vef_type(const char *name) {
  std::string_view sv(name);
  if (sv == "STRING") {
    return vef_type_t{VEF_TYPE_STRING, nullptr};
  }
  if (sv == "INT") {
    return vef_type_t{VEF_TYPE_INT, nullptr};
  }
  if (sv == "REAL") {
    return vef_type_t{VEF_TYPE_REAL, nullptr};
  }
  // Custom type
  return vef_type_t{VEF_TYPE_CUSTOM, name};
}

}  // namespace func_builder
}  // namespace villagesql

#endif  // VILLAGESQL_SDK_FUNC_BUILDER_H
