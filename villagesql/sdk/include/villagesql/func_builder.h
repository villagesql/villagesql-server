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
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

#include <villagesql/abi/types.h>
#include <villagesql/func_types.h>

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
        return_type{},
        param_types{},
        num_params(0),
        buffer_size(0),
        deterministic(false) {}

  ExtFunc f;
  vef_prerun_func_t prerun;
  vef_postrun_func_t postrun;
  vef_type_t return_type;
  std::array<vef_type_t, kMaxParams> param_types;
  size_t num_params;
  size_t buffer_size;
  bool deterministic;
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
// typed wrapper (IntArg, BinaryResult, etc.) and adapt accordingly.
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
      result->type = VEF_RESULT_ERROR;
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

// Encode: string -> binary. Returns false on success, true on error.
// Set *length = SIZE_MAX to produce SQL NULL.
using TypeEncodeFunc = bool (*)(std::string_view from, Span<unsigned char> buf,
                                size_t *length);

// Decode: binary -> string. Returns false on success, true on error.
using TypeDecodeFunc = bool (*)(Span<const unsigned char> data, Span<char> out,
                                size_t *out_len);

// Compare: two binary values. Returns <0, 0, or >0.
using TypeCompareFunc = int (*)(Span<const unsigned char> a,
                                Span<const unsigned char> b);

// Hash: binary value -> hash code.
using TypeHashFunc = size_t (*)(Span<const unsigned char> data);

// Extension author signatures for intrinsic_default.
// Called when a NOT NULL custom column is set to NULL with IGNORE (e.g.
// INSERT IGNORE or UPDATE IGNORE). Writes the encoded default value into
// 'buffer' and sets '*length' to bytes written.
// Returns false on success, true on error (writes to error_msg).
//
// Fixed-size types use the simple form:
//   bool my_default(Span<unsigned char> buffer, size_t *length, char
//   *error_msg)
//
// Variable-size types that need type parameters use:
//   bool my_default(const std::map<std::string, std::string> &params,
//                   Span<unsigned char> buffer, size_t *length, char
//                   *error_msg)
using IntrinsicDefaultFunc = bool (*)(villagesql::Span<unsigned char> buffer,
                                      size_t *length, char *error_msg);
using IntrinsicDefaultWithParamsFunc = bool (*)(
    const std::map<std::string, std::string> &params,
    villagesql::Span<unsigned char> buffer, size_t *length, char *error_msg);

// IntrinsicDefaultWrapper: wraps either signature into a VDF.
// VDF signature: () -> CUSTOM. Uses if-constexpr to detect whether the
// author function accepts type parameters.
template <auto Func>
struct IntrinsicDefaultWrapper {
  static constexpr bool kWithParams =
      std::is_invocable_v<decltype(Func),
                          const std::map<std::string, std::string> &,
                          villagesql::Span<unsigned char>, size_t *, char *>;

  static void invoke(vef_context_t * /*ctx*/, vef_vdf_args_t * /*args*/,
                     vef_vdf_result_t *result) {
    size_t length = 0;
    villagesql::Span<unsigned char> buffer(result->bin_buf,
                                           result->max_bin_len);
    bool failed;
    if constexpr (kWithParams) {
      std::map<std::string, std::string> params;
      for (unsigned int i = 0; i < result->type_params.count; i++) {
        params.emplace(result->type_params.keys[i],
                       result->type_params.values[i]);
      }
      failed = Func(params, buffer, &length, result->error_msg);
    } else {
      failed = Func(buffer, &length, result->error_msg);
    }
    if (failed) {
      result->type = VEF_RESULT_ERROR;
      return;
    }
    result->actual_len = length;
    result->type = VEF_RESULT_VALUE;
  }
};

// TypeEncodeVdfWrapper: wraps a TypeEncodeFunc into a VDF.
// VDF signature: (STRING) -> CUSTOM(type).
template <TypeEncodeFunc Func>
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

// TypeDecodeVdfWrapper: wraps a TypeDecodeFunc into a VDF.
// VDF signature: (CUSTOM(type)) -> STRING.
template <TypeDecodeFunc Func>
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

// TypeCompareVdfWrapper: wraps a TypeCompareFunc into a VDF.
// VDF signature: (CUSTOM(type), CUSTOM(type)) -> INT.
template <TypeCompareFunc Func>
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

// TypeHashVdfWrapper: wraps a TypeHashFunc into a VDF.
// VDF signature: (CUSTOM(type)) -> INT.
template <TypeHashFunc Func>
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
// be avoided by changing IntToParamsOp to return a map directly instead of
// going through the VDF string ABI. Not expected to matter since this only
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
  size_t buffer_size_;
  bool deterministic_;

  constexpr StaticFuncDesc(const char *name, const FuncWithMetadata &meta)
      : name_(name),
        params_{},
        return_type_(meta.return_type),
        vdf_(meta.f),
        prerun_(meta.prerun),
        postrun_(meta.postrun),
        buffer_size_(meta.buffer_size),
        deterministic_(meta.deterministic) {
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
  constexpr size_t buffer_size() const { return buffer_size_; }
  constexpr bool deterministic() const { return deterministic_; }
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

  return &desc;
}

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
        deterministic_(false) {}

  const char *name_;
  const char *return_type_;
  std::array<const char *, NumParams> param_types_;
  size_t buffer_size_;
  vef_prerun_func_t prerun_;
  vef_postrun_func_t postrun_;
  bool deterministic_;

  constexpr FuncBuilder<Func, NumParams> returns(const char *t) const {
    FuncBuilder<Func, NumParams> copy = *this;
    copy.return_type_ = t;
    return copy;
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

  constexpr FuncBuilder<Func, NumParams> buffer_size(size_t s) const {
    FuncBuilder<Func, NumParams> copy = *this;
    copy.buffer_size_ = s;
    return copy;
  }

  constexpr FuncBuilder<Func, NumParams> deterministic(bool d = true) const {
    FuncBuilder<Func, NumParams> copy = *this;
    copy.deterministic_ = d;
    return copy;
  }

  template <vef_prerun_func_t Hook>
  constexpr FuncBuilder<Func, NumParams> prerun() const {
    FuncBuilder<Func, NumParams> copy = *this;
    copy.prerun_ = Hook;
    return copy;
  }

  template <vef_postrun_func_t Hook>
  constexpr FuncBuilder<Func, NumParams> postrun() const {
    FuncBuilder<Func, NumParams> copy = *this;
    copy.postrun_ = Hook;
    return copy;
  }

  // Finalize the function definition and produce the StaticFuncDesc
  constexpr StaticFuncDesc<NumParams> build() const {
    static_assert(NumParams <= kMaxParams,
                  "Too many parameters (max is kMaxParams)");

    FuncWithMetadata meta{};
    meta.f = &Wrapper<Func, NumParams>::invoke;
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

/// Entry point for intrinsic_default functions:
//   make_intrinsic_default<&my_func>("my_func", "MY_TYPE")
template <auto Func>
constexpr StaticFuncDesc<0> make_intrinsic_default(const char *name,
                                                   const char *type_name) {
  FuncWithMetadata meta{};
  meta.f = &IntrinsicDefaultWrapper<Func>::invoke;
  meta.return_type = to_vef_type(type_name);
  meta.num_params = 0;
  meta.buffer_size = 0;  // server provides bin_buf sized to persisted_length
  return StaticFuncDesc<0>(name, meta);
}

// Entry point for encode VDFs: (STRING) -> CUSTOM(type_name).
//   make_type_encode<&my_func>("func_name", TYPE)
// Register with .func() and reference in type via .encode("func_name").
template <TypeEncodeFunc Func>
constexpr StaticFuncDesc<1> make_type_encode(const char *name,
                                             const char *type_name) {
  FuncWithMetadata meta{};
  meta.f = &TypeEncodeVdfWrapper<Func>::invoke;
  meta.return_type = to_vef_type(type_name);
  meta.param_types[0] = to_vef_type(STRING);
  meta.num_params = 1;
  meta.buffer_size = 0;  // server provides bin_buf sized to persisted_length
  return StaticFuncDesc<1>(name, meta);
}

// Entry point for decode VDFs: (CUSTOM(type_name)) -> STRING.
//   make_type_decode<&my_func>("func_name", TYPE)
// Register with .func() and reference in type via .decode("func_name").
template <TypeDecodeFunc Func>
constexpr StaticFuncDesc<1> make_type_decode(const char *name,
                                             const char *type_name) {
  FuncWithMetadata meta{};
  meta.f = &TypeDecodeVdfWrapper<Func>::invoke;
  meta.return_type = to_vef_type(STRING);
  meta.param_types[0] = to_vef_type(type_name);
  meta.num_params = 1;
  meta.buffer_size = 0;
  return StaticFuncDesc<1>(name, meta);
}

// Entry point for compare VDFs: (CUSTOM(type_name), CUSTOM(type_name)) -> INT.
//   make_type_compare<&my_func>("func_name", TYPE)
// Register with .func() and reference in type via .compare("func_name").
template <TypeCompareFunc Func>
constexpr StaticFuncDesc<2> make_type_compare(const char *name,
                                              const char *type_name) {
  FuncWithMetadata meta{};
  meta.f = &TypeCompareVdfWrapper<Func>::invoke;
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
template <TypeHashFunc Func>
constexpr StaticFuncDesc<1> make_type_hash(const char *name,
                                           const char *type_name) {
  FuncWithMetadata meta{};
  meta.f = &TypeHashVdfWrapper<Func>::invoke;
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
