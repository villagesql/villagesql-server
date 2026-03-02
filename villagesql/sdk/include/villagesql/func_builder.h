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

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <optional>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

#include <villagesql/abi/types.h>
#include <villagesql/func_types.h>

namespace villagesql {
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
    if constexpr (kHasCtx) {
      // Deprecated style: passes ABI C types as arguments to function.
      // TODO(villagesql): Remove once all callers have migrated off
      // vef_context_t*.
      Func(ctx,
           make_arg<std::tuple_element_t<1 + Is, Params>>(&args->values[Is])...,
           make_result<std::tuple_element_t<1 + NumParams, Params>>(result));
    } else {
      Func(make_arg<std::tuple_element_t<Is, Params>>(&args->values[Is])...,
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
    vef_invalue_t *arg = &args->values[0];

    if (arg->is_null) {
      result->type = VEF_RESULT_NULL;
      return;
    }

    size_t length;
    bool failed = Func(result->bin_buf, result->max_bin_len, arg->str_value,
                       arg->str_len, &length);

    if (failed) {
      result->type = VEF_RESULT_ERROR;
      constexpr size_t kMaxInputDisplay = 64;
      size_t display_len = arg->str_len;
      const char *ellipsis = "";
      if (display_len > kMaxInputDisplay) {
        display_len = kMaxInputDisplay;
        ellipsis = "...";
      }
      snprintf(result->error_msg, VEF_MAX_ERROR_LEN,
               "failed to parse string '%.*s%s'", static_cast<int>(display_len),
               arg->str_value, ellipsis);
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
    vef_invalue_t *arg = &args->values[0];

    if (arg->is_null) {
      result->type = VEF_RESULT_NULL;
      return;
    }

    size_t to_length;
    bool failed = Func(arg->bin_value, arg->bin_len, result->str_buf,
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
