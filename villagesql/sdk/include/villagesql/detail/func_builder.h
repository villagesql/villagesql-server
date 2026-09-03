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

#ifndef VILLAGESQL_DETAIL_FUNC_BUILDER_H
#define VILLAGESQL_DETAIL_FUNC_BUILDER_H

// Internal implementation of vsql/func_builder.h. Not part of the public API.

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
#include <villagesql/vsql/func_types.h>
#include <villagesql/vsql/pre_post_run.h>
#include <villagesql/vsql/type_params.h>
#include <villagesql/vsql/var_args.h>

namespace vsql {
namespace func_builder {

constexpr size_t kMaxParams = 8;

namespace detail {

constexpr vef_type_t to_vef_type(const char *name) {
  std::string_view sv(name);
  if (sv == "STRING") return vef_type_t{VEF_TYPE_STRING, nullptr};
  if (sv == "INT") return vef_type_t{VEF_TYPE_INT, nullptr};
  if (sv == "REAL") return vef_type_t{VEF_TYPE_REAL, nullptr};
  return vef_type_t{VEF_TYPE_CUSTOM, name};
}

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

inline vef_invalue_t promote_v1(const vef_invalue_v1_t &v) {
  vef_invalue_t out{};
  memcpy(&out, &v, sizeof(vef_invalue_v1_t));
  return out;
}

inline vef_invalue_t get_invalue(vef_context_t *ctx, vef_vdf_args_t *args,
                                 unsigned int i) {
  if (ctx->protocol >= VEF_PROTOCOL_3) return *args->values[i];
  return promote_v1(args->values_v1[i]);
}

template <typename F>
struct FuncParamTypes;

template <typename R, typename... Args>
struct FuncParamTypes<R (*)(Args...)> {
  using type = std::tuple<Args...>;
};

template <typename F>
struct FuncReturnType;

template <typename R, typename... Args>
struct FuncReturnType<R (*)(Args...)> {
  using type = R;
};

// True for the deprecated ABI-style context parameter. Used by make_func to
// reject functions that still have vef_context_t* as their first parameter.
template <typename T>
struct is_context_param : std::false_type {};

template <>
struct is_context_param<vef_context_t *> : std::true_type {};

// True for result wrapper types. Used by build() to detect the typed aggregate
// result function pattern: void(const State&, ResultWrapper).
template <typename T>
struct is_result_wrapper : std::false_type {};
template <>
struct is_result_wrapper<IntResult> : std::true_type {};
template <>
struct is_result_wrapper<RealResult> : std::true_type {};
template <>
struct is_result_wrapper<StringResult> : std::true_type {};
template <>
struct is_result_wrapper<CustomResult> : std::true_type {};
template <typename P>
struct is_result_wrapper<CustomResultWith<P>> : std::true_type {};

// True for input wrapper types accepted by make_func and aggregate accumulate.
template <typename T>
struct is_arg_wrapper : std::false_type {};
template <>
struct is_arg_wrapper<IntArg> : std::true_type {};
template <>
struct is_arg_wrapper<RealArg> : std::true_type {};
template <>
struct is_arg_wrapper<StringArg> : std::true_type {};
template <>
struct is_arg_wrapper<CustomArg> : std::true_type {};
template <typename P>
struct is_arg_wrapper<CustomArgWith<P>> : std::true_type {};

template <typename T>
struct wrapper_vef_type;
template <>
struct wrapper_vef_type<IntArg> {
  static constexpr vef_type_id id = VEF_TYPE_INT;
};
template <>
struct wrapper_vef_type<RealArg> {
  static constexpr vef_type_id id = VEF_TYPE_REAL;
};
template <>
struct wrapper_vef_type<StringArg> {
  static constexpr vef_type_id id = VEF_TYPE_STRING;
};
template <>
struct wrapper_vef_type<CustomArg> {
  static constexpr vef_type_id id = VEF_TYPE_CUSTOM;
};
template <typename P>
struct wrapper_vef_type<CustomArgWith<P>> {
  static constexpr vef_type_id id = VEF_TYPE_CUSTOM;
};
template <>
struct wrapper_vef_type<IntResult> {
  static constexpr vef_type_id id = VEF_TYPE_INT;
};
template <>
struct wrapper_vef_type<RealResult> {
  static constexpr vef_type_id id = VEF_TYPE_REAL;
};
template <>
struct wrapper_vef_type<StringResult> {
  static constexpr vef_type_id id = VEF_TYPE_STRING;
};
template <>
struct wrapper_vef_type<CustomResult> {
  static constexpr vef_type_id id = VEF_TYPE_CUSTOM;
};
template <typename P>
struct wrapper_vef_type<CustomResultWith<P>> {
  static constexpr vef_type_id id = VEF_TYPE_CUSTOM;
};

template <typename Tuple, size_t... Is>
constexpr bool all_func_args_are_wrappers(std::index_sequence<Is...>) {
  return (
      is_arg_wrapper<std::remove_cv_t<
          std::remove_reference_t<std::tuple_element_t<Is, Tuple>>>>::value &&
      ...);
}

template <typename Tuple, size_t... Is>
constexpr bool all_accumulate_args_are_wrappers(std::index_sequence<Is...>) {
  return (is_arg_wrapper<std::remove_cv_t<std::remove_reference_t<
              std::tuple_element_t<Is + 1, Tuple>>>>::value &&
          ...);
}

template <typename Wrapper>
bool declared_type_matches_wrapper(const vef_type_t &declared) {
  using W = std::remove_cv_t<std::remove_reference_t<Wrapper>>;
  return declared.id == wrapper_vef_type<W>::id;
}

template <typename Tuple, size_t I, size_t N>
struct signature_checker_impl {
  static const char *check_params(const vef_type_t *params) {
    using Wrapper = std::tuple_element_t<I, Tuple>;
    if (!declared_type_matches_wrapper<Wrapper>(params[I])) {
      return "VDF declared .param(TYPE) type does not match the C++ argument "
             "wrapper type";
    }
    return signature_checker_impl<Tuple, I + 1, N>::check_params(params);
  }
};

template <typename Tuple, size_t N>
struct signature_checker_impl<Tuple, N, N> {
  static const char *check_params(const vef_type_t *) { return nullptr; }
};

template <typename Tuple, size_t NumParams>
struct signature_checker {
  static const char *check(const vef_type_t *params, size_t param_count,
                           const vef_type_t &return_type) {
    if (param_count != NumParams) {
      return "VDF declared parameter count does not match the C++ function "
             "signature";
    }
    if (const char *err =
            signature_checker_impl<Tuple, 0, NumParams>::check_params(params)) {
      return err;
    }
    using ResultWrapper = std::tuple_element_t<NumParams, Tuple>;
    if (!declared_type_matches_wrapper<ResultWrapper>(return_type)) {
      return "VDF declared .returns(...) type does not match the C++ result "
             "wrapper type";
    }
    return nullptr;
  }
};

template <typename Tuple, size_t NumParams,
          bool ArityOk = (std::tuple_size_v<Tuple> == NumParams + 1)>
struct func_signature_shape {
  static constexpr bool args_ok = false;
  static constexpr bool result_ok = false;
};

template <typename Tuple, size_t NumParams>
struct func_signature_shape<Tuple, NumParams, true> {
  static constexpr bool args_ok =
      all_func_args_are_wrappers<Tuple>(std::make_index_sequence<NumParams>{});
  static constexpr bool result_ok = is_result_wrapper<std::remove_cv_t<
      std::remove_reference_t<std::tuple_element_t<NumParams, Tuple>>>>::value;
};

template <typename Tuple, size_t NumParams,
          bool ArityOk = (std::tuple_size_v<Tuple> == NumParams + 1)>
struct accumulate_signature_shape {
  static constexpr bool args_ok = false;
};

template <typename Tuple, size_t NumParams>
struct accumulate_signature_shape<Tuple, NumParams, true> {
  static constexpr bool args_ok = all_accumulate_args_are_wrappers<Tuple>(
      std::make_index_sequence<NumParams>{});
};

// Validates that AllParams matches void(const State&, ResultWrapper) for
// make_aggregate_func. Specialized only for exactly 2-element tuples.
template <typename State, typename AllParams,
          size_t = std::tuple_size_v<AllParams>>
struct is_agg_result_for_state : std::false_type {};
template <typename State, typename P0, typename P1>
struct is_agg_result_for_state<State, std::tuple<P0, P1>, 2>
    : std::bool_constant<
          std::is_lvalue_reference_v<P0> &&
          std::is_const_v<std::remove_reference_t<P0>> &&
          std::is_same_v<std::remove_const_t<std::remove_reference_t<P0>>,
                         State> &&
          is_result_wrapper<P1>::value> {};

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
        max_result_length(0),
        deterministic(false),
        is_varargs(false),
        check_params_cache_bound(nullptr),
        check_signature(nullptr) {}

  vef_vdf_func_t f;
  vef_prerun_func_t prerun;
  vef_postrun_func_t postrun;
  vef_vdf_clear_func_t clear;
  vef_vdf_accumulate_func_t accumulate;
  vef_type_t return_type;
  std::array<vef_type_t, kMaxParams> param_types;
  size_t num_params;
  size_t buffer_size;
  size_t max_result_length;
  bool deterministic;
  // When true, the function accepts a variable number of arguments. The
  // server skips argument validation and delegates to prerun. num_params
  // and param_types are unused (a varargs StaticFuncDesc has NumParams == 0).
  bool is_varargs;
  bool (*check_params_cache_bound)();
  const char *(*check_signature)(const vef_type_t *, size_t,
                                 const vef_type_t &);
};

// Extracts the params type P from a type operation function pointer,
// or void for non-parameterized signatures.
template <typename F>
struct TypeOpParamsType {
  using type = void;
};
template <typename P>
struct TypeOpParamsType<void (*)(MaybeParams<P> &, std::string_view,
                                 CustomResult)> {
  using type = P;
};
template <typename P>
struct TypeOpParamsType<void (*)(CustomArgWith<P>, StringResult)> {
  using type = P;
};
template <typename P>
struct TypeOpParamsType<int (*)(CustomArgWith<P>, CustomArgWith<P>)> {
  using type = P;
};
template <typename P>
struct TypeOpParamsType<size_t (*)(CustomArgWith<P>)> {
  using type = P;
};

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

// Wraps void(const State&, ResultWrapper) -> vef_vdf_func_t
template <typename State, typename ResultWrapper, auto Func>
struct AggResultWithOutputWrapper {
  static void invoke(vef_context_t *, vef_vdf_args_t *args,
                     vef_vdf_result_t *result) {
    const auto &state = *static_cast<const State *>(args->user_data);
    Func(state, ResultWrapper(result));
  }
};

// Wrappers that convert a typed user prerun/postrun (void(PrerunArgs,
// PrerunResult) / void(PostrunArgs)) into the raw vef_*_func_t ABI shape.
// Used by FuncBuilder::prerun/postrun when the user's hook is typed.

template <auto Hook>
void typed_prerun_wrapper(vef_context_t *, vef_prerun_args_t *args,
                          vef_prerun_result_t *result) {
  Hook(PrerunArgs(args), PrerunResult(result));
}

template <auto Hook>
void typed_postrun_wrapper(vef_context_t *, vef_postrun_args_t *args,
                           vef_postrun_result_t *) {
  Hook(PostrunArgs(args));
}

// Predicates used by FuncBuilder::prerun/postrun to decide which wrapper
// (if any) to install. Each is true exactly when the hook's signature
// matches the typed shape for its slot.

template <auto Hook>
constexpr bool is_typed_prerun() {
  using Params = typename FuncParamTypes<decltype(Hook)>::type;
  if constexpr (std::tuple_size_v<Params> != 2) {
    return false;
  } else {
    return std::is_same_v<std::tuple_element_t<0, Params>, PrerunArgs> &&
           std::is_same_v<std::tuple_element_t<1, Params>, PrerunResult>;
  }
}

template <auto Hook>
constexpr bool is_typed_postrun() {
  using Params = typename FuncParamTypes<decltype(Hook)>::type;
  if constexpr (std::tuple_size_v<Params> != 1) {
    return false;
  } else {
    return std::is_same_v<std::tuple_element_t<0, Params>, PostrunArgs>;
  }
}

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

// Generates a vef_vdf_func_t for a varargs VDF whose signature is
//   void func(vsql::VarArgs, ResultWrapper)
//
// The wrapper constructs a VarArgs view over args and the declared typed
// result wrapper, then calls Func. Argument-count and argument-type
// validation are skipped at this layer (varargs functions delegate that
// to their prerun).
template <auto Func>
struct VarArgsWrapper {
  using Params = typename FuncParamTypes<decltype(Func)>::type;
  static_assert(
      std::tuple_size_v<Params> == 2,
      "vsql .varargs(): function must take exactly (VarArgs, ResultWrapper)");
  using ArgsParam = std::tuple_element_t<0, Params>;
  using ResultParam = std::tuple_element_t<1, Params>;
  static_assert(std::is_same_v<ArgsParam, ::vsql::VarArgs>,
                "vsql .varargs(): first parameter must be vsql::VarArgs");
  static_assert(is_result_wrapper<ResultParam>::value,
                "vsql .varargs(): second parameter must be a result wrapper "
                "(IntResult, RealResult, StringResult, CustomResult, or "
                "CustomResultWith<P>)");

  static void invoke(vef_context_t * /*ctx*/, vef_vdf_args_t *args,
                     vef_vdf_result_t *result) {
    Func(::vsql::VarArgs(args), ResultParam(result));
  }
};

// Wrapper for VDFs whose first parameter is `State&` — typed per-statement
// state allocated in prerun via PrerunResult::emplace_state<State>. The
// wrapper dereferences args->user_data as State* and forwards a reference.
//
// Param tuple shape: <State&, TypedArg..., ResultWrapper>. NumParams is the
// SQL argument count (not counting state or result), so typed args live at
// indices [1, NumParams].
template <auto Func, typename State, size_t NumParams>
struct WrapperTypedState {
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
    State &state = *static_cast<State *>(args->user_data);
    std::array<vef_invalue_t, NumParams> vals{
        get_invalue(ctx, args, static_cast<unsigned int>(Is))...};
    Func(state, make_arg<std::tuple_element_t<1 + Is, Params>>(&vals[Is])...,
         make_result<std::tuple_element_t<1 + NumParams, Params>>(result));
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

// Wrapper for VDFs whose first parameter is `void*` — raw escape hatch for
// extensions that manage state with custom allocators, polymorphic state,
// or anything that doesn't fit emplace_state<T>. The wrapper forwards
// args->user_data straight through.
//
// Param tuple shape: <void*, TypedArg..., ResultWrapper>. Same indexing as
// WrapperTypedState.
//
// Read-only with respect to the user_data slot: any assignment inside the
// VDF body is local because the pointer is copied. Use WrapperVoidStarRefState
// (declare `void*&`) if the body needs to update the slot.
template <auto Func, size_t NumParams>
struct WrapperVoidStarState {
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
    Func(args->user_data,
         make_arg<std::tuple_element_t<1 + Is, Params>>(&vals[Is])...,
         make_result<std::tuple_element_t<1 + NumParams, Params>>(result));
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

// Wrapper for VDFs whose first parameter is `void*&` — like
// WrapperVoidStarState but binds args->user_data by reference so the VDF body
// can write back into the slot. Use this when the body needs to lazily allocate
// scratch space (or otherwise update the pointer) and have the new pointer
// survive across rows and reach postrun for cleanup.
//
// Param tuple shape: <void*&, TypedArg..., ResultWrapper>. Same indexing as
// WrapperVoidStarState.
template <auto Func, size_t NumParams>
struct WrapperVoidStarRefState {
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
    Func(args->user_data,
         make_arg<std::tuple_element_t<1 + Is, Params>>(&vals[Is])...,
         make_result<std::tuple_element_t<1 + NumParams, Params>>(result));
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

// Pre-fills result->type / result->error_msg with a default
// "failed to encode '<input>'" warning, truncating long inputs. Both encode
// wrappers call this before invoking the extension's from_string so that an
// extension that early-returns without setting an outcome still surfaces a
// useful warning.
//
// TODO(villagesql-beta): tighten the contract so every from_string (and
// every other wrapped type-op once they're migrated to typed result
// wrappers) is expected to explicitly call out.set_length / set_null /
// warning / error on every path. This default-WARNING fallback exists today
// because some in-tree extensions (vsql-complex, vsql-simple, vsql-test-only,
// vsql-storage-test) early-return on failure paths without calling anything;
// once they're updated to be explicit, drop this synthesis and treat
// "extension didn't set a result" as an SDK bug.
inline void set_default_encode_failure(vef_vdf_result_t *result,
                                       const vef_invalue_t &arg) {
  result->type = VEF_RESULT_WARNING;
  constexpr size_t kMaxInputDisplay = 64;
  size_t display_len = arg.str_len;
  const char *ellipsis = "";
  if (display_len > kMaxInputDisplay) {
    display_len = kMaxInputDisplay;
    ellipsis = "...";
  }
  snprintf(result->error_msg, VEF_MAX_ERROR_LEN, "failed to encode '%.*s%s'",
           static_cast<int>(display_len), arg.str_value, ellipsis);
}

// TypeEncodeVdfWrapper: wraps TypeEncodeFunc into a VDF.
// VDF signature: (STRING) -> CUSTOM(type).
//
// Pre-sets the result to VEF_RESULT_WARNING with a default
// "failed to encode '<input>'" message; the extension can override by calling
// out.set_length(), out.set_null(), out.warning(), or out.error(); any early
// return without such a call surfaces the default warning.
template <auto Func>
struct TypeEncodeVdfWrapper {
  static void invoke(vef_context_t *ctx, vef_vdf_args_t *args,
                     vef_vdf_result_t *result) {
    vef_invalue_t arg = get_invalue(ctx, args, 0);
    if (arg.is_null) {
      result->type = VEF_RESULT_NULL;
      return;
    }
    set_default_encode_failure(result, arg);
    Func({arg.str_value, arg.str_len}, CustomResult(result));
  }
};

// Pre-fills result->type / result->error_msg with a default "failed to
// decode value" ERROR. The decode wrappers call this before
// invoking the extension's to_string so that an extension that early-returns
// without setting an outcome still surfaces a useful error.
//
// TODO(villagesql-beta): tighten the contract so every to_string explicitly
// sets an outcome (set_length / set / set_null / warning / error) on every
// path, then drop this synthesis and treat the silent-return case as an SDK
// bug. Mirrors the same TODO on set_default_encode_failure.
inline void set_default_decode_failure(vef_vdf_result_t *result) {
  result->type = VEF_RESULT_ERROR;
  snprintf(result->error_msg, VEF_MAX_ERROR_LEN, "failed to decode value");
}

// TypeDecodeVdfWrapper: wraps TypeDecodeFunc into a VDF.
// VDF signature: (CUSTOM(type)) -> STRING.
//
// Pre-sets the result to VEF_RESULT_ERROR with a default "failed to decode
// value" message; the extension can override by calling out.set_length /
// out.set / out.set_null / out.warning / out.error.
template <auto Func>
struct TypeDecodeVdfWrapper {
  static void invoke(vef_context_t *ctx, vef_vdf_args_t *args,
                     vef_vdf_result_t *result) {
    vef_invalue_t arg = get_invalue(ctx, args, 0);
    if (arg.is_null) {
      result->type = VEF_RESULT_NULL;
      return;
    }
    set_default_decode_failure(result);
    Func(CustomArg(&arg), StringResult(result));
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
    result->int_value = Func(CustomArg(&a), CustomArg(&b));
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
    result->int_value = static_cast<long long>(Func(CustomArg(&arg)));
    result->type = VEF_RESULT_VALUE;
  }
};

// Cache-aware wrapper for parameterized types.

template <auto Func>
struct TypeEncodeWithCacheVdfWrapper {
  // Func signature: void(MaybeParams<P>&, string_view, CustomResult).
  // Recover P from the first argument's MaybeParams<P>.
  using FirstArgStripped = std::remove_cv_t<std::remove_reference_t<
      std::tuple_element_t<0, typename FuncParamTypes<decltype(Func)>::type>>>;
  template <typename T>
  struct ExtractP;
  template <typename P_>
  struct ExtractP<MaybeParams<P_>> {
    using type = P_;
  };
  using P = typename ExtractP<FirstArgStripped>::type;

  static void invoke(vef_context_t *ctx, vef_vdf_args_t *args,
                     vef_vdf_result_t *result) {
    vef_invalue_t arg = get_invalue(ctx, args, 0);
    if (arg.is_null) {
      result->type = VEF_RESULT_NULL;
      return;
    }
    // Two call sites reach this wrapper:
    //   - Row time: the server has already resolved the return type's params,
    //     and they arrive via result->type_params (count > 0). We construct
    //     MaybeParams<P> in the known state from the cache.
    //   - fix_fields-time constant-string inference: the server invokes the
    //     encode VDF on a literal to learn the params, passing empty
    //     type_params (count == 0). We leave MaybeParams<P> in the unknown
    //     state so the extension's from_string can call p.set() to publish
    //     them; the wrapper then writes them back via result->out_type_params
    //     below.
    MaybeParams<P> maybe_params;
    const bool input_params_known = result->type_params.count > 0;
    if (input_params_known) {
      maybe_params =
          MaybeParams<P>(type_params_cache_for<P>().get(result->type_params));
    }
    set_default_encode_failure(result, arg);
    Func(maybe_params, {arg.str_value, arg.str_len}, CustomResult(result));

    // Inference-path write-back: when the server invoked us with no input
    // type_params (signalling "please infer"), the wrapper publishes the
    // canonical "k=v,k=v" form of the resulting MaybeParams<P> back to the
    // server via result->out_type_params. All four guards must hold:
    //   1. server opted in by setting result->out_type_params
    //   2. encode succeeded (no warning/error)
    //   3. we were on the inference path (input type_params was empty)
    //   4. the extension actually inferred params and registered to_strings
    if (result->out_type_params != nullptr &&
        result->type == VEF_RESULT_VALUE && !input_params_known &&
        maybe_params.is_known() &&
        type_params_cache_for<P>().has_to_strings()) {
      std::map<std::string, std::string> m;
      type_params_cache_for<P>().to_strings(maybe_params.value(), m);

      // Single-pass greedy write into the caller's buffer. If a pair (with
      // its leading comma if not first) won't fit, stop writing but keep
      // iterating to accumulate `needed` for the snprintf-style overflow
      // signal. Caller retries with a larger buffer.
      char *const buf_begin = result->out_type_params->buf;
      const size_t cap = result->out_type_params->max_buf_len;
      char *p = buf_begin;
      size_t needed = 0;
      bool ok = true;
      bool first = true;
      for (const auto &[k, v] : m) {
        const size_t pair_size = (first ? 0u : 1u) + k.size() + 1u + v.size();
        if (ok && static_cast<size_t>(p - buf_begin) + pair_size <= cap) {
          if (!first) *p++ = ',';
          std::memcpy(p, k.data(), k.size());
          p += k.size();
          *p++ = '=';
          std::memcpy(p, v.data(), v.size());
          p += v.size();
        } else {
          ok = false;
        }
        needed += pair_size;
        first = false;
      }
      result->out_type_params->actual_len = needed;
      result->out_type_params->overflow = !ok;
    }
  }
};

// Recovers P from the first argument of a decode/compare/hash-with-params
// function. After cv/ref stripping the first arg must be CustomArgWith<P>;
// the primary template is left undefined so non-conforming signatures fail
// to compile with a clear pointer at this trait.
template <typename T>
struct ExtractDchParamsType;
template <typename P>
struct ExtractDchParamsType<CustomArgWith<P>> {
  using type = P;
};

template <auto Func>
struct TypeDecodeWithCacheVdfWrapper {
  using FirstArgStripped = std::remove_cv_t<std::remove_reference_t<
      std::tuple_element_t<0, typename FuncParamTypes<decltype(Func)>::type>>>;
  using P = typename ExtractDchParamsType<FirstArgStripped>::type;

  static void invoke(vef_context_t *ctx, vef_vdf_args_t *args,
                     vef_vdf_result_t *result) {
    vef_invalue_t arg = get_invalue(ctx, args, 0);
    if (arg.is_null) {
      result->type = VEF_RESULT_NULL;
      return;
    }
    set_default_decode_failure(result);
    Func(CustomArgWith<P>(&arg), StringResult(result));
  }
};

template <auto Func>
struct TypeCompareWithCacheVdfWrapper {
  using FirstArgStripped = std::remove_cv_t<std::remove_reference_t<
      std::tuple_element_t<0, typename FuncParamTypes<decltype(Func)>::type>>>;
  using P = typename ExtractDchParamsType<FirstArgStripped>::type;

  static void invoke(vef_context_t *ctx, vef_vdf_args_t *args,
                     vef_vdf_result_t *result) {
    vef_invalue_t a = get_invalue(ctx, args, 0);
    vef_invalue_t b = get_invalue(ctx, args, 1);
    if (a.is_null || b.is_null) {
      result->type = VEF_RESULT_NULL;
      return;
    }
    result->int_value = Func(CustomArgWith<P>(&a), CustomArgWith<P>(&b));
    result->type = VEF_RESULT_VALUE;
  }
};

template <auto Func>
struct TypeHashWithCacheVdfWrapper {
  using FirstArgStripped = std::remove_cv_t<std::remove_reference_t<
      std::tuple_element_t<0, typename FuncParamTypes<decltype(Func)>::type>>>;
  using P = typename ExtractDchParamsType<FirstArgStripped>::type;

  static void invoke(vef_context_t *ctx, vef_vdf_args_t *args,
                     vef_vdf_result_t *result) {
    vef_invalue_t arg = get_invalue(ctx, args, 0);
    if (arg.is_null) {
      result->type = VEF_RESULT_NULL;
      return;
    }
    result->int_value = static_cast<long long>(Func(CustomArgWith<P>(&arg)));
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

// Serialize a params map into the canonical "key=value,key=value,..." string.
// Keys and values may not be empty (keys) or contain ',' or '='. On violation,
// writes error_msg and returns true. op_name is the operation being serialized
// for; it prefixes the error message.
inline bool serialize_type_params(
    const std::map<std::string, std::string> &params, const char *op_name,
    std::string &out, char *error_msg) {
  // TODO(villagesql-charset): decide on a broader character set policy.
  out.clear();
  for (const auto &[key, value] : params) {
    if (key.empty() || key.find_first_of(",=") != std::string::npos) {
      snprintf(error_msg, VEF_MAX_ERROR_LEN,
               "%s: key '%s' is empty or contains ',' or '='", op_name,
               key.c_str());
      return true;
    }
    if (value.find_first_of(",=") != std::string::npos) {
      snprintf(error_msg, VEF_MAX_ERROR_LEN,
               "%s: value '%s' contains ',' or '='", op_name, value.c_str());
      return true;
    }
    if (!out.empty()) out += ',';
    out += key;
    out += '=';
    out += value;
  }
  return false;
}

// Appends ",<byte-len>[,key=value,...]" to result->str_buf, advancing
// actual_len. Used only by the mutating resolve_params overload so the server
// adopts the rewritten params as canonical. Prefixing the byte length of the
// serialized blob makes the params section self-delimiting: the server reads
// exactly that many bytes, leaving room to append further trailing fields later
// without ambiguity. Returns true and writes error_msg on a serialization error
// or if the combined result would overflow str_buf.
inline bool add_mutated_params(const std::map<std::string, std::string> &params,
                               vef_vdf_result_t *result) {
  std::string serialized;
  if (serialize_type_params(params, "resolve_params", serialized,
                            result->error_msg)) {
    return true;
  }
  std::string tail = ",";
  tail += std::to_string(serialized.size());
  if (!serialized.empty()) {
    tail += ',';
    tail += serialized;
  }
  if (result->actual_len + tail.size() > result->max_str_len) {
    snprintf(result->error_msg, VEF_MAX_ERROR_LEN,
             "resolve_params result too large for buffer");
    return true;
  }
  memcpy(result->str_buf + result->actual_len, tail.data(), tail.size());
  result->actual_len += tail.size();
  return false;
}

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

    std::string serialized;
    if (serialize_type_params(params, "int_to_params", serialized,
                              result->error_msg)) {
      result->type = VEF_RESULT_ERROR;
      return;
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

// ResolveParamsWrapper: wraps a resolve_params callback into a VDF.
// VDF signature: (STRING) -> STRING.
//
// The result is "persisted_length,max_decode_buffer_length". When Func is the
// mutating overload (ResolveTypeParamsMutableFunc), the (possibly rewritten)
// params are appended as "...,<byte-len>[,key=value,...]" so the server can
// adopt them as the canonical parameters. The const overload emits only the two
// numbers.
template <auto Func>
struct ResolveParamsWrapper {
  static constexpr bool is_mutable =
      std::is_same_v<decltype(Func), ResolveTypeParamsMutableFunc>;

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
    result->actual_len = static_cast<size_t>(written);

    // The mutating overload may have rewritten params; append them so the
    // server adopts the rewritten set as canonical.
    if constexpr (is_mutable) {
      if (add_mutated_params(params, result)) {
        result->type = VEF_RESULT_ERROR;
        return;
      }
    }

    result->type = VEF_RESULT_VALUE;
  }
};

// StaticFuncDesc lives in the detail namespace. Extension authors pass
// instances to .func() on the extension builder but never name or call
// methods on this type directly. Keeping it here means ABI-typed members are
// not part of the public SDK surface, without requiring private/friend
// (which would strip the visibility("hidden") attribute from
// materialize_func_desc and re-introduce the func_desc sharing bug).
template <size_t NumParams>
struct StaticFuncDesc {
  const char *name_;
  vef_type_t params_[NumParams > 0 ? NumParams : 1];
  vef_type_t return_type_;
  vef_vdf_func_t vdf_;
  vef_prerun_func_t prerun_;
  vef_postrun_func_t postrun_;
  vef_vdf_clear_func_t clear_;
  vef_vdf_accumulate_func_t accumulate_;
  size_t buffer_size_;
  size_t max_result_length_;
  bool deterministic_;
  bool is_varargs_;
  bool (*check_params_cache_bound_)();
  const char *(*check_signature_)(const vef_type_t *, size_t,
                                  const vef_type_t &);

  constexpr const char *name() const { return name_; }
  // For varargs: reports VEF_PARAM_VARARGS so materialize_func_desc writes
  // the sentinel into vef_signature_t::param_count and the server (and
  // tooling that reads it back, e.g. extension_registration) treat the
  // function as variadic.
  constexpr size_t num_params() const {
    return is_varargs_ ? VEF_PARAM_VARARGS : NumParams;
  }
  // Varargs reads args->values (protocol-3 pointer-array layout) without a
  // protocol-1 fallback, so a varargs VDF cannot run on protocol 1.
  constexpr vef_protocol_t required_protocol() const {
    if (max_result_length_ > 0) return VEF_PROTOCOL_4;
    return is_varargs_ ? VEF_PROTOCOL_3 : VEF_PROTOCOL_1;
  }
  constexpr size_t buffer_size() const { return buffer_size_; }
  constexpr size_t max_result_length() const { return max_result_length_; }
  constexpr bool deterministic() const { return deterministic_; }
  constexpr auto check_params_cache_bound() const -> bool (*)() {
    return check_params_cache_bound_;
  }
  constexpr auto check_signature() const -> const
      char *(*)(const vef_type_t *, size_t, const vef_type_t &) {
    return check_signature_;
  }
  constexpr void init_name() const {}
  constexpr const vef_type_t *params() const { return params_; }
  constexpr vef_type_t return_type() const { return return_type_; }
  constexpr vef_vdf_func_t vdf() const { return vdf_; }
  constexpr vef_prerun_func_t prerun() const { return prerun_; }
  constexpr vef_postrun_func_t postrun() const { return postrun_; }
  constexpr vef_vdf_clear_func_t clear() const { return clear_; }
  constexpr vef_vdf_accumulate_func_t accumulate() const { return accumulate_; }

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
        max_result_length_(meta.max_result_length),
        deterministic_(meta.deterministic),
        is_varargs_(meta.is_varargs),
        check_params_cache_bound_(meta.check_params_cache_bound),
        check_signature_(meta.check_signature) {
    for (size_t i = 0; i < NumParams && i < meta.num_params; ++i) {
      params_[i] = meta.param_types[i];
    }
  }
};

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

}  // namespace detail
}  // namespace func_builder
}  // namespace vsql

#endif  // VILLAGESQL_DETAIL_FUNC_BUILDER_H
