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

#include <array>
#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>

#include <villagesql/vsql/func_types.h>
#include <villagesql/vsql/type_params.h>

// Storage characteristics resolved from type parameters.
namespace vsql {
struct ResolvedTypeParams {
  int64_t persisted_length;
  int64_t max_decode_buffer_length;
};
}  // namespace vsql

// IntToTypeParamsFunc and ResolveTypeParamsFunc must be declared before
// villagesql/detail/func_builder.h because that header uses them as non-type
// template parameters in IntToParamsWrapper and ResolveParamsWrapper.
namespace vsql {
namespace func_builder {

// int_to_params: converts MYTYPE(N) integer to parameter key-value pairs.
using IntToTypeParamsFunc = bool (*)(int64_t value,
                                     std::map<std::string, std::string> &params,
                                     char *error_msg);

// resolve_params: validates type parameters and computes storage sizes.
using ResolveTypeParamsFunc =
    bool (*)(const std::map<std::string, std::string> &params,
             vsql::ResolvedTypeParams *result, char *error_msg);

}  // namespace func_builder
}  // namespace vsql

#include <villagesql/detail/func_builder.h>

namespace vsql {
namespace func_builder {

constexpr const char *STRING = "STRING";
constexpr const char *INT = "INT";
constexpr const char *REAL = "REAL";

// Encode: string -> custom binary. The function reports its outcome by
// calling out.set_length(n), out.set_null(), out.warning(msg), or
// out.error(msg). If none is called, the result is undefined.
using TypeEncodeFunc = void (*)(std::string_view from, CustomResult out);
// Parameterized variant: If known, MaybeParams<P> carries the params, to
// be used when parsing the string value. This may also be called on strings
// where the type parameters are not known before this call, and this
// function must infer the type parameters from the string, and set them via
// MaybeParams<P> so that the type can be correctly carried through the rest
// of the SQL statement.
//
// The result wrapper is plain CustomResult (not CustomResultWith<P>) because
// params come from the MaybeParams<P>& argument.
template <typename P>
using TypeEncodeWithParamsFunc = void (*)(MaybeParams<P> &,
                                          std::string_view from, CustomResult);

// Decode: custom -> string. The function reports its outcome by calling
// out.set_length(n), out.set(sv), out.set_null(), out.warning(msg), or
// out.error(msg). If none is called the wrapper falls back to a default
// "failed to decode value" ERROR.
using TypeDecodeFunc = void (*)(CustomArg in, StringResult out);
// Parameterized variant: params come from the CustomArgWith<P> input.
template <typename P>
using TypeDecodeWithParamsFunc = void (*)(CustomArgWith<P> in,
                                          StringResult out);

// Compare: returns <0, 0, or >0.
using TypeCompareFunc = int (*)(CustomArg a, CustomArg b);
template <typename P>
using TypeCompareWithParamsFunc = int (*)(CustomArgWith<P> a,
                                          CustomArgWith<P> b);

// Hash: returns hash code.
using TypeHashFunc = size_t (*)(CustomArg in);
template <typename P>
using TypeHashWithParamsFunc = size_t (*)(CustomArgWith<P> in);

// intrinsic_default: returns string representation of the default value.
using IntrinsicDefaultFunc = std::string (*)(char *error_msg);
template <typename P>
using IntrinsicDefaultWithParamsFunc = std::string (*)(const P &,
                                                       char *error_msg);

// params_to_strings: converts the typed params struct P into its canonical
// key/value string form (e.g., SVectorParams{6} -> {"dimension":"6"}). This
// is the inverse direction of the parse callback an extension registers via
// `.params<P, &Parse>()` on the type builder, whose signature is
//   P parse(const std::map<std::string,std::string>&)
// — Parse turns server-supplied strings into a typed P; params_to_strings
// turns a typed P back into strings the server can read.
//
// Used by inference paths (e.g. constant-string from_string pre-execute at
// fix_fields time) that produce a P and need to publish the equivalent
// string-form params back to the server.
//
// The map keys/values are subject to the same rules as int_to_params: keys
// are non-empty and free of ',' and '='; values are free of ',' and '='.
template <typename P>
using ParamsToStringsFunc = void (*)(const P &,
                                     std::map<std::string, std::string> &);

// =============================================================================
// FuncBuilder
// =============================================================================

template <auto Func, size_t NumParams>
class FuncBuilder {
 public:
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

  // Marks the function as deterministic: identical inputs always produce
  // identical outputs. The optimizer may cache or elide calls for
  // constant-folding and query rewriting. Defaults to false.
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

  constexpr detail::StaticFuncDesc<NumParams> build() const {
    static_assert(NumParams <= kMaxParams,
                  "Too many parameters (max is kMaxParams)");

    using AllParams = typename detail::FuncParamTypes<decltype(Func)>::type;
    using ReturnType = typename detail::FuncReturnType<decltype(Func)>::type;
    using Shape = detail::func_signature_shape<AllParams, NumParams>;
    static_assert(std::is_void_v<ReturnType>,
                  "make_func: C++ function must return void and set the SQL "
                  "result through the final typed result wrapper parameter");
    static_assert(std::tuple_size_v<AllParams> == NumParams + 1,
                  "make_func: C++ function must have one typed result wrapper "
                  "after the declared SQL parameters; check the number of "
                  ".param(...) calls");
    static_assert(
        std::tuple_size_v<AllParams> != NumParams + 1 || Shape::args_ok,
        "make_func: every C++ function parameter before the result "
        "must be a typed argument wrapper such as IntArg, RealArg, "
        "StringArg, CustomArg, or CustomArgWith<P>");
    static_assert(
        std::tuple_size_v<AllParams> != NumParams + 1 || Shape::result_ok,
        "make_func: final C++ function parameter must be a typed "
        "result wrapper such as IntResult, RealResult, StringResult, "
        "CustomResult, or CustomResultWith<P>");

    detail::FuncWithMetadata meta{};
    meta.prerun = prerun_;
    meta.postrun = postrun_;
    meta.return_type = detail::to_vef_type(return_type_);
    meta.num_params = NumParams;
    meta.buffer_size = buffer_size_;
    meta.deterministic = deterministic_;
    for (size_t i = 0; i < NumParams; ++i) {
      meta.param_types[i] = detail::to_vef_type(param_types_[i]);
    }
    if constexpr (std::tuple_size_v<AllParams> == NumParams + 1 &&
                  Shape::args_ok && Shape::result_ok) {
      using UniquePTuple =
          typename detail::unique_params_types<AllParams>::type;
      meta.f = &detail::Wrapper<Func, NumParams>::invoke;
      if constexpr (std::tuple_size_v<UniquePTuple> > 0) {
        meta.check_params_cache_bound =
            &detail::apply_params_cache_checker<UniquePTuple>::check;
      }
      meta.check_signature =
          &detail::signature_checker<AllParams, NumParams>::check;
    }

    return detail::StaticFuncDesc<NumParams>(name_, meta);
  }

 private:
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

  template <auto F, size_t M>
  friend class FuncBuilder;

  template <auto F>
  friend constexpr FuncBuilder<F, 0> make_func(const char *);
};

// =============================================================================
// AggFuncBuilder
// =============================================================================
//
// Builder for aggregate VDFs. Use make_aggregate_func<State, &result_fn>().
// The State type is explicit, and clear/accumulate signatures are validated
// against State at compile time.

template <typename State, auto Func, size_t NumParams>
class AggFuncBuilder {
 public:
  constexpr AggFuncBuilder<State, Func, NumParams> &returns(const char *t) {
    return_type_ = t;
    return *this;
  }

  constexpr AggFuncBuilder<State, Func, NumParams + 1> param(
      const char *t) const {
    AggFuncBuilder<State, Func, NumParams + 1> next;
    next.name_ = name_;
    next.return_type_ = return_type_;
    next.buffer_size_ = buffer_size_;
    next.clear_ = clear_;
    next.accumulate_ = accumulate_;
    next.deterministic_ = deterministic_;
    for (size_t i = 0; i < NumParams; ++i) {
      next.param_types_[i] = param_types_[i];
    }
    next.param_types_[NumParams] = t;
    return next;
  }

  constexpr AggFuncBuilder<State, Func, NumParams> &buffer_size(size_t s) {
    buffer_size_ = s;
    return *this;
  }

  constexpr AggFuncBuilder<State, Func, NumParams> &deterministic(
      bool d = true) {
    deterministic_ = d;
    return *this;
  }

  // void(State&) — first parameter must match the State type declared in
  // make_aggregate_func<State, ...>.
  template <auto Fn>
  constexpr AggFuncBuilder<State, Func, NumParams> &clear() {
    using Params = typename detail::FuncParamTypes<decltype(Fn)>::type;
    using ReturnType = typename detail::FuncReturnType<decltype(Fn)>::type;
    using DeducedState =
        std::remove_reference_t<std::tuple_element_t<0, Params>>;
    static_assert(std::is_void_v<ReturnType>,
                  "clear: C++ function must return void");
    static_assert(std::is_same_v<DeducedState, State>,
                  "clear: first parameter must be State& matching the "
                  "make_aggregate_func State type");
    clear_ = &detail::agg_clear_wrapper<State, Fn>;
    return *this;
  }

  // void(State&, TypedArgs...) — first parameter must match State; remaining
  // parameters must match the SQL param count declared via .param() calls.
  // Call .accumulate() after all .param() calls.
  template <auto Fn>
  constexpr AggFuncBuilder<State, Func, NumParams> &accumulate() {
    using Params = typename detail::FuncParamTypes<decltype(Fn)>::type;
    using ReturnType = typename detail::FuncReturnType<decltype(Fn)>::type;
    using DeducedState =
        std::remove_reference_t<std::tuple_element_t<0, Params>>;
    static_assert(std::is_void_v<ReturnType>,
                  "accumulate: C++ function must return void");
    static_assert(std::is_same_v<DeducedState, State>,
                  "accumulate: first parameter must be State& matching the "
                  "make_aggregate_func State type");
    static_assert(
        std::tuple_size_v<Params> == NumParams + 1,
        "accumulate: typed arg count after State& must match the SQL "
        "parameter count; ensure .accumulate() is called after .param()");
    static_assert(
        std::tuple_size_v<Params> != NumParams + 1 ||
            detail::accumulate_signature_shape<Params, NumParams>::args_ok,
        "accumulate: every C++ parameter after State& must be a typed argument "
        "wrapper such as IntArg, RealArg, StringArg, CustomArg, or "
        "CustomArgWith<P>");
    accumulate_ = &detail::AggAccumulateWrapper<State, Fn, NumParams>::invoke;
    return *this;
  }

  constexpr detail::StaticFuncDesc<NumParams> build() const {
    static_assert(NumParams <= kMaxParams,
                  "Too many parameters (max is kMaxParams)");
    if ((clear_ == nullptr) != (accumulate_ == nullptr)) {
      detail::config_error__aggregate_must_set_both_clear_and_accumulate();
    }

    using AllParams = typename detail::FuncParamTypes<decltype(Func)>::type;
    using UniquePTuple = typename detail::unique_params_types<AllParams>::type;

    detail::FuncWithMetadata meta{};
    // void(const State&, ResultWrapper).
    using ResultWrapper = std::tuple_element_t<1, AllParams>;
    meta.f =
        &detail::AggResultWithOutputWrapper<State, ResultWrapper, Func>::invoke;
    meta.prerun = &detail::auto_prerun<State>;
    meta.postrun = &detail::auto_postrun<State>;
    meta.clear = clear_;
    meta.accumulate = accumulate_;
    meta.return_type = detail::to_vef_type(return_type_);
    meta.num_params = NumParams;
    meta.buffer_size = buffer_size_;
    meta.deterministic = deterministic_;
    for (size_t i = 0; i < NumParams; ++i) {
      meta.param_types[i] = detail::to_vef_type(param_types_[i]);
    }
    if constexpr (std::tuple_size_v<UniquePTuple> > 0) {
      meta.check_params_cache_bound =
          &detail::apply_params_cache_checker<UniquePTuple>::check;
    }
    return detail::StaticFuncDesc<NumParams>(name_, meta);
  }

 private:
  constexpr AggFuncBuilder()
      : name_(nullptr),
        return_type_(nullptr),
        param_types_{},
        buffer_size_(0),
        deterministic_(false),
        clear_(nullptr),
        accumulate_(nullptr) {}

  const char *name_;
  const char *return_type_;
  std::array<const char *, NumParams> param_types_;
  size_t buffer_size_;
  bool deterministic_;
  vef_vdf_clear_func_t clear_;
  vef_vdf_accumulate_func_t accumulate_;

  template <typename S, auto F, size_t M>
  friend class AggFuncBuilder;

  template <typename S, auto F>
  friend constexpr AggFuncBuilder<S, F, 0> make_aggregate_func(const char *);
};

// =============================================================================
// Entry Points
// =============================================================================

// make_aggregate_func<State, &result_fn>("name")
//
// Entry point for aggregate VDFs. State is the per-group accumulation type.
// The result function must be: void(const State&, ResultWrapper)
// where ResultWrapper is one of IntResult, RealResult, StringResult,
// CustomResult, or CustomResultWith<P>.
//
// prerun/postrun are auto-generated: prerun allocates State via
// value-initialization, postrun deletes it.
template <typename State, auto Func>
constexpr AggFuncBuilder<State, Func, 0> make_aggregate_func(const char *name) {
  using AllParams = typename detail::FuncParamTypes<decltype(Func)>::type;
  using ReturnType = typename detail::FuncReturnType<decltype(Func)>::type;
  static_assert(
      std::is_void_v<ReturnType> &&
          detail::is_agg_result_for_state<State, AllParams>::value,
      "make_aggregate_func: result function must be "
      "void(const State&, ResultWrapper) where ResultWrapper is IntResult, "
      "RealResult, StringResult, CustomResult, or CustomResultWith<P>");
  AggFuncBuilder<State, Func, 0> builder;
  builder.name_ = name;
  return builder;
}

// make_func<&impl>("name")
template <auto Func>
constexpr FuncBuilder<Func, 0> make_func(const char *name) {
  using AllParams = typename detail::FuncParamTypes<decltype(Func)>::type;
  static_assert(
      std::tuple_size_v<AllParams> == 0 ||
          !detail::is_context_param<std::tuple_element_t<0, AllParams>>::value,
      "vsql make_func: deprecated vef_context_t* first parameter not "
      "supported; use a typed function or make_aggregate_func (see "
      "extension.h)");
  FuncBuilder<Func, 0> builder;
  builder.name_ = name;
  return builder;
}

// make_type_encode<&fn>("name", TYPE) — (STRING) -> CUSTOM(type).
//
// Accepts two signatures:
//   TypeEncodeFunc              (non-parameterized)
//   TypeEncodeWithParamsFunc<P> (parameterized)
template <auto Func>
constexpr detail::StaticFuncDesc<1> make_type_encode(const char *name,
                                                     const char *type_name) {
  using F = decltype(Func);
  detail::FuncWithMetadata meta{};
  if constexpr (std::is_same_v<F, TypeEncodeFunc>) {
    meta.f = &detail::TypeEncodeVdfWrapper<Func>::invoke;
  } else {
    using P = typename detail::TypeEncodeWithCacheVdfWrapper<Func>::P;
    static_assert(std::is_same_v<F, TypeEncodeWithParamsFunc<P>>,
                  "make_type_encode: function must match either "
                  "TypeEncodeFunc or TypeEncodeWithParamsFunc<P>.");
    meta.f = &detail::TypeEncodeWithCacheVdfWrapper<Func>::invoke;
    meta.check_params_cache_bound = &is_params_cache_bound<P>;
  }
  meta.return_type = detail::to_vef_type(type_name);
  meta.param_types[0] = detail::to_vef_type(STRING);
  meta.num_params = 1;
  meta.buffer_size = 0;
  meta.deterministic = true;
  return detail::StaticFuncDesc<1>(name, meta);
}

// make_type_decode<&fn>("name", TYPE) — (CUSTOM(type)) -> STRING.
//
// Accepts two signatures:
//   TypeDecodeFunc              (non-parameterized)
//   TypeDecodeWithParamsFunc<P> (parameterized)
template <auto Func>
constexpr detail::StaticFuncDesc<1> make_type_decode(const char *name,
                                                     const char *type_name) {
  using F = decltype(Func);
  detail::FuncWithMetadata meta{};
  if constexpr (std::is_same_v<F, TypeDecodeFunc>) {
    meta.f = &detail::TypeDecodeVdfWrapper<Func>::invoke;
  } else {
    using P = typename detail::TypeDecodeWithCacheVdfWrapper<Func>::P;
    static_assert(std::is_same_v<F, TypeDecodeWithParamsFunc<P>>,
                  "make_type_decode: function must match either "
                  "TypeDecodeFunc or TypeDecodeWithParamsFunc<P>.");
    meta.f = &detail::TypeDecodeWithCacheVdfWrapper<Func>::invoke;
    meta.check_params_cache_bound = &is_params_cache_bound<P>;
  }
  meta.return_type = detail::to_vef_type(STRING);
  meta.param_types[0] = detail::to_vef_type(type_name);
  meta.num_params = 1;
  meta.buffer_size = 0;
  meta.deterministic = true;
  return detail::StaticFuncDesc<1>(name, meta);
}

// make_type_compare<&fn>("name", TYPE) — (CUSTOM(type), CUSTOM(type)) -> INT.
//
// Accepts two signatures:
//   TypeCompareFunc              (non-parameterized)
//   TypeCompareWithParamsFunc<P> (parameterized)
template <auto Func>
constexpr detail::StaticFuncDesc<2> make_type_compare(const char *name,
                                                      const char *type_name) {
  using F = decltype(Func);
  detail::FuncWithMetadata meta{};
  if constexpr (std::is_same_v<F, TypeCompareFunc>) {
    meta.f = &detail::TypeCompareVdfWrapper<Func>::invoke;
  } else {
    using P = typename detail::TypeCompareWithCacheVdfWrapper<Func>::P;
    static_assert(std::is_same_v<F, TypeCompareWithParamsFunc<P>>,
                  "make_type_compare: function must match either "
                  "TypeCompareFunc or TypeCompareWithParamsFunc<P>.");
    meta.f = &detail::TypeCompareWithCacheVdfWrapper<Func>::invoke;
    meta.check_params_cache_bound = &is_params_cache_bound<P>;
  }
  meta.return_type = detail::to_vef_type(INT);
  meta.param_types[0] = detail::to_vef_type(type_name);
  meta.param_types[1] = detail::to_vef_type(type_name);
  meta.num_params = 2;
  meta.buffer_size = 0;
  meta.deterministic = true;
  return detail::StaticFuncDesc<2>(name, meta);
}

// make_type_hash<&fn>("name", TYPE) — (CUSTOM(type)) -> INT.
//
// Accepts two signatures:
//   TypeHashFunc              (non-parameterized)
//   TypeHashWithParamsFunc<P> (parameterized)
template <auto Func>
constexpr detail::StaticFuncDesc<1> make_type_hash(const char *name,
                                                   const char *type_name) {
  using F = decltype(Func);
  detail::FuncWithMetadata meta{};
  if constexpr (std::is_same_v<F, TypeHashFunc>) {
    meta.f = &detail::TypeHashVdfWrapper<Func>::invoke;
  } else {
    using P = typename detail::TypeHashWithCacheVdfWrapper<Func>::P;
    static_assert(std::is_same_v<F, TypeHashWithParamsFunc<P>>,
                  "make_type_hash: function must match either "
                  "TypeHashFunc or TypeHashWithParamsFunc<P>.");
    meta.f = &detail::TypeHashWithCacheVdfWrapper<Func>::invoke;
    meta.check_params_cache_bound = &is_params_cache_bound<P>;
  }
  meta.return_type = detail::to_vef_type(INT);
  meta.param_types[0] = detail::to_vef_type(type_name);
  meta.num_params = 1;
  meta.buffer_size = 0;
  meta.deterministic = true;
  return detail::StaticFuncDesc<1>(name, meta);
}

// make_int_to_params<&fn>("name") — (INT) -> STRING.
template <IntToTypeParamsFunc Func>
constexpr detail::StaticFuncDesc<1> make_int_to_params(const char *name) {
  detail::FuncWithMetadata meta{};
  meta.f = &detail::IntToParamsWrapper<Func>::invoke;
  meta.return_type = detail::to_vef_type(STRING);
  meta.param_types[0] = detail::to_vef_type(INT);
  meta.num_params = 1;
  meta.buffer_size = VEF_MAX_TYPE_PARAMS_STRING_LEN;
  meta.deterministic = true;
  return detail::StaticFuncDesc<1>(name, meta);
}

// make_resolve_params<&fn>("name") — (STRING) -> STRING.
template <ResolveTypeParamsFunc Func>
constexpr detail::StaticFuncDesc<1> make_resolve_params(const char *name) {
  detail::FuncWithMetadata meta{};
  meta.f = &detail::ResolveParamsWrapper<Func>::invoke;
  meta.return_type = detail::to_vef_type(STRING);
  meta.param_types[0] = detail::to_vef_type(STRING);
  meta.num_params = 1;
  meta.buffer_size = VEF_MAX_TYPE_PARAMS_STRING_LEN;
  meta.deterministic = true;
  return detail::StaticFuncDesc<1>(name, meta);
}

// make_intrinsic_default<&fn>("name") — () -> STRING.
template <auto Func>
constexpr detail::StaticFuncDesc<0> make_intrinsic_default(const char *name) {
  detail::FuncWithMetadata meta{};
  if constexpr (std::is_same_v<decltype(Func), IntrinsicDefaultFunc>) {
    meta.f = &detail::IntrinsicDefaultWrapper<Func>::invoke;
  } else {
    meta.f = &detail::IntrinsicDefaultWithCacheWrapper<Func>::invoke;
    meta.check_params_cache_bound = &is_params_cache_bound<
        typename detail::IntrinsicDefaultWithCacheWrapper<Func>::P>;
  }
  meta.return_type = detail::to_vef_type(STRING);
  meta.num_params = 0;
  meta.buffer_size = VEF_MAX_TYPE_PARAMS_STRING_LEN;
  return detail::StaticFuncDesc<0>(name, meta);
}

}  // namespace func_builder
}  // namespace vsql

#endif  // VILLAGESQL_VSQL_FUNC_BUILDER_H
