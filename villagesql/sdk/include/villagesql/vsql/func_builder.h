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
// use typed arguments and results (IntArg, RealArg, StringArg,
// CustomArg, etc.).
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
#include <villagesql/vsql/pre_post_run.h>
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
// The server pre-validates the map, so every key is non-empty, appears exactly
// once, and has a non-empty value. What the keys mean, and which are required,
// is entirely up to the type.
using ResolveTypeParamsFunc =
    bool (*)(const std::map<std::string, std::string> &params,
             vsql::ResolvedTypeParams *result, char *error_msg);

// resolve_params (mutating overload): same as ResolveTypeParamsFunc but the
// params map is passed by non-const reference, so the type may rewrite it
// (e.g. fill in default values. The rewritten map is serialized back and
// becomes the canonical parameter string persisted for the type. The rewrite
// must be idempotent: resolving the rewritten params again must yield the same
// params and storage sizes.
// The rewrite may add keys and change values, but it must not erase a key or
// blank out a value: the server rejects the DDL rather than silently dropping
// a parameter the user wrote. To reject an unwanted key, return an error.
using ResolveTypeParamsMutableFunc =
    bool (*)(std::map<std::string, std::string> &params,
             vsql::ResolvedTypeParams *result, char *error_msg);

}  // namespace func_builder
}  // namespace vsql

#include <villagesql/detail/func_builder.h>

namespace vsql {
namespace func_builder {

constexpr const char *STRING = "STRING";
constexpr const char *INT = "INT";
constexpr const char *REAL = "REAL";

namespace detail {

// Compile-time traps for function-builder misuse. These are intentionally left
// undefined and are NOT constexpr. Every extension is registered inside a
// `static constexpr` builder chain (see VEF_GENERATE_ENTRY_POINTS), so the
// whole chain is constant-evaluated; reaching a call to one of these during
// that evaluation is ill-formed and the function name is the compiler's
// diagnostic. (If a builder is instead used at runtime, the bad path fails to
// link, for the same absence of a definition.)
[[noreturn]] void max_result_length_is_only_valid_for_a_STRING_return_type();
[[noreturn]] void call_returns_before_max_result_length();

}  // namespace detail

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
// The result is plain CustomResult (not CustomResultWith<P>) because
// params come from the MaybeParams<P>& argument.
template <typename P>
using TypeEncodeWithParamsFunc = void (*)(MaybeParams<P> &,
                                          std::string_view from, CustomResult);

// Decode: custom -> string. The function reports its outcome by calling
// out.set_length(n), out.set(sv), out.set_null(), out.warning(msg), or
// out.error(msg). If none is called then the result falls back to a default
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

// Real value: converts a custom value to REAL for numeric contexts.
using TypeRealValueFunc = void (*)(CustomArg in, RealResult out);
template <typename P>
using TypeRealValueWithParamsFunc = void (*)(CustomArgWith<P> in,
                                             RealResult out);

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

// FuncBuilder
//
// HasPrerun is set to true by .prerun<>(). build() requires it when Func's
// signature reads state from user_data (void(State&,...) / void(void*,...)),
// so that registering a state-style VDF without a prerun is a compile error
// rather than a runtime null dereference.

// Selects between fixed-arity and varargs builders. A builder starts in
// kUnset; .param(TYPE) (or .no_params() for zero-arity) transitions to
// kFixed, .varargs() transitions to kVarargs. The two are mutually
// exclusive, enforced by static_assert.
enum class ParamMode { kUnset, kFixed, kVarargs };

template <auto Func, size_t NumParams, ParamMode Mode = ParamMode::kUnset,
          bool HasPrerun = false>
class FuncBuilder {
 public:
  constexpr FuncBuilder<Func, NumParams, Mode, HasPrerun> &returns(
      const char *t) {
    return_type_ = t;
    return *this;
  }

  // Add a typed parameter. Once called, no further .varargs() is allowed.
  constexpr FuncBuilder<Func, NumParams + 1, ParamMode::kFixed, HasPrerun>
  param(const char *t) const {
    static_assert(Mode != ParamMode::kVarargs,
                  ".param(TYPE) and .varargs() are mutually exclusive");
    static_assert(Mode != ParamMode::kFixed || NumParams > 0,
                  ".param(TYPE) and .no_params() are mutually exclusive");
    FuncBuilder<Func, NumParams + 1, ParamMode::kFixed, HasPrerun> next;
    next.name_ = name_;
    next.return_type_ = return_type_;
    next.buffer_size_ = buffer_size_;
    next.max_result_length_ = max_result_length_;
    next.prerun_ = prerun_;
    next.postrun_ = postrun_;
    next.deterministic_ = deterministic_;
    for (size_t i = 0; i < NumParams; ++i) {
      next.param_types_[i] = param_types_[i];
    }
    next.param_types_[NumParams] = t;
    return next;
  }

  // Explicitly declare zero-arity. Useful for functions that read only
  // session state or return a constant. Mutually exclusive with .param(TYPE)
  // and .varargs().
  constexpr FuncBuilder<Func, 0, ParamMode::kFixed, HasPrerun> no_params()
      const {
    static_assert(NumParams == 0,
                  ".no_params() and .param(TYPE) are mutually exclusive");
    static_assert(Mode != ParamMode::kVarargs,
                  ".no_params() and .varargs() are mutually exclusive");
    FuncBuilder<Func, 0, ParamMode::kFixed, HasPrerun> next;
    next.name_ = name_;
    next.return_type_ = return_type_;
    next.buffer_size_ = buffer_size_;
    next.max_result_length_ = max_result_length_;
    next.prerun_ = prerun_;
    next.postrun_ = postrun_;
    next.deterministic_ = deterministic_;
    return next;
  }

  // Declare a varargs VDF. The function signature must be
  //   void(vsql::VarArgs, TypedResult)
  // The prerun hook is responsible for validating argument count and types.
  // Mutually exclusive with .no_params() / .param(TYPE).
  constexpr FuncBuilder<Func, 0, ParamMode::kVarargs, HasPrerun> varargs()
      const {
    static_assert(Mode != ParamMode::kFixed,
                  ".varargs() is mutually exclusive with .no_params() and "
                  ".param(TYPE)");
    FuncBuilder<Func, 0, ParamMode::kVarargs, HasPrerun> next;
    next.name_ = name_;
    next.return_type_ = return_type_;
    next.buffer_size_ = buffer_size_;
    next.max_result_length_ = max_result_length_;
    next.prerun_ = prerun_;
    next.postrun_ = postrun_;
    next.deterministic_ = deterministic_;
    return next;
  }

  constexpr FuncBuilder<Func, NumParams, Mode, HasPrerun> &buffer_size(
      size_t s) {
    buffer_size_ = s;
    return *this;
  }

  // Declare the maximum length of the STRING result, in characters, so the
  // result column is sized to hold the full value when materialized. A
  // STRING-returning VDF that does not declare this falls back to the argument
  // width (a large result then truncates when materialized). Values above
  // VEF_MAX_RESULT_LENGTH are capped by the server. Bumps the required protocol
  // to VEF_PROTOCOL_4.
  //
  // Only valid for a STRING return type. Call .returns(STRING) before this so
  // the return type is known here: a non-STRING return (or an undeclared one)
  // is rejected at build time rather than silently ignored.
  constexpr FuncBuilder<Func, NumParams, Mode, HasPrerun> &max_result_length(
      size_t n) {
    if (return_type_ == nullptr) {
      detail::call_returns_before_max_result_length();
    } else if (detail::to_vef_type(return_type_).id != VEF_TYPE_STRING) {
      detail::max_result_length_is_only_valid_for_a_STRING_return_type();
    }
    max_result_length_ = n;
    return *this;
  }

  // Marks the function as deterministic: identical inputs always produce
  // identical outputs. The optimizer may cache or elide calls for
  // constant-folding and query rewriting. Defaults to false.
  constexpr FuncBuilder<Func, NumParams, Mode, HasPrerun> &deterministic(
      bool d = true) {
    deterministic_ = d;
    return *this;
  }

  template <auto Hook>
  constexpr FuncBuilder<Func, NumParams, Mode, true> prerun() const {
    static_assert(detail::is_typed_prerun<Hook>(),
                  "prerun<Hook>(): Hook must be void(PrerunArgs, "
                  "PrerunResult). Raw ABI signatures are not accepted.");
    FuncBuilder<Func, NumParams, Mode, true> next;
    next.name_ = name_;
    next.return_type_ = return_type_;
    next.buffer_size_ = buffer_size_;
    next.max_result_length_ = max_result_length_;
    next.prerun_ = &detail::typed_prerun_wrapper<Hook>;
    next.postrun_ = postrun_;
    next.deterministic_ = deterministic_;
    for (size_t i = 0; i < NumParams; ++i) {
      next.param_types_[i] = param_types_[i];
    }
    return next;
  }

  template <auto Hook>
  constexpr FuncBuilder<Func, NumParams, Mode, HasPrerun> &postrun() {
    static_assert(detail::is_typed_postrun<Hook>(),
                  "postrun<Hook>(): Hook must be void(PostrunArgs). Raw ABI "
                  "signatures are not accepted.");
    postrun_ = &detail::typed_postrun_wrapper<Hook>;
    return *this;
  }

  constexpr detail::StaticFuncDesc<NumParams> build() const {
    static_assert(NumParams <= kMaxParams,
                  "Too many parameters (max is kMaxParams)");
    static_assert(Mode != ParamMode::kUnset,
                  "vsql make_func: arity must be declared explicitly. Call "
                  ".param(TYPE) for each typed argument, .no_params() for "
                  "a zero-arity function, or .varargs() for a variadic "
                  "function, before .build()");

    using AllParams = typename detail::FuncParamTypes<decltype(Func)>::type;
    using ReturnType = typename detail::FuncReturnType<decltype(Func)>::type;
    static_assert(std::is_void_v<ReturnType>,
                  "make_func: C++ function must return void and set the SQL "
                  "result through the final typed result parameter");

    // Detect state-style signatures: first parameter is `void*` or any
    // lvalue reference (State& / const State&). Aggregate result-shape
    // functions also have a `const State&` first parameter but go through
    // AggFuncBuilder, not here. Varargs VDFs do not use state.
    constexpr bool has_state_param = []() constexpr {
      if constexpr (std::tuple_size_v<AllParams> >= 1) {
        using First = std::tuple_element_t<0, AllParams>;
        return std::is_same_v<First, void *> ||
               std::is_lvalue_reference_v<First>;
      } else {
        return false;
      }
    }();

    detail::FuncWithMetadata meta{};
    if constexpr (Mode == ParamMode::kVarargs) {
      meta.f = &detail::VarArgsWrapper<Func>::invoke;
      meta.is_varargs = true;
    } else if constexpr (has_state_param) {
      // Shapes:
      //   void(State&, TypedArgs..., TypedResult)     — typed state, mutable
      //   void(const State&, TypedArgs..., TypedResult) — typed state, const
      //   void(void*, TypedArgs..., TypedResult)      — raw state
      static_assert(HasPrerun,
                    "VDF declares state (State& or void*) but no prerun is "
                    "configured. user_data would be nullptr at every call. "
                    "Attach a .prerun<>() that calls set_user_data(), or "
                    "drop the state parameter.");
      static_assert(std::tuple_size_v<AllParams> == NumParams + 2,
                    "make_func: state-style VDF must have one State (or "
                    "void*) parameter, then the declared SQL parameters, "
                    "then a typed result");
      // TODO(villagesql-beta): also run args/result shape and runtime
      // signature checks (declared .param/.returns vs C++ wrappers) for
      // state-style VDFs. Today these run only for stateless shapes.
      using First = std::tuple_element_t<0, AllParams>;
      if constexpr (std::is_same_v<First, void *>) {
        meta.f = &detail::WrapperVoidStarState<Func, NumParams>::invoke;
      } else if constexpr (std::is_same_v<First, void *&>) {
        // `void*&` routes to WrapperVoidStarRefState so assignments inside the
        // VDF write back to args->user_data and survive across rows.
        meta.f = &detail::WrapperVoidStarRefState<Func, NumParams>::invoke;
      } else {
        // Both `State&` and `const State&` route to WrapperTypedState; the
        // implicit conversion from `State&` to `const State&` happens at
        // the call site if the user opted for read-only.
        using State = std::remove_const_t<std::remove_reference_t<First>>;
        meta.f = &detail::WrapperTypedState<Func, State, NumParams>::invoke;
      }
    } else {
      using Shape = detail::func_signature_shape<AllParams, NumParams>;
      static_assert(std::tuple_size_v<AllParams> == NumParams + 1,
                    "make_func: C++ function must have one typed result value "
                    "after the declared SQL parameters; check the number of "
                    ".param(TYPE) calls");
      static_assert(
          std::tuple_size_v<AllParams> != NumParams + 1 || Shape::args_ok,
          "make_func: every C++ function parameter before the result "
          "must be a typed argument such as IntArg, RealArg, StringArg, "
          "CustomArg, or CustomArgWith<P>");
      static_assert(
          std::tuple_size_v<AllParams> != NumParams + 1 || Shape::result_ok,
          "make_func: final C++ function parameter must be a typed "
          "result such as IntResult, RealResult, StringResult, "
          "CustomResult, or CustomResultWith<P>");
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
    }

    meta.prerun = prerun_;
    meta.postrun = postrun_;
    meta.return_type = detail::to_vef_type(return_type_);
    meta.num_params = NumParams;
    meta.buffer_size = buffer_size_;
    meta.max_result_length = max_result_length_;
    meta.deterministic = deterministic_;
    for (size_t i = 0; i < NumParams; ++i) {
      meta.param_types[i] = detail::to_vef_type(param_types_[i]);
    }

    return detail::StaticFuncDesc<NumParams>(name_, meta);
  }

 private:
  constexpr FuncBuilder()
      : name_(nullptr),
        return_type_(nullptr),
        param_types_{},
        buffer_size_(0),
        max_result_length_(0),
        prerun_(nullptr),
        postrun_(nullptr),
        deterministic_(false) {}

  const char *name_;
  const char *return_type_;
  std::array<const char *, NumParams> param_types_;
  size_t buffer_size_;
  size_t max_result_length_;
  vef_prerun_func_t prerun_;
  vef_postrun_func_t postrun_;
  bool deterministic_;

  template <auto F, size_t M, ParamMode N, bool HP>
  friend class FuncBuilder;

  template <auto F>
  friend constexpr FuncBuilder<F, 0, ParamMode::kUnset, false> make_func(
      const char *);
};

// =============================================================================
// AggFuncBuilder
// =============================================================================
//
// Builder for aggregate VDFs. Use make_aggregate_func<State, &result_fn>().
// The State type is explicit, and clear/accumulate signatures are validated
// against State at compile time.

template <typename State, auto Func, size_t NumParams, bool HasClear = false,
          bool HasAccumulate = false>
class AggFuncBuilder {
 public:
  constexpr AggFuncBuilder<State, Func, NumParams, HasClear, HasAccumulate> &
  returns(const char *t) {
    return_type_ = t;
    return *this;
  }

  constexpr AggFuncBuilder<State, Func, NumParams + 1, HasClear, HasAccumulate>
  param(const char *t) const {
    AggFuncBuilder<State, Func, NumParams + 1, HasClear, HasAccumulate> next;
    next.name_ = name_;
    next.return_type_ = return_type_;
    next.buffer_size_ = buffer_size_;
    next.max_result_length_ = max_result_length_;
    next.clear_ = clear_;
    next.accumulate_ = accumulate_;
    next.deterministic_ = deterministic_;
    for (size_t i = 0; i < NumParams; ++i) {
      next.param_types_[i] = param_types_[i];
    }
    next.param_types_[NumParams] = t;
    return next;
  }

  constexpr AggFuncBuilder<State, Func, NumParams, HasClear, HasAccumulate> &
  buffer_size(size_t s) {
    buffer_size_ = s;
    return *this;
  }

  // Declare the maximum length of the STRING result, in characters, so the
  // result column is sized to hold the full value when materialized. An
  // aggregate returning STRING that does not declare this falls back to the
  // argument width (a large result then truncates when materialized). Values
  // above VEF_MAX_RESULT_LENGTH are capped by the server. Bumps the required
  // protocol to VEF_PROTOCOL_4.
  //
  // Only valid for a STRING return type. Call .returns(STRING) before this so
  // the return type is known here: a non-STRING return (or an undeclared one)
  // is rejected at build time rather than silently ignored.
  constexpr AggFuncBuilder<State, Func, NumParams, HasClear, HasAccumulate> &
  max_result_length(size_t n) {
    if (return_type_ == nullptr) {
      detail::call_returns_before_max_result_length();
    } else if (detail::to_vef_type(return_type_).id != VEF_TYPE_STRING) {
      detail::max_result_length_is_only_valid_for_a_STRING_return_type();
    }
    max_result_length_ = n;
    return *this;
  }

  constexpr AggFuncBuilder<State, Func, NumParams, HasClear, HasAccumulate> &
  deterministic(bool d = true) {
    deterministic_ = d;
    return *this;
  }

  // void(State&) — first parameter must match the State type declared in
  // make_aggregate_func<State, ...>. Sets HasClear on the returned builder so
  // build() can static_assert that both clear and accumulate were configured.
  template <auto Fn>
  constexpr AggFuncBuilder<State, Func, NumParams, true, HasAccumulate> clear()
      const {
    using Params = typename detail::FuncParamTypes<decltype(Fn)>::type;
    using ReturnType = typename detail::FuncReturnType<decltype(Fn)>::type;
    using DeducedState =
        std::remove_reference_t<std::tuple_element_t<0, Params>>;
    static_assert(std::is_void_v<ReturnType>,
                  "clear: C++ function must return void");
    static_assert(std::is_same_v<DeducedState, State>,
                  "clear: first parameter must be State& matching the "
                  "make_aggregate_func State type");
    AggFuncBuilder<State, Func, NumParams, true, HasAccumulate> next;
    next.name_ = name_;
    next.return_type_ = return_type_;
    next.buffer_size_ = buffer_size_;
    next.max_result_length_ = max_result_length_;
    next.clear_ = &detail::agg_clear_wrapper<State, Fn>;
    next.accumulate_ = accumulate_;
    next.deterministic_ = deterministic_;
    for (size_t i = 0; i < NumParams; ++i) {
      next.param_types_[i] = param_types_[i];
    }
    return next;
  }

  // void(State&, TypedArgs...) — first parameter must match State; remaining
  // parameters must match the SQL param count declared via .param(TYPE)
  // calls. Call .accumulate() after all .param(TYPE) calls. Sets HasAccumulate
  // on the returned builder (see clear()).
  template <auto Fn>
  constexpr AggFuncBuilder<State, Func, NumParams, HasClear, true> accumulate()
      const {
    using Params = typename detail::FuncParamTypes<decltype(Fn)>::type;
    using ReturnType = typename detail::FuncReturnType<decltype(Fn)>::type;
    using DeducedState =
        std::remove_reference_t<std::tuple_element_t<0, Params>>;
    static_assert(std::is_void_v<ReturnType>,
                  "accumulate: C++ function must return void");
    static_assert(std::is_same_v<DeducedState, State>,
                  "accumulate: first parameter must be State& matching the "
                  "make_aggregate_func State type");
    static_assert(std::tuple_size_v<Params> == NumParams + 1,
                  "accumulate: typed arg count after State& must match the SQL "
                  "parameter count; ensure .accumulate() is called after "
                  ".param(TYPE)");
    static_assert(
        std::tuple_size_v<Params> != NumParams + 1 ||
            detail::accumulate_signature_shape<Params, NumParams>::args_ok,
        "accumulate: every C++ parameter after State& must be a typed argument "
        "such as IntArg, RealArg, StringArg, CustomArg, or "
        "CustomArgWith<P>");
    AggFuncBuilder<State, Func, NumParams, HasClear, true> next;
    next.name_ = name_;
    next.return_type_ = return_type_;
    next.buffer_size_ = buffer_size_;
    next.max_result_length_ = max_result_length_;
    next.clear_ = clear_;
    next.accumulate_ =
        &detail::AggAccumulateWrapper<State, Fn, NumParams>::invoke;
    next.deterministic_ = deterministic_;
    for (size_t i = 0; i < NumParams; ++i) {
      next.param_types_[i] = param_types_[i];
    }
    return next;
  }

  constexpr detail::StaticFuncDesc<NumParams> build() const {
    static_assert(NumParams <= kMaxParams,
                  "Too many parameters (max is kMaxParams)");
    static_assert(HasClear && HasAccumulate,
                  "aggregate must set both .clear<>() and .accumulate<>()");

    using AllParams = typename detail::FuncParamTypes<decltype(Func)>::type;
    using UniquePTuple = typename detail::unique_params_types<AllParams>::type;

    detail::FuncWithMetadata meta{};
    // void(const State&, TypedResult).
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
    meta.max_result_length = max_result_length_;
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
        max_result_length_(0),
        deterministic_(false),
        clear_(nullptr),
        accumulate_(nullptr) {}

  const char *name_;
  const char *return_type_;
  std::array<const char *, NumParams> param_types_;
  size_t buffer_size_;
  size_t max_result_length_;
  bool deterministic_;
  vef_vdf_clear_func_t clear_;
  vef_vdf_accumulate_func_t accumulate_;

  template <typename S, auto F, size_t M, bool C, bool A>
  friend class AggFuncBuilder;

  template <typename S, auto F>
  friend constexpr AggFuncBuilder<S, F, 0> make_aggregate_func(const char *);
};

// =============================================================================
// Entry Points
// =============================================================================

// make_aggregate_func<State, &result_fn>("name")
//
// Entry point for aggregate VDFs. Aggregate VDFs (like SQL SUM, COUNT, etc.)
// accumulate state across rows within each GROUP BY group, then return a
// final result per group. State is the per-group accumulation type; it is
// explicit, and all three callback signatures are validated against it at
// compile time.
//
// The result function always uses an output parameter, consistent with normal
// VDFs. All three callbacks follow the same pattern:
//
//   void my_clear(State &s)                     // reset state
//   void my_acc(State &s, TypedArg v, ...)      // accumulate one row
//   void my_result(const State &s, TypedResult out)  // produce final value
//
// The result function must be void(const State&, TypedResult) where
// TypedResult is one of IntResult, RealResult, StringResult, CustomResult, or
// CustomResultWith<P>. Use out.set_null() to return SQL NULL.
//
// How it works:
//   - prerun/postrun are auto-generated: prerun allocates State via
//     value-initialization, postrun deletes it.
//   - .clear<&fn>()      void(State&)
//   - .accumulate<&fn>() void(State&, TypedArgs...) — TypedArgs deduced from
//     the function signature (IntArg, StringArg, CustomArg, etc.).
//     Call .accumulate() after all .param(TYPE) calls.
//
// Example — nullable integer sum:
//
//   using SumState = std::optional<long long>;
//
//   void my_clear(SumState &s) { s = std::nullopt; }
//   void my_acc(SumState &s, IntArg v) {
//     if (!v.is_null()) s = s.value_or(0) + v.value();
//   }
//   void my_result(const SumState &s, IntResult out) {
//     if (!s.has_value()) { out.set_null(); return; }
//     out.set(s.value());
//   }
//
//   make_aggregate_func<SumState, &my_result>("my_sum")
//       .returns(INT)
//       .param(INT)
//       .clear<&my_clear>()
//       .accumulate<&my_acc>()
//       .build()
//
// Example — non-nullable count:
//
//   using CountState = long long;
//   void count_clear(CountState &s) { s = 0; }
//   void count_acc(CountState &s, IntArg v) { if (!v.is_null()) ++s; }
//   void count_result(const CountState &s, IntResult out) { out.set(s); }
//
//   make_aggregate_func<CountState, &count_result>("my_count")
//       .returns(INT).param(INT)
//       .clear<&count_clear>().accumulate<&count_acc>()
//       .build()
//
// Example — aggregate returning a custom (binary) type:
//
//   using SumState = std::optional<MyType>;
//   void my_clear(SumState &s)            { s = std::nullopt; }
//   void my_acc(SumState &s, CustomArg v) { /* update s */ }
//   void my_result(const SumState &s, CustomResult out) {
//     if (!s.has_value()) { out.set_null(); return; }
//     store_mytype(out.buffer().data(), s.value());
//     out.set_length(kMyTypeSize);
//   }
//
//   make_aggregate_func<SumState, &my_result>("my_agg")
//       .returns(MYTYPE).param(MYTYPE)
//       .clear<&my_clear>().accumulate<&my_acc>()
//       .build()
//
// See aggregate_vdf.cc in the test suite for complete examples.
template <typename State, auto Func>
constexpr AggFuncBuilder<State, Func, 0> make_aggregate_func(const char *name) {
  using AllParams = typename detail::FuncParamTypes<decltype(Func)>::type;
  using ReturnType = typename detail::FuncReturnType<decltype(Func)>::type;
  static_assert(
      std::is_void_v<ReturnType> &&
          detail::is_agg_result_for_state<State, AllParams>::value,
      "make_aggregate_func: result function must be "
      "void(const State&, TypedResult) where TypedResult is IntResult, "
      "RealResult, StringResult, CustomResult, or CustomResultWith<P>");
  AggFuncBuilder<State, Func, 0> builder;
  builder.name_ = name;
  return builder;
}

// make_func<&impl>("name")
template <auto Func>
constexpr FuncBuilder<Func, 0, ParamMode::kUnset, false> make_func(
    const char *name) {
  using AllParams = typename detail::FuncParamTypes<decltype(Func)>::type;
  static_assert(
      std::tuple_size_v<AllParams> == 0 ||
          !detail::is_context_param<std::tuple_element_t<0, AllParams>>::value,
      "vsql make_func: deprecated vef_context_t* first parameter not "
      "supported; use a typed function or make_aggregate_func (see "
      "extension.h)");
  FuncBuilder<Func, 0, ParamMode::kUnset, false> builder;
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

// make_type_real_value<&fn>("name", TYPE) — (CUSTOM(type)) -> REAL.
//
// Accepts two signatures:
//   TypeRealValueFunc              (non-parameterized)
//   TypeRealValueWithParamsFunc<P> (parameterized)
template <auto Func>
constexpr detail::StaticFuncDesc<1> make_type_real_value(
    const char *name, const char *type_name) {
  using F = decltype(Func);
  detail::FuncWithMetadata meta{};
  if constexpr (std::is_same_v<F, TypeRealValueFunc>) {
    meta.f = &detail::TypeRealValueVdfWrapper<Func>::invoke;
  } else {
    using P = typename detail::TypeRealValueWithCacheVdfWrapper<Func>::P;
    static_assert(std::is_same_v<F, TypeRealValueWithParamsFunc<P>>,
                  "make_type_real_value: function must match either "
                  "TypeRealValueFunc or "
                  "TypeRealValueWithParamsFunc<P>.");
    meta.f = &detail::TypeRealValueWithCacheVdfWrapper<Func>::invoke;
    meta.check_params_cache_bound = &is_params_cache_bound<P>;
  }
  meta.return_type = detail::to_vef_type(REAL);
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
template <auto Func>
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
