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

#ifndef VILLAGESQL_VSQL_TYPE_BUILDER_H
#define VILLAGESQL_VSQL_TYPE_BUILDER_H

// Object-based type builder for the ::vsql API.
//
// Usage (non-parameterized type):
//
//   // One declaration per type — serves as the Non-Type Template Parameter
//   // (NTTP) for auto-naming.
//   static constexpr const char kMyTypeName[] = "MYTYPE";
//
//   constexpr auto MYTYPE = vsql::make_type<kMyTypeName>()
//       .persisted_length(16)
//       .max_decode_buffer_length(64)
//       .from_string<&my_from_string>()
//       .to_string<&my_to_string>()
//       .compare<&my_compare>()
//       .hash<&my_hash>()    // optional
//       .build();
//
// Usage (parameterized type, e.g. TVECTOR(N)):
//
//   struct TVectorParams {
//     int64_t dimension;
//     static TVectorParams parse(const std::map<std::string,std::string>&);
//     static void to_strings(const TVectorParams&,
//                            std::map<std::string,std::string>&);
//   };
//   static constexpr const char kTVectorTypeName[] = "TVECTOR";
//
//   constexpr auto TVECTOR = vsql::make_type<kTVectorTypeName>()
//       .max_persisted_length(/* upper bound across all valid params */)
//       .params<TVectorParams, &TVectorParams::parse,
//               &TVectorParams::to_strings>()
//       .int_to_params<&my_int_to_params_fn>()
//       .resolve_params<&my_resolve_params_fn>()
//       .from_string<&my_from_string_fn>()
//       .to_string<&my_to_string_fn>()
//       .compare<&my_compare_fn>()
//       .build();
//
// VDF names are auto-generated from the type name at compile time:
//   .from_string<&fn>()      ->  VDF name "MYTYPE::from_string"  (SQL-callable)
//   .to_string<&fn>()        ->  VDF name "MYTYPE::to_string"    (SQL-callable)
//   .compare<&fn>()          ->  VDF name "MYTYPE::compare"      (SQL-callable)
//   .hash<&fn>()             ->  VDF name "MYTYPE::hash"         (SQL-callable)
//   .int_to_params<&fn>()    ->  VDF name "MYTYPE::int_to_params"
//   .resolve_params<&fn>()   ->  VDF name "MYTYPE::resolve_params"
//
// Two types sharing the same function pointer still get independent VDF name
// buffers because the kMyTypeName pointer value differs per type.
//
// The resulting TypeObject converts implicitly to const char* so it can be
// passed directly to .returns() and .param() on FuncBuilder:
//
//   make_func<&add_impl>("add")
//       .returns(MYTYPE).param(MYTYPE).param(MYTYPE).build()
//
// Pass TypeObject to ExtensionBuilder::type() to register the type and any
// embedded SQL-callable VDFs in one step:
//
//   make_extension()
//       .type(MYTYPE)   // registers type + all embedded VDFs
//       .func(...)
//
// intrinsic_default VDFs must be registered separately with
// make_intrinsic_default(). Reference them by name with
// .intrinsic_default_vdf("vdf_name") on the TypeBuilder.

#include <cstdint>
#include <tuple>
#include <type_traits>

#include <villagesql/detail/type_builder.h>
#include <villagesql/vsql/func_builder.h>

namespace vsql {

// =============================================================================
// TypeObject — the built type, usable as a type reference
// =============================================================================
//
// EmbeddedFuncsTuple holds SQL-callable VDFs (from_string/to_string/compare/
// hash/int_to_params/resolve_params) that are automatically registered
// alongside the type when passed to ExtensionBuilder::type(). These are
// produced by the template methods on TypeBuilder.

template <typename EmbeddedFuncsTuple = std::tuple<>>
struct TypeObject {
  villagesql::type_builder::TypeDescriptor descriptor;
  EmbeddedFuncsTuple embedded_funcs;
  // Init fns bound to TypeParamsCache<P> at extension registration. Set by
  // .params<P, &Parse, &ToStrings>(); read by the registration loop in
  // detail/vef_register.h.
  void (*params_init_fn)() = nullptr;
  void (*params_to_strings_init_fn)() = nullptr;

  // Converts to the SQL type name string for use in .returns() and .param().
  constexpr operator const char *() const { return descriptor.vef_desc.name; }

  constexpr const char *name() const { return descriptor.vef_desc.name; }
};

// =============================================================================
// TypeBuilder
// =============================================================================

template <bool HasFromString = false, bool HasToString = false,
          bool HasCompare = false, typename ParamsType = void,
          bool HasIntToParams = false, bool HasResolveParams = false,
          bool HasMaxPersistedLength = false, typename EFT = std::tuple<>,
          const char *Name = nullptr>
class TypeBuilder {
 public:
  constexpr TypeBuilder &persisted_length(int64_t len) {
    state_.desc.vef_desc.persisted_length = len;
    return *this;
  }

  constexpr TypeBuilder &max_decode_buffer_length(int64_t len) {
    state_.desc.vef_desc.max_decode_buffer_length = len;
    return *this;
  }

  // Upper bound on persisted_length across all valid parameterizations.
  // Required for parameterized types (parse + to_strings) — build()
  // static_asserts the pairing. Ignored for non-parameterized types
  // (persisted_length suffices there).
  //
  // Used only on the fix_fields-time constant-string inference path, where
  // the server has not yet inferred the params and so cannot consult
  // resolve_params to size the encode buffer. The server allocates this many
  // bytes, runs from_string with MaybeParams<P> unknown, then trims to
  // actual_len. After inference, normal row-time calls continue to use the
  // params-resolved persisted_length as today.
  constexpr auto max_persisted_length(int64_t len) const {
    detail::TypeBuilderState s = state_;
    s.desc.vef_desc.max_persisted_length = len;
    return TypeBuilder<HasFromString, HasToString, HasCompare, ParamsType,
                       HasIntToParams, HasResolveParams,
                       /*HasMaxPersistedLength=*/true, EFT, Name>{
        s, embedded_funcs_};
  }

  // -------------------------------------------------------------------------
  // Parameterized type support
  // -------------------------------------------------------------------------

  // Bind the params parse function and its inverse params_to_strings function
  // to TypeParamsCache<P>. Must be called before int_to_params() or
  // resolve_params().
  //
  // P is the params struct type. Required signatures:
  //   ParseFunc:            P  fn(const std::map<std::string,std::string>&)
  //   ParamsToStringsFunc:  void fn(const P&,
  //                                 std::map<std::string,std::string>&)
  //
  // ParamsToStringsFunc is the inverse of ParseFunc and is needed by paths
  // that produce a typed P at runtime (e.g., constant-string from_string
  // pre-execute at fix_fields time) and need to publish the equivalent
  // string-form params back to the server.
  template <typename P, auto ParseFunc, auto ParamsToStringsFunc>
  constexpr auto params() const {
    static_assert(!HasFromString && !HasToString && !HasCompare,
                  "vsql::TypeBuilder: .params<P>() must be called before "
                  ".from_string(), .to_string(), and .compare()");
    static_assert(
        std::is_same_v<decltype(ParamsToStringsFunc),
                       func_builder::ParamsToStringsFunc<P>>,
        "params<P, &Parse, &ToStrings>(): third argument must have signature "
        "void fn(const P&, std::map<std::string,std::string>&)");
    detail::TypeBuilderState s = state_;
    s.params_init_fn = &detail::bind_params_cache<P, ParseFunc>;
    s.params_to_strings_init_fn =
        &detail::bind_params_to_strings_cache<P, ParamsToStringsFunc>;
    s.desc.vef_desc.protocol = VEF_PROTOCOL_2;
    return TypeBuilder<HasFromString, HasToString, HasCompare, P,
                       HasIntToParams, HasResolveParams, HasMaxPersistedLength,
                       EFT, Name>{s, embedded_funcs_};
  }

  // int_to_params: VDF that converts MYTYPE(N) integer to a params string.
  // Auto-generates VDF name "TYPENAME::int_to_params".
  // Function signature: bool fn(int64_t, std::map<std::string,std::string>&,
  //                             char* error_msg)
  template <auto Func>
  constexpr auto int_to_params() const {
    using namespace detail;
    static_assert(
        std::is_same_v<decltype(Func), func_builder::IntToTypeParamsFunc>,
        "int_to_params<Func>() requires: "
        "bool fn(int64_t, std::map<std::string,std::string>&, char*)");
    constexpr const char *vdf_name =
        kTypeOpVdfName<Name, TypeOp::kIntToParams>.buf;
    auto inner = func_builder::make_int_to_params<Func>(vdf_name);
    TypeBuilderState s = state_;
    s.desc.vef_desc.int_to_params_vdf_name = vdf_name;
    s.desc.vef_desc.protocol = VEF_PROTOCOL_2;
    auto new_embedded = std::tuple_cat(embedded_funcs_, std::make_tuple(inner));
    return TypeBuilder<HasFromString, HasToString, HasCompare, ParamsType, true,
                       HasResolveParams, HasMaxPersistedLength,
                       decltype(new_embedded), Name>{s, new_embedded};
  }

  // resolve_params: VDF that validates params and computes storage sizes.
  // Auto-generates VDF name "TYPENAME::resolve_params".
  // Function signature:
  //   bool fn(const std::map<std::string,std::string>&,
  //           vsql::ResolvedTypeParams*, char* error_msg)
  template <auto Func>
  constexpr auto resolve_params() const {
    using namespace detail;
    static_assert(
        std::is_same_v<decltype(Func), func_builder::ResolveTypeParamsFunc>,
        "resolve_params<Func>() requires: "
        "bool fn(const std::map<std::string,std::string>&, "
        "vsql::ResolvedTypeParams*, char*)");
    constexpr const char *vdf_name =
        kTypeOpVdfName<Name, TypeOp::kResolveParams>.buf;
    auto inner = func_builder::make_resolve_params<Func>(vdf_name);
    TypeBuilderState s = state_;
    s.desc.vef_desc.resolve_params_vdf_name = vdf_name;
    s.desc.vef_desc.protocol = VEF_PROTOCOL_2;
    auto new_embedded = std::tuple_cat(embedded_funcs_, std::make_tuple(inner));
    return TypeBuilder<HasFromString, HasToString, HasCompare, ParamsType,
                       HasIntToParams, true, HasMaxPersistedLength,
                       decltype(new_embedded), Name>{s, new_embedded};
  }

  // -------------------------------------------------------------------------
  // Auto-named template methods (SQL-callable, v2 ABI)
  //
  // VDF name is auto-generated as "TYPENAME::method" at compile time.
  // The named VDF is embedded in the TypeObject and registered automatically
  // by ExtensionBuilder::type().
  // -------------------------------------------------------------------------

  template <auto Func>
  constexpr auto from_string() const {
    using namespace detail;
    using OpP =
        typename func_builder::detail::TypeOpParamsType<decltype(Func)>::type;
    static_assert(
        std::is_same_v<OpP, ParamsType>,
        "vsql::TypeBuilder::from_string(): function params type is "
        "inconsistent with the declared params type — if the type uses "
        ".params<P>(), from_string must use TypeEncodeWithParamsFunc<P>; "
        "otherwise use TypeEncodeFunc");
    constexpr const char *vdf_name = kTypeOpVdfName<Name, TypeOp::kEncode>.buf;
    auto inner = func_builder::make_type_encode<Func>(
        vdf_name, state_.desc.vef_desc.name);
    TypeBuilderState s = state_;
    s.desc.vef_desc.encode_vdf_name = vdf_name;
    s.desc.vef_desc.protocol = VEF_PROTOCOL_2;
    auto new_embedded = std::tuple_cat(embedded_funcs_, std::make_tuple(inner));
    return TypeBuilder<true, HasToString, HasCompare, ParamsType,
                       HasIntToParams, HasResolveParams, HasMaxPersistedLength,
                       decltype(new_embedded), Name>{s, new_embedded};
  }

  template <auto Func>
  constexpr auto to_string() const {
    using namespace detail;
    using OpP =
        typename func_builder::detail::TypeOpParamsType<decltype(Func)>::type;
    static_assert(
        std::is_same_v<OpP, ParamsType>,
        "vsql::TypeBuilder::to_string(): function params type is inconsistent "
        "with the declared params type — if the type uses .params<P>(), "
        "to_string must use TypeDecodeWithParamsFunc<P>; otherwise use "
        "TypeDecodeFunc");
    constexpr const char *vdf_name = kTypeOpVdfName<Name, TypeOp::kDecode>.buf;
    auto inner = func_builder::make_type_decode<Func>(
        vdf_name, state_.desc.vef_desc.name);
    TypeBuilderState s = state_;
    s.desc.vef_desc.decode_vdf_name = vdf_name;
    s.desc.vef_desc.protocol = VEF_PROTOCOL_2;
    auto new_embedded = std::tuple_cat(embedded_funcs_, std::make_tuple(inner));
    return TypeBuilder<HasFromString, true, HasCompare, ParamsType,
                       HasIntToParams, HasResolveParams, HasMaxPersistedLength,
                       decltype(new_embedded), Name>{s, new_embedded};
  }

  template <auto Func>
  constexpr auto compare() const {
    using namespace detail;
    using OpP =
        typename func_builder::detail::TypeOpParamsType<decltype(Func)>::type;
    static_assert(
        std::is_same_v<OpP, ParamsType>,
        "vsql::TypeBuilder::compare(): function params type is inconsistent "
        "with the declared params type — if the type uses .params<P>(), "
        "compare must use TypeCompareWithParamsFunc<P>; otherwise use "
        "TypeCompareFunc");
    constexpr const char *vdf_name = kTypeOpVdfName<Name, TypeOp::kCompare>.buf;
    auto inner = func_builder::make_type_compare<Func>(
        vdf_name, state_.desc.vef_desc.name);
    TypeBuilderState s = state_;
    s.desc.vef_desc.compare_vdf_name = vdf_name;
    s.desc.vef_desc.protocol = VEF_PROTOCOL_2;
    auto new_embedded = std::tuple_cat(embedded_funcs_, std::make_tuple(inner));
    return TypeBuilder<HasFromString, HasToString, true, ParamsType,
                       HasIntToParams, HasResolveParams, HasMaxPersistedLength,
                       decltype(new_embedded), Name>{s, new_embedded};
  }

  template <auto Func>
  constexpr auto hash() const {
    using namespace detail;
    using OpP =
        typename func_builder::detail::TypeOpParamsType<decltype(Func)>::type;
    static_assert(
        std::is_same_v<OpP, ParamsType>,
        "vsql::TypeBuilder::hash(): function params type is inconsistent with "
        "the declared params type — if the type uses .params<P>(), hash must "
        "use TypeHashWithParamsFunc<P>; otherwise use TypeHashFunc");
    constexpr const char *vdf_name = kTypeOpVdfName<Name, TypeOp::kHash>.buf;
    auto inner =
        func_builder::make_type_hash<Func>(vdf_name, state_.desc.vef_desc.name);
    TypeBuilderState s = state_;
    s.desc.vef_desc.hash_vdf_name = vdf_name;
    s.desc.vef_desc.protocol = VEF_PROTOCOL_2;
    auto new_embedded = std::tuple_cat(embedded_funcs_, std::make_tuple(inner));
    return TypeBuilder<HasFromString, HasToString, HasCompare, ParamsType,
                       HasIntToParams, HasResolveParams, HasMaxPersistedLength,
                       decltype(new_embedded), Name>{s, new_embedded};
  }

  // intrinsic_default_str: literal string representation of the default value
  // for NOT NULL columns receiving NULL with IGNORE (e.g., "0" for an integer
  // type). The server encodes it via the type's from_string VDF at first use of
  // the type. Use intrinsic_default_vdf() instead when the default must be
  // computed.
  constexpr TypeBuilder &intrinsic_default_str(const char *str) {
    state_.desc.vef_desc.intrinsic_default_str = str;
    state_.desc.vef_desc.protocol = VEF_PROTOCOL_2;
    return *this;
  }

  // intrinsic_default_vdf: name of a separately-registered VDF that writes the
  // default binary value for a NOT NULL column receiving NULL with IGNORE.
  // The named VDF must be registered separately with make_intrinsic_default().
  //
  // TODO(villagesql-beta): embed intrinsic_default inline once the vsql API
  // supports auto-generating its VDF name too.
  constexpr TypeBuilder &intrinsic_default_vdf(const char *vdf_name) {
    state_.desc.vef_desc.intrinsic_default_vdf_name = vdf_name;
    state_.desc.vef_desc.protocol = VEF_PROTOCOL_2;
    return *this;
  }

  // -------------------------------------------------------------------------
  // build() — finalize to TypeObject<EFT>
  // -------------------------------------------------------------------------

  constexpr TypeObject<EFT> build() const {
    static_assert(
        HasFromString,
        "vsql::TypeBuilder: from_string() is required before build()");
    static_assert(HasToString,
                  "vsql::TypeBuilder: to_string() is required before build()");
    static_assert(HasCompare,
                  "vsql::TypeBuilder: compare() is required before build()");
    static_assert(!HasIntToParams || !std::is_void_v<ParamsType>,
                  "vsql::TypeBuilder: params<P, &parse_fn>() is required when "
                  "int_to_params() is used");
    static_assert(!HasResolveParams || !std::is_void_v<ParamsType>,
                  "vsql::TypeBuilder: params<P, &parse_fn>() is required when "
                  "resolve_params() is used");
    static_assert(
        std::is_void_v<ParamsType> || HasMaxPersistedLength,
        "vsql::TypeBuilder: parameterized types must call "
        ".max_persisted_length(N) before build() — required so constant-string "
        "type parameter inference can size its encode buffer. Pass the upper "
        "bound of persisted_length across all valid parameterizations.");
    return TypeObject<EFT>{state_.desc, embedded_funcs_, state_.params_init_fn,
                           state_.params_to_strings_init_fn};
  }

  // Cross-specialization and make_type access.
  template <bool, bool, bool, typename, bool, bool, bool, typename,
            const char *>
  friend class TypeBuilder;

  template <const char *N>
  friend constexpr TypeBuilder<false, false, false, void, false, false, false,
                               std::tuple<>, N>
  make_type();

 private:
  detail::TypeBuilderState state_;
  EFT embedded_funcs_;

  constexpr explicit TypeBuilder(const detail::TypeBuilderState &s,
                                 EFT ef = EFT{})
      : state_(s), embedded_funcs_(ef) {}
};

// =============================================================================
// make_type<Name>() — entry point
// =============================================================================
//
// Name must be a const char* — a pointer to a static constexpr char array
// declared with external or internal linkage, e.g.:
//
//   static constexpr const char kMyTypeName[] = "MYTYPE";
//   constexpr auto MYTYPE = vsql::make_type<kMyTypeName>()...build();

template <const char *Name>
constexpr TypeBuilder<false, false, false, void, false, false, false,
                      std::tuple<>, Name>
make_type() {
  detail::TypeBuilderState s{};
  s.desc.vef_desc.name = Name;
  s.desc.vef_desc.protocol = VEF_PROTOCOL_1;
  return TypeBuilder<false, false, false, void, false, false, false,
                     std::tuple<>, Name>{s};
}

}  // namespace vsql

#endif  // VILLAGESQL_VSQL_TYPE_BUILDER_H
