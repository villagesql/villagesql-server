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
//   };
//   static constexpr const char kTVectorTypeName[] = "TVECTOR";
//
//   constexpr auto TVECTOR = vsql::make_type<kTVectorTypeName>()
//       .params<TVectorParams, &TVectorParams::parse>()
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
// buffers because the (the kMyTypeName pointer value) differs per type.
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

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <tuple>
#include <type_traits>

#include <villagesql/abi/types.h>
#include <villagesql/type_builder.h>
#include <villagesql/vsql/func_builder.h>
#include <villagesql/vsql/type_params_cache.h>

namespace vsql {

namespace detail {

// Binds the parse function for parameterized type caches. Stored as a function
// pointer in TypeDescriptor.params_init_fn and called once during registration.
template <typename P, auto ParseFunc>
void bind_params_cache() {
  villagesql::type_params_cache_for<P>().bind(ParseFunc);
}

// =============================================================================
// TypeOpVdfName and TypeOp — internal helpers for auto-named VDF strings
// =============================================================================

enum class TypeOp {
  kEncode = 0,
  kDecode = 1,
  kCompare = 2,
  kHash = 3,
  kIntToParams = 4,
  kResolveParams = 5,
};

// TypeOpVdfName: constexpr-initialized string "TypeName::suffix" for a given
// (TypeName, Op) pair.
//
// The buf[] array is filled by the constexpr constructor, so the address
// kTypeOpVdfName<TypeName,Op>.buf is an address constant expression — usable as
// a constexpr const char* pointer in vef_type_desc_t fields.
//
// The TypeName Template Parameter must point to a static constexpr char array
// (e.g., `static constexpr const char kFoo[] = "FOO"`), so TypeName[j] is
// readable in a constant-expression context.
template <const char *TypeName, TypeOp Op>
struct TypeOpVdfName {
  static constexpr std::string_view op_name(TypeOp op) {
    switch (op) {
      case TypeOp::kEncode:
        return "from_string";
      case TypeOp::kDecode:
        return "to_string";
      case TypeOp::kCompare:
        return "compare";
      case TypeOp::kHash:
        return "hash";
      case TypeOp::kIntToParams:
        return "int_to_params";
      case TypeOp::kResolveParams:
        return "resolve_params";
    }
  }
  static constexpr std::string_view kTypeName = std::string_view{TypeName};
  static constexpr std::string_view kSep = "::";
  static constexpr std::string_view kOpName = op_name(Op);
  static constexpr size_t kSize =
      kTypeName.size() + kSep.size() + kOpName.size() + 1;
  char buf[kSize];
  constexpr TypeOpVdfName() : buf{} {
    size_t i = 0;
    for (char c : kTypeName) buf[i++] = c;
    for (char c : kSep) buf[i++] = c;
    for (char c : kOpName) buf[i++] = c;
  }
};

// One constexpr instance per (TypeName, Op) pair — inline ensures a single
// definition across translation units (C++17 inline variable).
template <const char *TypeName, TypeOp Op>
inline constexpr TypeOpVdfName<TypeName, Op> kTypeOpVdfName{};

// Shared builder state passed by value between TypeBuilder specializations.
struct TypeBuilderState {
  villagesql::type_builder::TypeDescriptor desc;
};

}  // namespace detail

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

  // Converts to the SQL type name string for use in .returns() and .param().
  constexpr operator const char *() const { return descriptor.vef_desc.name; }

  constexpr const char *name() const { return descriptor.vef_desc.name; }
};

// =============================================================================
// TypeBuilder
// =============================================================================
//
// Template parameters:
//   HasFromString / HasToString / HasCompare — set by the corresponding
//     builder methods; build() static_asserts all three are true.
//   HasParams — set by .params<P, &ParseFn>(); build() requires this when
//     HasIntToParams or HasResolveParams is true.
//   HasIntToParams / HasResolveParams — set by the corresponding template
//     methods.
//   EFT — accumulates embedded SQL-callable VDFs (StaticFuncDesc values)
//     produced by the template methods.
//   Name — const char* NTTP from make_type<kName>(); drives selection of the
//     per-(Name,Op) constexpr VDF name buffers via kTypeOpVdfName<Name,Op>.
//
//     NB - there is no HasHash because it is optional.

template <bool HasFromString = false, bool HasToString = false,
          bool HasCompare = false, bool HasParams = false,
          bool HasIntToParams = false, bool HasResolveParams = false,
          typename EFT = std::tuple<>, const char *Name = nullptr>
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

  // -------------------------------------------------------------------------
  // Parameterized type support
  // -------------------------------------------------------------------------

  // Bind the params parse function to TypeParamsCache<P>.
  // Must be called before int_to_params() or resolve_params().
  // P is the params struct type; ParseFunc is a function:
  //   P fn(const std::map<std::string, std::string>&)
  template <typename P, auto ParseFunc>
  constexpr auto params() const {
    detail::TypeBuilderState s = state_;
    s.desc.params_init_fn = &detail::bind_params_cache<P, ParseFunc>;
    s.desc.vef_desc.protocol = VEF_PROTOCOL_2;
    return TypeBuilder<HasFromString, HasToString, HasCompare, true,
                       HasIntToParams, HasResolveParams, EFT, Name>{
        s, embedded_funcs_};
  }

  // int_to_params: VDF that converts MYTYPE(N) integer to a params string.
  // Auto-generates VDF name "TYPENAME::int_to_params".
  // Function signature: bool fn(int64_t, std::map<std::string,std::string>&,
  //                             char* error_msg)
  template <auto Func>
  constexpr auto int_to_params() const {
    using namespace detail;
    static_assert(
        std::is_same_v<decltype(Func),
                       villagesql::func_builder::IntToTypeParamsFunc>,
        "int_to_params<Func>() requires: "
        "bool fn(int64_t, std::map<std::string,std::string>&, char*)");
    constexpr const char *vdf_name =
        kTypeOpVdfName<Name, TypeOp::kIntToParams>.buf;
    auto inner = villagesql::func_builder::make_int_to_params<Func>(vdf_name);
    TypeBuilderState s = state_;
    s.desc.vef_desc.int_to_params_vdf_name = vdf_name;
    s.desc.vef_desc.protocol = VEF_PROTOCOL_2;
    auto new_embedded = std::tuple_cat(embedded_funcs_, std::make_tuple(inner));
    return TypeBuilder<HasFromString, HasToString, HasCompare, HasParams, true,
                       HasResolveParams, decltype(new_embedded), Name>{
        s, new_embedded};
  }

  // resolve_params: VDF that validates params and computes storage sizes.
  // Auto-generates VDF name "TYPENAME::resolve_params".
  // Function signature:
  //   bool fn(const std::map<std::string,std::string>&,
  //           villagesql::ResolvedTypeParams*, char* error_msg)
  template <auto Func>
  constexpr auto resolve_params() const {
    using namespace detail;
    static_assert(
        std::is_same_v<decltype(Func),
                       villagesql::func_builder::ResolveTypeParamsFunc>,
        "resolve_params<Func>() requires: "
        "bool fn(const std::map<std::string,std::string>&, "
        "villagesql::ResolvedTypeParams*, char*)");
    constexpr const char *vdf_name =
        kTypeOpVdfName<Name, TypeOp::kResolveParams>.buf;
    auto inner = villagesql::func_builder::make_resolve_params<Func>(vdf_name);
    TypeBuilderState s = state_;
    s.desc.vef_desc.resolve_params_vdf_name = vdf_name;
    s.desc.vef_desc.protocol = VEF_PROTOCOL_2;
    auto new_embedded = std::tuple_cat(embedded_funcs_, std::make_tuple(inner));
    return TypeBuilder<HasFromString, HasToString, HasCompare, HasParams,
                       HasIntToParams, true, decltype(new_embedded), Name>{
        s, new_embedded};
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
    // Accepts TypeEncodeFunc or TypeEncodeWithParamsFunc<P> (const P& as first
    // arg). Signature validation is handled by make_type_encode<Func>.
    constexpr const char *vdf_name = kTypeOpVdfName<Name, TypeOp::kEncode>.buf;
    auto inner = villagesql::func_builder::make_type_encode<Func>(
        vdf_name, state_.desc.vef_desc.name);
    TypeBuilderState s = state_;
    s.desc.vef_desc.encode_vdf_name = vdf_name;
    s.desc.vef_desc.protocol = VEF_PROTOCOL_2;
    auto new_embedded = std::tuple_cat(embedded_funcs_, std::make_tuple(inner));
    return TypeBuilder<true, HasToString, HasCompare, HasParams, HasIntToParams,
                       HasResolveParams, decltype(new_embedded), Name>{
        s, new_embedded};
  }

  template <auto Func>
  constexpr auto to_string() const {
    using namespace detail;
    // Accepts TypeDecodeFunc or TypeDecodeWithParamsFunc<P>.
    // Signature validation is handled by make_type_decode<Func>.
    constexpr const char *vdf_name = kTypeOpVdfName<Name, TypeOp::kDecode>.buf;
    auto inner = villagesql::func_builder::make_type_decode<Func>(
        vdf_name, state_.desc.vef_desc.name);
    TypeBuilderState s = state_;
    s.desc.vef_desc.decode_vdf_name = vdf_name;
    s.desc.vef_desc.protocol = VEF_PROTOCOL_2;
    auto new_embedded = std::tuple_cat(embedded_funcs_, std::make_tuple(inner));
    return TypeBuilder<HasFromString, true, HasCompare, HasParams,
                       HasIntToParams, HasResolveParams, decltype(new_embedded),
                       Name>{s, new_embedded};
  }

  template <auto Func>
  constexpr auto compare() const {
    using namespace detail;
    // Accepts TypeCompareFunc or TypeCompareWithParamsFunc<P>.
    // Signature validation is handled by make_type_compare<Func>.
    constexpr const char *vdf_name = kTypeOpVdfName<Name, TypeOp::kCompare>.buf;
    auto inner = villagesql::func_builder::make_type_compare<Func>(
        vdf_name, state_.desc.vef_desc.name);
    TypeBuilderState s = state_;
    s.desc.vef_desc.compare_vdf_name = vdf_name;
    s.desc.vef_desc.protocol = VEF_PROTOCOL_2;
    auto new_embedded = std::tuple_cat(embedded_funcs_, std::make_tuple(inner));
    return TypeBuilder<HasFromString, HasToString, true, HasParams,
                       HasIntToParams, HasResolveParams, decltype(new_embedded),
                       Name>{s, new_embedded};
  }

  template <auto Func>
  constexpr auto hash() const {
    using namespace detail;
    // Accepts TypeHashFunc or TypeHashWithParamsFunc<P>.
    // Signature validation is handled by make_type_hash<Func>.
    constexpr const char *vdf_name = kTypeOpVdfName<Name, TypeOp::kHash>.buf;
    auto inner = villagesql::func_builder::make_type_hash<Func>(
        vdf_name, state_.desc.vef_desc.name);
    TypeBuilderState s = state_;
    s.desc.vef_desc.hash_vdf_name = vdf_name;
    s.desc.vef_desc.protocol = VEF_PROTOCOL_2;
    auto new_embedded = std::tuple_cat(embedded_funcs_, std::make_tuple(inner));
    return TypeBuilder<HasFromString, HasToString, HasCompare, HasParams,
                       HasIntToParams, HasResolveParams, decltype(new_embedded),
                       Name>{s, new_embedded};
  }

  // intrinsic_default_str:
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

  // Constexpr-safe overload for use with a storage interface that has a fixed
  // address (i.e., declared static constexpr at namespace scope). The address
  // is supplied as a template argument so it is known at compile time, allowing
  // vef_desc.storage_intf to be set directly without embedding a copy of the
  // interface inside the TypeDescriptor. This keeps storage_intf.version == 0,
  // so TypeDescriptor's copy constructor skips the self-referential assignment
  // `vef_desc.storage_intf = &storage_intf` that GCC rejects in constexpr
  // context.
  template <const vef_type_storage_intf_t *Intf>
  constexpr TypeBuilder &column_storage() {
    state_.desc.vef_desc.storage_intf = Intf;
    return *this;
  }

  // -------------------------------------------------------------------------
  // build() — finalize to TypeObject<EFT>
  // -------------------------------------------------------------------------

  constexpr TypeObject<EFT> build() const {
    // TODO(villagesql-beta): Static check that the operation take type params
    // if they are registered with param functions.
    static_assert(
        HasFromString,
        "vsql::TypeBuilder: from_string() is required before build()");
    static_assert(HasToString,
                  "vsql::TypeBuilder: to_string() is required before build()");
    static_assert(HasCompare,
                  "vsql::TypeBuilder: compare() is required before build()");
    static_assert(!HasIntToParams || HasParams,
                  "vsql::TypeBuilder: params<P, &parse_fn>() is required when "
                  "int_to_params() is used");
    static_assert(!HasResolveParams || HasParams,
                  "vsql::TypeBuilder: params<P, &parse_fn>() is required when "
                  "resolve_params() is used");
    return TypeObject<EFT>{state_.desc, embedded_funcs_};
  }

  // Cross-specialization and make_type access.
  template <bool, bool, bool, bool, bool, bool, typename, const char *>
  friend class TypeBuilder;

  template <const char *N>
  friend constexpr TypeBuilder<false, false, false, false, false, false,
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
constexpr TypeBuilder<false, false, false, false, false, false, std::tuple<>,
                      Name>
make_type() {
  detail::TypeBuilderState s{};
  s.desc.vef_desc.name = Name;
  s.desc.vef_desc.protocol = VEF_PROTOCOL_1;
  return TypeBuilder<false, false, false, false, false, false, std::tuple<>,
                     Name>{s};
}

}  // namespace vsql

#endif  // VILLAGESQL_VSQL_TYPE_BUILDER_H
