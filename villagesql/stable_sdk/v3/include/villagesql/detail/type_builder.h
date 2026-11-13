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

#ifndef VILLAGESQL_DETAIL_TYPE_BUILDER_H
#define VILLAGESQL_DETAIL_TYPE_BUILDER_H

// Internal implementation of vsql/type_builder.h. Not part of the public API.

#include <cstddef>
#include <string_view>

#include <villagesql/type_builder.h>
#include <villagesql/vsql/type_params.h>

namespace vsql {
namespace detail {

// Binds the parse function for parameterized type caches. Stored as a function
// pointer in TypeObject.params_init_fn and called once during registration.
template <typename P, auto ParseFunc>
void bind_params_cache() {
  type_params_cache_for<P>().bind(ParseFunc);
}

// Binds the inverse-of-parse (params_to_strings) function. Stored in
// TypeObject.params_to_strings_init_fn and called once during registration.
// P is recovered from the function signature via ParamsToStringsFunc<P>.
template <typename P, auto ToStringsFunc>
void bind_params_to_strings_cache() {
  type_params_cache_for<P>().bind_to_strings(ToStringsFunc);
}

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
// hidden visibility prevents weak-symbol coalescing across DSO boundaries on
// Linux, ensuring the pointer value in the extension DSO is stable after
// dlopen.
template <const char *TypeName, TypeOp Op>
inline constexpr
    __attribute__((visibility("hidden"))) TypeOpVdfName<TypeName, Op>
        kTypeOpVdfName{};

// Shared builder state passed by value between TypeBuilder specializations.
//
// params_init_fn / params_to_strings_init_fn are typed-API-side state set by
// the two- and three-argument forms of .params<P, &Parse[, &ToStrings]>().
// They are forwarded into the built TypeObject and called once at extension
// registration to bind the corresponding callbacks into TypeParamsCache<P>.
// They live here (not on the low-level TypeDescriptor) because parameterized
// types only flow through the typed C++ API.
struct TypeBuilderState {
  villagesql::type_builder::TypeDescriptor desc;
  void (*params_init_fn)() = nullptr;
  void (*params_to_strings_init_fn)() = nullptr;
};

}  // namespace detail
}  // namespace vsql

#endif  // VILLAGESQL_DETAIL_TYPE_BUILDER_H
