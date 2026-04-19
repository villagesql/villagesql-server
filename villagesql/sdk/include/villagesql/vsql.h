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

#ifndef VILLAGESQL_VSQL_H
#define VILLAGESQL_VSQL_H

// =============================================================================
// VillageSQL Extension Framework — Typed C++ API (::vsql namespace)
// =============================================================================
//
// This is the main header for writing VillageSQL extensions using the typed
// C++ API. Raw ABI types (vef_context_t*, vef_invalue_t*, vef_encode_func_t,
// etc.) are not exposed here; use typed wrappers (IntArg, RealArg, Span<>,
// etc.) and the object-based type builder (vsql::make_type<Name>()) instead.
//
// QUICK START
// -----------
//
//   #include <villagesql/vsql.h>
//   using namespace vsql;
//
//   // 1. Implement your type operations
//   bool complex_from_string(std::string_view s,
//                            villagesql::Span<unsigned char> buf, size_t* len);
//   bool complex_to_string(villagesql::Span<const unsigned char> data,
//                          villagesql::Span<char> out, size_t* out_len);
//   int  complex_compare(villagesql::Span<const unsigned char> a,
//                        villagesql::Span<const unsigned char> b);
//
//   // 2. Define the type as a constexpr object
//   static constexpr const char kComplexTypeName[] = "COMPLEX";
//   constexpr auto COMPLEX = make_type<kComplexTypeName>()
//       .persisted_length(16)
//       .max_decode_buffer_length(64)
//       .from_string<&complex_from_string>()
//       .to_string<&complex_to_string>()
//       .compare<&complex_compare>()
//       .build();  // compile error if any required operation is missing
//
//   // 3. Implement regular VDFs using COMPLEX as a type reference
//   void complex_add(CustomArg a, CustomArg b, CustomResult out) { ... }
//
//   // 4. Register
//   VEF_GENERATE_ENTRY_POINTS(
//     make_extension()
//       .type(COMPLEX)                          // type object, not a string
//       .func(make_func<&complex_add>("complex_add")
//           .returns(COMPLEX)
//           .param(COMPLEX)
//           .param(COMPLEX)
//           .build()))
//
// For full documentation see the individual headers below.

// Typed function and type-operation builders (rejects raw ABI signatures).
#include <villagesql/vsql/func_builder.h>

// Typed argument/result wrappers: IntArg, RealArg, StringArg, CustomArg, etc.
#include <villagesql/vsql/func_types.h>

// Object-based type builder: vsql::make_type<Name>()
#include <villagesql/vsql/type_builder.h>

// Parameterized type cache: TypeParamsCache<P>, type_params_cache_for<P>()
#include <villagesql/vsql/type_params_cache.h>

// System variable builder: make_sys_var_int(), make_sys_var_str(), etc.
#include <villagesql/vsql/sys_var_builder.h>

// Keyring access: vsql::keyring::read(), vsql::keyring::write()
#include <villagesql/vsql/keyring.h>

// SQL query execution from background threads: villagesql::run_query()
#include <villagesql/vsql/run_query.h>

// Extension builder and VEF_GENERATE_ENTRY_POINTS macro
#include <villagesql/extension_builder.h>

namespace vsql {

// Re-export make_extension from villagesql::extension_builder
using villagesql::extension_builder::make_extension;

// Re-export make_func and type-operation entry points from the typed builder
using villagesql::func_builder::make_func;
using villagesql::func_builder::make_int_to_params;
using villagesql::func_builder::make_intrinsic_default;
using villagesql::func_builder::make_resolve_params;
using villagesql::func_builder::make_type_compare;
using villagesql::func_builder::make_type_decode;
using villagesql::func_builder::make_type_encode;
using villagesql::func_builder::make_type_hash;

// Re-export sys_var, keyring, and run_query
namespace sys_var = villagesql::sys_var;
namespace keyring = villagesql::keyring;
using villagesql::run_query;
using villagesql::sys_var_builder::make_sys_var_bool;
using villagesql::sys_var_builder::make_sys_var_double;
using villagesql::sys_var_builder::make_sys_var_int;
using villagesql::sys_var_builder::make_sys_var_str;

// Re-export typed argument/result wrappers
using villagesql::CustomArg;
using villagesql::CustomArgWith;
using villagesql::CustomResult;
using villagesql::CustomResultWith;
using villagesql::IntArg;
using villagesql::IntResult;
using villagesql::RealArg;
using villagesql::RealResult;
using villagesql::Span;
using villagesql::StringArg;
using villagesql::StringResult;

// Re-export built-in type name constants
using villagesql::func_builder::INT;
using villagesql::func_builder::REAL;
using villagesql::func_builder::STRING;

}  // namespace vsql

#endif  // VILLAGESQL_VSQL_H
