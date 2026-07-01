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
//   void complex_from_string(std::string_view s, vsql::CustomResult out);
//   void complex_to_string(vsql::CustomArg in, vsql::StringResult out);
//   int  complex_compare(vsql::CustomArg a, vsql::CustomArg b);
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
// Optional result sizing on the function builder:
//   .buffer_size(n)         // initial row-time output buffer; grows on demand
//   .max_result_length(n)   // max STRING result length; sizes the result
//                           // column so a materialized STRING result is not
//                           // truncated at the argument width. Without it the
//                           // column falls back to the argument width, like a
//                           // classic UDF that does not set
//                           initid->max_length.
//                           // Has no effect for non-STRING return types.
//
// For aggregate VDFs (SQL SUM, COUNT, etc.), see make_aggregate_func in
// vsql/func_builder.h. For per-statement lifecycle hooks (prerun/postrun),
// see vsql/pre_post_run.h. For full documentation see the individual
// headers below.

// Typed function and type-operation builders (rejects raw ABI signatures).
#include <villagesql/vsql/func_builder.h>

// Typed argument/result wrappers: IntArg, RealArg, StringArg, CustomArg, etc.
#include <villagesql/vsql/func_types.h>

// Varargs typed views: vsql::VarArgs and vsql::AnyArg
#include <villagesql/vsql/var_args.h>

// Object-based type builder: vsql::make_type<Name>()
#include <villagesql/vsql/type_builder.h>

// MaybeParams<P>, TypeParamsCache<P>, type_params_cache_for<P>()
#include <villagesql/vsql/type_params.h>

// vsql ExtensionBuilder, make_extension(), and VEF_GENERATE_ENTRY_POINTS
#include <villagesql/vsql/extension_builder.h>

namespace vsql {

// Re-export from func_builder sub-namespace into the flat vsql:: namespace
using func_builder::INT;
using func_builder::make_aggregate_func;
using func_builder::make_func;
using func_builder::make_int_to_params;
using func_builder::make_intrinsic_default;
using func_builder::make_resolve_params;
using func_builder::make_type_compare;
using func_builder::make_type_decode;
using func_builder::make_type_encode;
using func_builder::make_type_hash;
using func_builder::REAL;
using func_builder::STRING;

}  // namespace vsql

#endif  // VILLAGESQL_VSQL_H
