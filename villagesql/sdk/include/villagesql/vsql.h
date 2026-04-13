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
// VillageSQL Extension Framework — Object-based API (::vsql namespace)
// =============================================================================
//
// This header provides the new object-based API for writing VillageSQL
// extensions. Include this instead of <villagesql/extension.h> when using
// the new API.
//
// QUICK START
// -----------
//
//   #include <villagesql/vsql.h>
//   using namespace vsql;
//
//   // 1. Implement your type operations
//   bool complex_from_string(std::string_view s,
//                            villagesql::Span<unsigned char> buf, size_t*
//                            len);
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
//       .from_string<&complex_from_string>()  // statically checked signature
//       .to_string<&complex_to_string>()
//       .compare<&complex_compare>()
//       .build();  // compile error if any required operation is missing
//
//   // 3. Implement regular VDFs, using COMPLEX as a type reference
//   void complex_add(CustomArg a, CustomArg b, CustomResult out) { ... }
//
//   // 4. Register
//   VEF_GENERATE_ENTRY_POINTS(
//     make_extension("my_ext", "1.0.0")
//       .type(COMPLEX)                          // type object, not a string
//       .func(make_func<&complex_add>("complex_add")
//           .returns(COMPLEX)                   // type object in signature
//           .param(COMPLEX)
//           .param(COMPLEX)
//           .build()))
//
// STATIC CHECKING
// ---------------
//
// - build() fails to compile if from_string, to_string, or compare is missing.
// - Each template method checks the function signature via static_assert.
//   Wrong signatures (wrong param count, wrong types) are compile errors.
// - Type name mismatches are impossible: .returns(COMPLEX) derives the SQL
//   name from the object; no manual string is involved.
//
// BACKWARD COMPATIBILITY
// ----------------------
//
// The existing villagesql::type_builder and villagesql::func_builder APIs
// are unchanged. Extensions using the old API continue to compile. The new
// vsql API is additive — both styles can be mixed in the same extension.
//
// LIMITATIONS
// -----------
//
// - intrinsic_default VDFs must be registered separately with
//   make_intrinsic_default(). Reference them by name with
//   .intrinsic_default_vdf("vdf_name") on the vsql TypeBuilder.

// Pull in the full existing SDK (func_types, func_builder, extension_builder,
// VEF_GENERATE_ENTRY_POINTS macro, etc.)
#include <villagesql/extension.h>

// New object-based type builder
#include <villagesql/vsql/type_builder.h>

// System variable builder
#include <villagesql/vsql/sys_var_builder.h>

// Keyring access
#include <villagesql/vsql/keyring.h>

// Bring the new vsql API into scope alongside the existing villagesql names.
// After `using namespace vsql`, make_type() comes from ::vsql, while
// make_func() and make_extension() continue to come from
// villagesql::func_builder / villagesql::extension_builder (via extension.h).
namespace vsql {

// Re-export make_func and make_extension from the existing villagesql API
// so that `using namespace vsql` is sufficient for a complete extension.
using villagesql::extension_builder::make_extension;
using villagesql::func_builder::make_func;

// Re-export sys_var and keyring namespaces
namespace sys_var = villagesql::sys_var;
namespace keyring = villagesql::keyring;
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

// Re-export built-in type name constants (the string versions, for
// .returns(INT) / .param(STRING) etc.)
using villagesql::func_builder::INT;
using villagesql::func_builder::REAL;
using villagesql::func_builder::STRING;

}  // namespace vsql

#endif  // VILLAGESQL_VSQL_H
