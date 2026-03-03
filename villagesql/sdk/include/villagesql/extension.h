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

#ifndef VILLAGESQL_SDK_EXTENSION_H
#define VILLAGESQL_SDK_EXTENSION_H

// =============================================================================
// VillageSQL Extension Framework
// =============================================================================
//
// This is the main header for writing VillageSQL extensions. It provides
// a fluent builder API for defining extension functions and types.
//
// QUICK START
// -----------
//
// Create a .cc file for your extension:
//
//   #include <villagesql/extension.h>
//
//   // Implement your function
//   void add_impl(vef_context_t* ctx,
//                 vef_invalue_t* a, vef_invalue_t* b,
//                 vef_vdf_result_t* result) {
//     result->int_value = a->int_value + b->int_value;
//     result->type = VEF_RESULT_VALUE;
//   }
//
//   // Register the extension with inline function definitions
//   VEF_GENERATE_ENTRY_POINTS(
//     make_extension("my_extension", "1.0.0")
//       .func(make_func<&add_impl>("add")
//         .returns(INT)
//         .param(INT)
//         .param(INT)
//         .build()))
//
//
// TYPED WRAPPERS
// --------------
//
// Typed wrappers from func_types.h provide a type-safe interface for writing
// extension functions. Each parameter and return type has a corresponding
// wrapper class with methods for null checking and value access:
//
//   Input:   IntArg, RealArg, StringArg, BinaryArg
//   Output:  IntResult, RealResult, StringResult, BinaryResult
//
// The registration syntax is identical; the framework detects the parameter
// types and adapts automatically.
//
//   void add_impl(IntArg a, IntArg b, IntResult out) {
//     if (a.is_null() || b.is_null()) { out.set_null(); return; }
//     out.set(a.value() + b.value());
//   }
//
//   make_func<&add_impl>("add").returns(INT).param(INT).param(INT).build();
//
// For binary (custom) types, write directly into the caller-provided buffer
// to avoid copies:
//
//   void rot13_impl(BinaryArg in, BinaryResult out) {
//     if (in.is_null()) { out.set_null(); return; }
//     auto src = in.value();        // villagesql::Span<const unsigned char>
//     auto dst = out.buffer();      // villagesql::Span<unsigned char>
//     for (size_t i = 0; i < src.size(); i++) { dst[i] = transform(src[i]); }
//     out.set_length(src.size());
//   }
//
// DEFINING FUNCTIONS (deprecated, will be removed)
// ------------------
//
// Functions are defined using make_func<&impl>("name") and chained builder
// methods, ending with .build():
//
//   make_func<&my_impl>("my_func")
//     .returns(INT)       // Return type
//     .param(INT)         // First parameter
//     .param(STRING)      // Second parameter
//     .buffer_size(256)   // Optional: output buffer size
//     .build()            // Finalize the function definition
//
// Available types (all passed as strings):
//   - INT    - 64-bit integer
//   - STRING - Variable-length string
//   - REAL   - Double-precision float
//   - Custom types by name (see below)
//
// CUSTOM TYPES
// ------------
//
// For custom types, define a constant to avoid typos:
//
//   constexpr const char* MYTYPE = "mytype";
//
// Then use the constant in your function definitions:
//
//   make_func<&process>("process").returns(MYTYPE).param(STRING).build()
//
//
// PRERUN/POSTRUN FUNCTIONS
// ------------------------
//
// For prerun/postrun functions (per-statement setup/teardown):
//
//   make_func<&my_impl>("my_func")
//     .returns(STRING)
//     .prerun<&my_prerun>()   // Called before first row
//     .postrun<&my_postrun>() // Called after last row
//     .build()
//
// Note: Prerun and postrun functions can be a cumbersome API. The func builder
// already handles simple cases (e.g., type checking for functions with fixed
// args and allocating fixed buffer sizes). We want to cover more cases. If
// you find that you need to use prerun or postrun functions, please come talk
// to us so we can understand your use case.
//
// For type conversion functions (from_string / to_string):
//
//   make_func("mytype_from_string")
//     .from_string<&encode_func>("mytype")   // STRING -> mytype
//
//   make_func("mytype_to_string")
//     .to_string<&decode_func>("mytype")     // mytype -> STRING
//
//
// DEFINING TYPES
// --------------
//
// Custom types are defined using make_type("name") and chained builder methods,
// ending with .build():
//
//   make_type("mytype")
//     .persisted_length(8)           // Fixed storage size in bytes
//     .max_decode_buffer_length(64)  // Max bytes for string representation
//     .encode(&mytype_encode)        // String -> binary
//     .decode(&mytype_decode)        // Binary -> string
//     .compare(&mytype_compare)      // For ORDER BY, indexes
//     .hash(&mytype_hash)            // Optional: for hash joins
//     .build()
//
//
// REGISTERING THE EXTENSION
// -------------------------
//
// Use VEF_GENERATE_ENTRY_POINTS with make_extension():
//
//   VEF_GENERATE_ENTRY_POINTS(
//     make_extension("my_ext", "1.0.0")
//       .func(make_func<&func1_impl>("func1").returns(INT).build())
//       .func(make_func<&func2_impl>("func2").returns(STRING).build())
//       .type(make_type("mytype").persisted_length(8)
//         .encode(&enc).decode(&dec).compare(&cmp).build()))
//
// This generates the extern "C" vef_register() and vef_unregister() functions
// that mysqld calls to load the extension.
//
//
// COMPLETE EXAMPLE
// ----------------
//
//   #include <villagesql/extension.h>
//   #include <cstring>
//
//   static const size_t kBytearrayLen = 8;
//   constexpr const char* BYTEARRAY = "bytearray";
//
//   // BYTEARRAY type: fixed 8-byte value stored as raw bytes
//
//   // Encode: string -> binary (copy up to 8 bytes, zero-pad)
//   bool bytearray_encode(unsigned char* buf, size_t buf_size,
//                         const char* from, size_t from_len, size_t* length) {
//     if (buf_size < kBytearrayLen) return true;  // error
//     memset(buf, 0, kBytearrayLen);
//     size_t copy_len = from_len < kBytearrayLen ? from_len : kBytearrayLen;
//     if (from && copy_len > 0) memcpy(buf, from, copy_len);
//     *length = kBytearrayLen;
//     return false;  // success
//   }
//
//   // Decode: binary -> string (copy 8 bytes)
//   bool bytearray_decode(const unsigned char* buf, size_t buf_size,
//                         char* to, size_t to_size, size_t* to_length) {
//     if (to_size < kBytearrayLen) return true;  // error
//     memcpy(to, buf, kBytearrayLen);
//     *to_length = kBytearrayLen;
//     return false;  // success
//   }
//
//   // Compare: lexicographic byte comparison
//   int bytearray_compare(const unsigned char* a, size_t a_len,
//                         const unsigned char* b, size_t b_len) {
//     return memcmp(a, b, kBytearrayLen);
//   }
//
//   // ROT13: apply ROT13 cipher to ASCII letters in a bytearray
//   void rot13_impl(vef_context_t* ctx,
//                   vef_invalue_t* input, vef_vdf_result_t* result) {
//     if (input->is_null) { result->type = VEF_RESULT_NULL; return; }
//     for (size_t i = 0; i < kBytearrayLen; i++) {
//       unsigned char c = input->bin_value[i];
//       if (c >= 'A' && c <= 'Z') c = 'A' + ((c - 'A' + 13) % 26);
//       else if (c >= 'a' && c <= 'z') c = 'a' + ((c - 'a' + 13) % 26);
//       result->bin_buf[i] = c;
//     }
//     result->type = VEF_RESULT_VALUE;
//     result->actual_len = kBytearrayLen;
//   }
//
//   // Register everything inline
//   VEF_GENERATE_ENTRY_POINTS(
//     make_extension("bytearray_ext", "1.0.0")
//       .type(make_type(BYTEARRAY)
//         .persisted_length(kBytearrayLen)
//         .max_decode_buffer_length(kBytearrayLen)
//         .encode(&bytearray_encode)
//         .decode(&bytearray_decode)
//         .compare(&bytearray_compare)
//         .build())
//       .func(make_func<&rot13_impl>("rot13")
//         .returns(BYTEARRAY)
//         .param(BYTEARRAY)
//         .build()))
//

#include <villagesql/extension_builder.h>

#endif  // VILLAGESQL_SDK_EXTENSION_H
