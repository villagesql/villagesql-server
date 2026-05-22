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

// Do not write new code against this header. It is included only for backward
// compatibility and will be removed before Beta. Use the C++ API in
// <villagesql/vsql.h> instead.

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
//   using namespace villagesql;
//
//   // Implement your function
//   void add_impl(IntArg a, IntArg b, IntResult out) {
//     if (a.is_null() || b.is_null()) { out.set_null(); return; }
//     out.set(a.value() + b.value());
//   }
//
//   // Register the extension with inline function definitions
//   VEF_GENERATE_ENTRY_POINTS(
//     make_extension()
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
//   Input:   IntArg, RealArg, StringArg
//            CustomArg, CustomArgWith<P>
//   Output:  IntResult, RealResult, StringResult
//            CustomResult, CustomResultWith<P>
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
//   void rot13_impl(CustomArg in, CustomResult out) {
//     if (in.is_null()) { out.set_null(); return; }
//     auto src = in.value();        // villagesql::Span<const unsigned char>
//     auto dst = out.buffer();      // villagesql::Span<unsigned char>
//     for (size_t i = 0; i < src.size(); i++) { dst[i] = transform(src[i]); }
//     out.set_length(src.size());
//   }
//
// DEFINING FUNCTIONS
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
//
// AGGREGATE FUNCTIONS
// -------------------
//
// Aggregate VDFs (like SQL SUM, COUNT, etc.) accumulate state across rows
// within each GROUP BY group, then return a final result per group.
//
// Use make_aggregate_func<State, &result_fn>() as the entry point. The State
// type is explicit, and all three callback signatures are validated against it
// at compile time.
//
// The result function always uses an output parameter, consistent with normal
// VDFs. All three callbacks follow the same pattern:
//
//   void my_clear(State &s)                     // reset state
//   void my_acc(State &s, TypedArg v, ...)      // accumulate one row
//   void my_result(const State &s, ResultWrapper out)  // produce final value
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
// How it works:
//   - prerun/postrun are auto-generated: prerun allocates State via
//     value-initialization, postrun deletes it.
//   - .clear<&fn>()      void(State&)
//   - .accumulate<&fn>() void(State&, TypedArgs...) — TypedArgs deduced from
//     the function signature (IntArg, StringArg, CustomArg, etc.).
//     Call .accumulate() after all .param(TYPE) calls.
//   - Result function uses void(const State&, ResultWrapper) where
//     ResultWrapper is IntResult, RealResult, StringResult, CustomResult, or
//     CustomResultWith<P>. Use out.set_null() to return SQL NULL.
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
//
//
// DEFINING TYPES
// --------------
//
// Custom types are defined using make_type("name") and chained builder methods,
// ending with .build(). The type operations (encode, decode, compare, hash) are
// implemented as VDFs registered separately using make_type_encode,
// make_type_decode, make_type_compare, and make_type_hash, then referenced
// by name in the type descriptor:
//
//   make_type("mytype")
//     .persisted_length(8)              // Fixed storage size in bytes
//     .max_decode_buffer_length(64)     // Max bytes for string representation
//     .encode("mytype_encode")          // Name of the encode VDF
//     .decode("mytype_decode")          // Name of the decode VDF
//     .compare("mytype_compare")        // Name of the compare VDF
//     .hash("mytype_hash")              // Optional: name of the hash VDF
//     .build()
//
// The VDFs are created using the ergonomic make_type_* entry points:
//
//   make_type_encode<&my_encode>("mytype_encode", MYTYPE)
//   make_type_decode<&my_decode>("mytype_decode", MYTYPE)
//   make_type_compare<&my_compare>("mytype_compare", MYTYPE)
//   make_type_hash<&my_hash>("mytype_hash", MYTYPE)  // optional
//
// Extension authors write against these ergonomic C++ signatures:
//
//   // Encode: string -> binary. false=success, true=error.
//   // Set *length = SIZE_MAX to produce SQL NULL.
//   bool my_encode(std::string_view from, Span<unsigned char> buf,
//                  size_t *length);
//
//   // Decode: binary -> string. false=success, true=error.
//   bool my_decode(Span<const unsigned char> data, Span<char> out,
//                  size_t *out_len);
//
//   // Compare: returns <0, 0, or >0.
//   int my_compare(Span<const unsigned char> a, Span<const unsigned char> b);
//
//   // Hash: returns hash code.
//   size_t my_hash(Span<const unsigned char> data);
//
//
// PARAMETERIZED TYPES
// -------------------
//
// If the type has SQL-level parameters (e.g., TVECTOR(1536)), define a params
// struct and a parse function, then use the struct as the first argument of
// the type operation functions. The SDK detects the parameterized signature
// and wires up a memoized parse cache automatically. Note the parse function
// is called based on the canonicalized output of the `resolve_params`
// function, all parameter error checking should be done there.
//
// The parse function can be a static method on the struct (shown below) or
// any free function with the signature:
//   P parse_fn(const std::map<std::string, std::string>& params)
//
//   struct MyParams {
//     int64_t dimension;
//     static MyParams parse(const std::map<std::string, std::string>& p) {
//       return {.dimension = stoll(p.at("dimension"))};
//     }
//   };
//
//   // encode takes MaybeParams<MyParams>& — params may be unknown so the
//   // function can infer them from the input string and call p.set(...).
//   // Reports outcome via CustomResult: out.set_length(n), out.set_null(),
//   // out.warning(msg), or out.error(msg). Plain CustomResult (not
//   // CustomResultWith<P>) since params come from the MaybeParams& arg.
//   void my_encode(MaybeParams<MyParams>& p, std::string_view from,
//                  CustomResult out) { ... }
//
//   // decode/compare/hash take const MyParams& — params are always known.
//   bool my_decode(const MyParams& p, ...);
//
//   make_type("MYTYPE")
//     .persisted_length(...)
//     .encode("my_encode")
//     .params<MyParams, &MyParams::parse>()  // required; binds parse function
//     .build()
//
//   make_type_encode<&my_encode>("my_encode", MYTYPE)
//
// Both registrations are required: make_type_encode detects the parameterized
// signature and routes through the cache; .params<>() binds the parse
// function at startup. Omitting .params<>() while using parameterized
// signatures will crash at runtime.
// TODO(villagesql-beta): make this a compile time error.
//
// Note if a Params type is registered for more than one custom type, each
// custom type MUST register the same type function.
// TODO(villagesql-beta): remove this restriction.
//
// REGISTERING THE EXTENSION
// -------------------------
//
// Use VEF_GENERATE_ENTRY_POINTS with make_extension():
//
//   VEF_GENERATE_ENTRY_POINTS(
//     make_extension()
//       .type(make_type(MYTYPE)
//         .persisted_length(8)
//         .max_decode_buffer_length(64)
//         .encode("mytype_encode")
//         .decode("mytype_decode")
//         .compare("mytype_compare")
//         .build())
//       .func(make_type_encode<&my_encode>("mytype_encode", MYTYPE))
//       .func(make_type_decode<&my_decode>("mytype_decode", MYTYPE))
//       .func(make_type_compare<&my_compare>("mytype_compare", MYTYPE))
//       .func(make_func<&func1_impl>("func1").returns(INT).build()))
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
//   using namespace villagesql;
//
//   static const size_t kBytearrayLen = 8;
//   constexpr const char* BYTEARRAY = "bytearray";
//
//   // BYTEARRAY type: fixed 8-byte value stored as raw bytes
//
//   // Encode: string -> binary (copy up to 8 bytes, zero-pad)
//   bool bytearray_encode(std::string_view from, Span<unsigned char> buf,
//                         size_t* length) {
//     if (buf.size() < kBytearrayLen) return true;  // error
//     memset(buf.data(), 0, kBytearrayLen);
//     size_t n = from.size() < kBytearrayLen ? from.size() : kBytearrayLen;
//     if (n > 0) memcpy(buf.data(), from.data(), n);
//     *length = kBytearrayLen;
//     return false;  // success
//   }
//
//   // Decode: binary -> string (copy 8 bytes)
//   bool bytearray_decode(Span<const unsigned char> data, Span<char> out,
//                         size_t* out_len) {
//     if (out.size() < kBytearrayLen) return true;  // error
//     memcpy(out.data(), data.data(), kBytearrayLen);
//     *out_len = kBytearrayLen;
//     return false;  // success
//   }
//
//   // Compare: lexicographic byte comparison
//   int bytearray_compare(Span<const unsigned char> a,
//                         Span<const unsigned char> b) {
//     return memcmp(a.data(), b.data(), kBytearrayLen);
//   }
//
//   // ROT13: apply ROT13 cipher to ASCII letters in a bytearray
//   void rot13_impl(CustomArg in, CustomResult out) {
//     if (in.is_null()) { out.set_null(); return; }
//     auto src = in.value();      // villagesql::Span<const unsigned char>
//     auto dst = out.buffer();    // villagesql::Span<unsigned char>
//     for (size_t i = 0; i < kBytearrayLen; i++) {
//       unsigned char c = src[i];
//       if (c >= 'A' && c <= 'Z') c = 'A' + ((c - 'A' + 13) % 26);
//       else if (c >= 'a' && c <= 'z') c = 'a' + ((c - 'a' + 13) % 26);
//       dst[i] = c;
//     }
//     out.set_length(kBytearrayLen);
//   }
//
//   // Register everything
//   VEF_GENERATE_ENTRY_POINTS(
//     make_extension()
//       .type(make_type(BYTEARRAY)
//         .persisted_length(kBytearrayLen)
//         .max_decode_buffer_length(kBytearrayLen)
//         .encode("bytearray_encode")
//         .decode("bytearray_decode")
//         .compare("bytearray_compare")
//         .build())
//       .func(make_type_encode<&bytearray_encode>(
//           "bytearray_encode", BYTEARRAY))
//       .func(make_type_decode<&bytearray_decode>(
//           "bytearray_decode", BYTEARRAY))
//       .func(make_type_compare<&bytearray_compare>(
//           "bytearray_compare", BYTEARRAY))
//       .func(make_func<&rot13_impl>("rot13")
//         .returns(BYTEARRAY)
//         .param(BYTEARRAY)
//         .build()))
//

// V1 extension entry point. Supports raw vef_vdf_func_t VDFs and raw function
// pointer type operations (RawFromStringFunc / RawToStringFunc).
//
// For the typed C++ API (IntArg, RealArg, Span<>, vsql::make_type<>, etc.)
// use villagesql/vsql.h instead.

#include <villagesql/extension_builder.h>
#include <villagesql/func_builder.h>

#endif  // VILLAGESQL_SDK_EXTENSION_H
