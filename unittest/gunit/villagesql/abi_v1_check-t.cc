/* Copyright (c) 2026 VillageSQL Contributors
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see <https://www.gnu.org/licenses/>.
 */

// ABI v1 Compile-time Layout Checks
//
// Each static_assert fires at compile time if the binary layout of a struct in
// villagesql/stable_sdk/v1/include/villagesql/abi/types.h changes.  A failure
// here means the server has broken ABI compatibility with extensions compiled
// against the v1 headers.
//
// Sizes and offsets were derived from the 64-bit LP64 layout (Linux/macOS
// x86-64 and ARM64) where:
//   pointer/size_t = 8 bytes (align 8), int/unsigned int = 4 bytes (align 4),
//   bool = 1 byte, double = 8 bytes, long long = 8 bytes,
//   enum : int / enum : unsigned int = 4 bytes (align 4).

#include <gtest/gtest.h>

#include <cstddef>

#include "villagesql/stable_sdk/v1/include/villagesql/abi/types.h"

// ---------------------------------------------------------------------------
// vef_version_t
//   unsigned int major;    // +0
//   unsigned int minor;    // +4
//   unsigned int patch;    // +8
//   [4 bytes padding]
//   unsigned char *extra;  // +16
// ---------------------------------------------------------------------------
static_assert(sizeof(vef_version_t) == 24,
              "ABI v1 break: vef_version_t size changed");
static_assert(offsetof(vef_version_t, major) == 0,
              "ABI v1 break: vef_version_t::major offset changed");
static_assert(offsetof(vef_version_t, minor) == 4,
              "ABI v1 break: vef_version_t::minor offset changed");
static_assert(offsetof(vef_version_t, patch) == 8,
              "ABI v1 break: vef_version_t::patch offset changed");
static_assert(offsetof(vef_version_t, extra) == 16,
              "ABI v1 break: vef_version_t::extra offset changed");

// ---------------------------------------------------------------------------
// vef_context_t
//   vef_protocol_t protocol;  // +0
// ---------------------------------------------------------------------------
static_assert(sizeof(vef_context_t) == 4,
              "ABI v1 break: vef_context_t size changed");
static_assert(offsetof(vef_context_t, protocol) == 0,
              "ABI v1 break: vef_context_t::protocol offset changed");

// ---------------------------------------------------------------------------
// vef_register_arg_t
//   vef_protocol_t protocol;        // +0
//   [4 bytes padding]
//   vef_version_t mysql_version;    // +8
//   vef_version_t vef_version;      // +32
// ---------------------------------------------------------------------------
static_assert(sizeof(vef_register_arg_t) == 56,
              "ABI v1 break: vef_register_arg_t size changed");
static_assert(offsetof(vef_register_arg_t, protocol) == 0,
              "ABI v1 break: vef_register_arg_t::protocol offset changed");
static_assert(offsetof(vef_register_arg_t, mysql_version) == 8,
              "ABI v1 break: vef_register_arg_t::mysql_version offset changed");
static_assert(offsetof(vef_register_arg_t, vef_version) == 32,
              "ABI v1 break: vef_register_arg_t::vef_version offset changed");

// ---------------------------------------------------------------------------
// vef_unregister_arg_t
//   vef_protocol_t protocol;  // +0
// ---------------------------------------------------------------------------
static_assert(sizeof(vef_unregister_arg_t) == 4,
              "ABI v1 break: vef_unregister_arg_t size changed");
static_assert(offsetof(vef_unregister_arg_t, protocol) == 0,
              "ABI v1 break: vef_unregister_arg_t::protocol offset changed");

// ---------------------------------------------------------------------------
// vef_invalue_t
//   vef_type_id type;   // +0  (enum : int, 4 bytes)
//   bool is_null;       // +4
//   [3 bytes padding]
//   union { ... };      // +8  (16 bytes: two 8-byte members)
// ---------------------------------------------------------------------------
static_assert(sizeof(vef_invalue_t) == 24,
              "ABI v1 break: vef_invalue_t size changed");
static_assert(offsetof(vef_invalue_t, type) == 0,
              "ABI v1 break: vef_invalue_t::type offset changed");
static_assert(offsetof(vef_invalue_t, is_null) == 4,
              "ABI v1 break: vef_invalue_t::is_null offset changed");

// ---------------------------------------------------------------------------
// vef_vdf_result_t
//   vef_return_value_type_t type;  // +0
//   [4 bytes padding]
//   size_t actual_len;             // +8
//   char *error_msg;               // +16
//   union { ... };                 // +24  (24 bytes: three 8-byte members)
// ---------------------------------------------------------------------------
static_assert(sizeof(vef_vdf_result_t) == 48,
              "ABI v1 break: vef_vdf_result_t size changed");
static_assert(offsetof(vef_vdf_result_t, type) == 0,
              "ABI v1 break: vef_vdf_result_t::type offset changed");
static_assert(offsetof(vef_vdf_result_t, actual_len) == 8,
              "ABI v1 break: vef_vdf_result_t::actual_len offset changed");
static_assert(offsetof(vef_vdf_result_t, error_msg) == 16,
              "ABI v1 break: vef_vdf_result_t::error_msg offset changed");

// ---------------------------------------------------------------------------
// vef_vdf_args_t
//   void *user_data;          // +0
//   unsigned int value_count; // +8
//   [4 bytes padding]
//   vef_invalue_t *values;    // +16
// ---------------------------------------------------------------------------
static_assert(sizeof(vef_vdf_args_t) == 24,
              "ABI v1 break: vef_vdf_args_t size changed");
static_assert(offsetof(vef_vdf_args_t, user_data) == 0,
              "ABI v1 break: vef_vdf_args_t::user_data offset changed");
static_assert(offsetof(vef_vdf_args_t, value_count) == 8,
              "ABI v1 break: vef_vdf_args_t::value_count offset changed");
static_assert(offsetof(vef_vdf_args_t, values) == 16,
              "ABI v1 break: vef_vdf_args_t::values offset changed");

// ---------------------------------------------------------------------------
// vef_type_t
//   vef_type_id id;          // +0
//   [4 bytes padding]
//   const char *custom_type; // +8
// ---------------------------------------------------------------------------
static_assert(sizeof(vef_type_t) == 16,
              "ABI v1 break: vef_type_t size changed");
static_assert(offsetof(vef_type_t, id) == 0,
              "ABI v1 break: vef_type_t::id offset changed");
static_assert(offsetof(vef_type_t, custom_type) == 8,
              "ABI v1 break: vef_type_t::custom_type offset changed");

// ---------------------------------------------------------------------------
// vef_signature_t
//   unsigned int param_count;    // +0
//   [4 bytes padding]
//   const vef_type_t *params;    // +8
//   vef_type_t return_type;      // +16
// ---------------------------------------------------------------------------
static_assert(sizeof(vef_signature_t) == 32,
              "ABI v1 break: vef_signature_t size changed");
static_assert(offsetof(vef_signature_t, param_count) == 0,
              "ABI v1 break: vef_signature_t::param_count offset changed");
static_assert(offsetof(vef_signature_t, params) == 8,
              "ABI v1 break: vef_signature_t::params offset changed");
static_assert(offsetof(vef_signature_t, return_type) == 16,
              "ABI v1 break: vef_signature_t::return_type offset changed");

// ---------------------------------------------------------------------------
// vef_prerun_args_t
//   unsigned int arg_count;   // +0
//   [4 bytes padding]
//   vef_type_t *arg_types;    // +8
//   char **const_values;      // +16
//   size_t *const_lengths;    // +24
// ---------------------------------------------------------------------------
static_assert(sizeof(vef_prerun_args_t) == 32,
              "ABI v1 break: vef_prerun_args_t size changed");
static_assert(offsetof(vef_prerun_args_t, arg_count) == 0,
              "ABI v1 break: vef_prerun_args_t::arg_count offset changed");
static_assert(offsetof(vef_prerun_args_t, arg_types) == 8,
              "ABI v1 break: vef_prerun_args_t::arg_types offset changed");
static_assert(offsetof(vef_prerun_args_t, const_values) == 16,
              "ABI v1 break: vef_prerun_args_t::const_values offset changed");
static_assert(offsetof(vef_prerun_args_t, const_lengths) == 24,
              "ABI v1 break: vef_prerun_args_t::const_lengths offset changed");

// ---------------------------------------------------------------------------
// vef_prerun_result_t
//   vef_return_value_type_t type; // +0
//   [4 bytes padding]
//   char *error_msg;              // +8
//   size_t result_buffer_size;    // +16
//   void *user_data;              // +24
// ---------------------------------------------------------------------------
static_assert(sizeof(vef_prerun_result_t) == 32,
              "ABI v1 break: vef_prerun_result_t size changed");
static_assert(offsetof(vef_prerun_result_t, type) == 0,
              "ABI v1 break: vef_prerun_result_t::type offset changed");
static_assert(offsetof(vef_prerun_result_t, error_msg) == 8,
              "ABI v1 break: vef_prerun_result_t::error_msg offset changed");
static_assert(
    offsetof(vef_prerun_result_t, result_buffer_size) == 16,
    "ABI v1 break: vef_prerun_result_t::result_buffer_size offset changed");
static_assert(offsetof(vef_prerun_result_t, user_data) == 24,
              "ABI v1 break: vef_prerun_result_t::user_data offset changed");

// ---------------------------------------------------------------------------
// vef_postrun_args_t
//   void *user_data;  // +0
// ---------------------------------------------------------------------------
static_assert(sizeof(vef_postrun_args_t) == 8,
              "ABI v1 break: vef_postrun_args_t size changed");
static_assert(offsetof(vef_postrun_args_t, user_data) == 0,
              "ABI v1 break: vef_postrun_args_t::user_data offset changed");

// ---------------------------------------------------------------------------
// vef_func_desc_t
//   vef_protocol_t protocol;     // +0
//   [4 bytes padding]
//   const char *name;            // +8
//   vef_signature_t *signature;  // +16
//   vef_vdf_func_t vdf;          // +24
//   vef_prerun_func_t prerun;    // +32
//   vef_postrun_func_t postrun;  // +40
//   size_t buffer_size;          // +48
// ---------------------------------------------------------------------------
static_assert(sizeof(vef_func_desc_t) == 56,
              "ABI v1 break: vef_func_desc_t size changed");
static_assert(offsetof(vef_func_desc_t, protocol) == 0,
              "ABI v1 break: vef_func_desc_t::protocol offset changed");
static_assert(offsetof(vef_func_desc_t, name) == 8,
              "ABI v1 break: vef_func_desc_t::name offset changed");
static_assert(offsetof(vef_func_desc_t, signature) == 16,
              "ABI v1 break: vef_func_desc_t::signature offset changed");
static_assert(offsetof(vef_func_desc_t, vdf) == 24,
              "ABI v1 break: vef_func_desc_t::vdf offset changed");
static_assert(offsetof(vef_func_desc_t, prerun) == 32,
              "ABI v1 break: vef_func_desc_t::prerun offset changed");
static_assert(offsetof(vef_func_desc_t, postrun) == 40,
              "ABI v1 break: vef_func_desc_t::postrun offset changed");
static_assert(offsetof(vef_func_desc_t, buffer_size) == 48,
              "ABI v1 break: vef_func_desc_t::buffer_size offset changed");

// ---------------------------------------------------------------------------
// vef_type_desc_t
//   vef_protocol_t protocol;                  // +0
//   [4 bytes padding]
//   const char *name;                         // +8
//   int64_t persisted_length;                 // +16
//   int64_t max_decode_buffer_length;         // +24
//   vef_encode_func_t encode_func;            // +32
//   vef_decode_func_t decode_func;            // +40
//   vef_compare_func_t compare_func;          // +48
//   vef_hash_func_t hash_func;               // +56
// ---------------------------------------------------------------------------
static_assert(sizeof(vef_type_desc_t) == 64,
              "ABI v1 break: vef_type_desc_t size changed");
static_assert(offsetof(vef_type_desc_t, protocol) == 0,
              "ABI v1 break: vef_type_desc_t::protocol offset changed");
static_assert(offsetof(vef_type_desc_t, name) == 8,
              "ABI v1 break: vef_type_desc_t::name offset changed");
static_assert(offsetof(vef_type_desc_t, persisted_length) == 16,
              "ABI v1 break: vef_type_desc_t::persisted_length offset changed");
static_assert(
    offsetof(vef_type_desc_t, max_decode_buffer_length) == 24,
    "ABI v1 break: vef_type_desc_t::max_decode_buffer_length offset changed");
static_assert(offsetof(vef_type_desc_t, encode_func) == 32,
              "ABI v1 break: vef_type_desc_t::encode_func offset changed");
static_assert(offsetof(vef_type_desc_t, decode_func) == 40,
              "ABI v1 break: vef_type_desc_t::decode_func offset changed");
static_assert(offsetof(vef_type_desc_t, compare_func) == 48,
              "ABI v1 break: vef_type_desc_t::compare_func offset changed");
static_assert(offsetof(vef_type_desc_t, hash_func) == 56,
              "ABI v1 break: vef_type_desc_t::hash_func offset changed");

// ---------------------------------------------------------------------------
// vef_registration_t
//   vef_protocol_t protocol;          // +0
//   [4 bytes padding]
//   char *error_msg;                  // +8
//   const char *extension_version;   // +16
//   vef_version_t sdk_version;        // +24  (size 24)
//   const char *extension_name;       // +48
//   unsigned int func_count;          // +56
//   [4 bytes padding]
//   vef_func_desc_t **funcs;         // +64
//   unsigned int type_count;          // +72
//   [4 bytes padding]
//   vef_type_desc_t **types;         // +80
// ---------------------------------------------------------------------------
static_assert(sizeof(vef_registration_t) == 88,
              "ABI v1 break: vef_registration_t size changed");
static_assert(offsetof(vef_registration_t, protocol) == 0,
              "ABI v1 break: vef_registration_t::protocol offset changed");
static_assert(offsetof(vef_registration_t, error_msg) == 8,
              "ABI v1 break: vef_registration_t::error_msg offset changed");
static_assert(
    offsetof(vef_registration_t, extension_version) == 16,
    "ABI v1 break: vef_registration_t::extension_version offset changed");
static_assert(offsetof(vef_registration_t, sdk_version) == 24,
              "ABI v1 break: vef_registration_t::sdk_version offset changed");
static_assert(
    offsetof(vef_registration_t, extension_name) == 48,
    "ABI v1 break: vef_registration_t::extension_name offset changed");
static_assert(offsetof(vef_registration_t, func_count) == 56,
              "ABI v1 break: vef_registration_t::func_count offset changed");
static_assert(offsetof(vef_registration_t, funcs) == 64,
              "ABI v1 break: vef_registration_t::funcs offset changed");
static_assert(offsetof(vef_registration_t, type_count) == 72,
              "ABI v1 break: vef_registration_t::type_count offset changed");
static_assert(offsetof(vef_registration_t, types) == 80,
              "ABI v1 break: vef_registration_t::types offset changed");

namespace {

// All checks above are static_asserts that fire at compile time.
// This test is a placeholder to satisfy gtest's requirement for at least one
// test case.
TEST(AbiV1Check, CompileTimeLayoutsVerified) {
  // If this test binary compiled successfully, all ABI layout checks passed.
  SUCCEED();
}

}  // namespace
