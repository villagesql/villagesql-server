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

// ABI v2 Compile-time Layout Checks
//
// Verifies the binary layout of protocol-2 fields in the main SDK headers.
// A failure here means the server has broken ABI compatibility with extensions
// compiled against VEF_PROTOCOL_2 headers.
//
// Sizes and offsets were derived from the 64-bit LP64 layout (Linux/macOS
// x86-64 and ARM64) where:
//   pointer/size_t = 8 bytes (align 8), int/unsigned int = 4 bytes (align 4),
//   bool = 1 byte, double = 8 bytes, long long = 8 bytes,
//   enum : int / enum : unsigned int = 4 bytes (align 4).

#include <gtest/gtest.h>

#include <cstddef>

#include "villagesql/sdk/include/villagesql/abi/types.h"

// ---------------------------------------------------------------------------
// vef_type_desc_t (protocol >= VEF_PROTOCOL_2 fields)
//
// Full layout including v1 and v2 fields:
//   vef_protocol_t protocol;                        // +0
//   [4 bytes padding]
//   const char *name;                               // +8
//   int64_t persisted_length;                       // +16
//   int64_t max_decode_buffer_length;               // +24
//   vef_encode_func_t encode_func;                  // +32  (protocol >= 1)
//   vef_decode_func_t decode_func;                  // +40  (protocol >= 1)
//   vef_compare_func_t compare_func;                // +48  (protocol >= 1)
//   vef_hash_func_t hash_func;                      // +56  (protocol >= 1)
//   const char *encode_vdf_name;                    // +64  (protocol >= 2)
//   const char *decode_vdf_name;                    // +72  (protocol >= 2)
//   const char *compare_vdf_name;                   // +80  (protocol >= 2)
//   const char *hash_vdf_name;                      // +88  (protocol >= 2)
//   const char *int_to_params_vdf_name;             // +96  (protocol >= 2)
//   const char *resolve_params_vdf_name;            // +104 (protocol >= 2)
//   const char *intrinsic_default_vdf_name;         // +112 (protocol >= 2)
//   const char *intrinsic_default_str;              // +120 (protocol >= 2)
//   vef_type_storage_intf_t *storage_intf;          // +128 (protocol >= 2)
//   int64_t max_persisted_length;                   // +136 (protocol >= 2)
// ---------------------------------------------------------------------------
static_assert(sizeof(vef_type_desc_t) == 144,
              "ABI v2 break: vef_type_desc_t size changed");
static_assert(offsetof(vef_type_desc_t, encode_vdf_name) == 64,
              "ABI v2 break: vef_type_desc_t::encode_vdf_name offset changed");
static_assert(offsetof(vef_type_desc_t, decode_vdf_name) == 72,
              "ABI v2 break: vef_type_desc_t::decode_vdf_name offset changed");
static_assert(offsetof(vef_type_desc_t, compare_vdf_name) == 80,
              "ABI v2 break: vef_type_desc_t::compare_vdf_name offset changed");
static_assert(offsetof(vef_type_desc_t, hash_vdf_name) == 88,
              "ABI v2 break: vef_type_desc_t::hash_vdf_name offset changed");
static_assert(
    offsetof(vef_type_desc_t, int_to_params_vdf_name) == 96,
    "ABI v2 break: vef_type_desc_t::int_to_params_vdf_name offset changed");
static_assert(
    offsetof(vef_type_desc_t, resolve_params_vdf_name) == 104,
    "ABI v2 break: vef_type_desc_t::resolve_params_vdf_name offset changed");
static_assert(
    offsetof(vef_type_desc_t, intrinsic_default_vdf_name) == 112,
    "ABI v2 break: vef_type_desc_t::intrinsic_default_vdf_name offset changed");
static_assert(
    offsetof(vef_type_desc_t, intrinsic_default_str) == 120,
    "ABI v2 break: vef_type_desc_t::intrinsic_default_str offset changed");
static_assert(offsetof(vef_type_desc_t, storage_intf) == 128,
              "ABI v2 break: vef_type_desc_t::storage_intf offset changed");
static_assert(offsetof(vef_type_desc_t, max_persisted_length) == 136,
              "ABI v2 break: vef_type_desc_t::max_persisted_length offset "
              "changed");

// ---------------------------------------------------------------------------
// vef_type_params_t (protocol >= VEF_PROTOCOL_2)
//   unsigned int count;            // +0
//   [4 bytes padding]
//   const char *const *keys;      // +8
//   const char *const *values;    // +16
// ---------------------------------------------------------------------------
static_assert(sizeof(vef_type_params_t) == 24,
              "ABI v2 break: vef_type_params_t size changed");
static_assert(offsetof(vef_type_params_t, count) == 0,
              "ABI v2 break: vef_type_params_t::count offset changed");
static_assert(offsetof(vef_type_params_t, keys) == 8,
              "ABI v2 break: vef_type_params_t::keys offset changed");
static_assert(offsetof(vef_type_params_t, values) == 16,
              "ABI v2 break: vef_type_params_t::values offset changed");

// ---------------------------------------------------------------------------
// vef_invalue_t (protocol >= VEF_PROTOCOL_2 adds type_params in CUSTOM)
//   vef_type_id type;              // +0  (enum : int, 4 bytes)
//   bool is_null;                  // +4
//   [3 bytes padding]
//   union { ... };                 // +8  (40 bytes: CUSTOM has bin_len +
//                                  //      bin_value + type_params)
// ---------------------------------------------------------------------------
static_assert(sizeof(vef_invalue_t) == 48,
              "ABI v2 break: vef_invalue_t size changed");
static_assert(offsetof(vef_invalue_t, type_params) == 24,
              "ABI v2 break: vef_invalue_t::type_params offset changed");

// ---------------------------------------------------------------------------
// vef_vdf_result_t (protocol >= VEF_PROTOCOL_2 adds type_params in CUSTOM)
//   vef_return_value_type_t type;             // +0
//   [4 bytes padding]
//   size_t actual_len;                        // +8
//   char *error_msg;                          // +16
//   union { ... };                            // +24  (48 bytes: CUSTOM has
//                                             //   bin_buf + max_bin_len +
//                                             //   alt_bin_buf + type_params)
// ---------------------------------------------------------------------------
static_assert(sizeof(vef_vdf_result_t) == 72,
              "ABI v2 break: vef_vdf_result_t size changed");
static_assert(offsetof(vef_vdf_result_t, type_params) == 48,
              "ABI v2 break: vef_vdf_result_t::type_params offset changed");

// ---------------------------------------------------------------------------
// vef_inferred_type_params_t (protocol >= VEF_PROTOCOL_2)
//   char *buf;             // +0   (8 bytes)
//   size_t max_buf_len;    // +8   (8 bytes)
//   size_t actual_len;     // +16  (8 bytes)
//   bool overflow;         // +24  (1 byte, +7 padding)
// ---------------------------------------------------------------------------
static_assert(sizeof(vef_inferred_type_params_t) == 32,
              "ABI v2 break: vef_inferred_type_params_t size changed");
static_assert(offsetof(vef_inferred_type_params_t, buf) == 0,
              "ABI v2 break: vef_inferred_type_params_t::buf offset changed");
static_assert(
    offsetof(vef_inferred_type_params_t, max_buf_len) == 8,
    "ABI v2 break: vef_inferred_type_params_t::max_buf_len offset changed");
static_assert(
    offsetof(vef_inferred_type_params_t, actual_len) == 16,
    "ABI v2 break: vef_inferred_type_params_t::actual_len offset changed");
static_assert(
    offsetof(vef_inferred_type_params_t, overflow) == 24,
    "ABI v2 break: vef_inferred_type_params_t::overflow offset changed");

// ---------------------------------------------------------------------------
// vef_vdf_args_t (protocol >= VEF_PROTOCOL_2 appends out_type_params)
//   void *user_data;                                  // +0   (8 bytes)
//   unsigned int value_count;                         // +8   (4 + 4 pad)
//   union { values_v1; values; };                     // +16  (8 bytes, ptr)
//   vef_inferred_type_params_t *out_type_params;      // +24  (8 bytes)
// ---------------------------------------------------------------------------
static_assert(sizeof(vef_vdf_args_t) == 32,
              "ABI v2 break: vef_vdf_args_t size changed");
static_assert(offsetof(vef_vdf_args_t, out_type_params) == 24,
              "ABI v2 break: vef_vdf_args_t::out_type_params offset changed");

// Placeholder test so the binary links and runs.
TEST(AbiV2Check, StaticAssertsPass) {}
