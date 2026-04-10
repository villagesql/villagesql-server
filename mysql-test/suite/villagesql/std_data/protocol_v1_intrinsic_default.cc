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

// Regression test extension for issue #202.
//
// Uses the raw C ABI (bypassing the SDK type_builder) to register a
// protocol-1 type that has intrinsic_default_str set to a non-null
// sentinel value.  A correct server must ignore intrinsic_default_str
// for protocol-1 types; the buggy code reads it and uses it as the
// type's intrinsic default.
//
// Observable difference via LOAD DATA IGNORE on a NOT NULL column:
//   Bug:   intrinsic default = encode("V1_BUG") = "V1_BUG**"
//   Fixed: intrinsic default = encode("")        = "********"

#include <cstddef>
#include <cstring>

#include <villagesql/abi/types.h>

static constexpr size_t kLen = 8;

static bool v1_encode(unsigned char *buf, size_t buf_size, const char *from,
                      size_t from_len, size_t *length) {
  if (buf_size < kLen) return true;
  memset(buf, '*', kLen);
  size_t copy_len = from_len < kLen ? from_len : kLen;
  if (from && copy_len > 0) memcpy(buf, from, copy_len);
  *length = kLen;
  return false;
}

static bool v1_decode(const unsigned char *buf, size_t buf_size, char *to,
                      size_t to_size, size_t *to_length) {
  if (to_size < kLen) return true;
  memcpy(to, buf, kLen);
  *to_length = kLen;
  return false;
}

static int v1_compare(const unsigned char *a, size_t a_len,
                      const unsigned char *b, size_t b_len) {
  return memcmp(a, b, kLen);
}

// The type descriptor.  Protocol is VEF_PROTOCOL_1 but we deliberately
// set intrinsic_default_str to a non-null sentinel.  A correct server
// must never read this field for protocol-1 types.
static vef_type_desc_t g_type_desc = {
    VEF_PROTOCOL_1,  // protocol
    "V1_REGTEST",    // name
    (int64_t)kLen,   // persisted_length
    (int64_t)kLen,   // max_decode_buffer_length
    v1_encode,       // encode_func
    v1_decode,       // decode_func
    v1_compare,      // compare_func
    nullptr,         // hash_func
    // Protocol-2 fields below.  For a real protocol-1 extension compiled
    // against an older SDK these bytes would be whatever was on the stack
    // or heap.  We simulate that by setting VDF name fields to nullptr
    // (benign) and intrinsic_default_str to a non-null sentinel (the bug
    // trigger).
    nullptr,   // encode_vdf_name
    nullptr,   // decode_vdf_name
    nullptr,   // compare_vdf_name
    nullptr,   // hash_vdf_name
    nullptr,   // int_to_params_vdf_name
    nullptr,   // resolve_params_vdf_name
    nullptr,   // intrinsic_default_vdf_name
    "V1_BUG",  // intrinsic_default_str  <-- the sentinel
    nullptr,   // storage_intf
};

static vef_type_desc_t *g_type_ptrs[] = {&g_type_desc};

static vef_registration_t g_reg;

extern "C" {

vef_registration_t *vef_register(vef_register_arg_t *arg) {
  memset(&g_reg, 0, sizeof(g_reg));
  g_reg.protocol = VEF_PROTOCOL_1;
  g_reg.error_msg = nullptr;
  g_reg.deprecated_extension_name = nullptr;
  g_reg.deprecated_extension_version = nullptr;
  g_reg.sdk_version = arg->vef_version;
  g_reg.func_count = 0;
  g_reg.funcs = nullptr;
  g_reg.type_count = 1;
  g_reg.types = g_type_ptrs;
  return &g_reg;
}

void vef_unregister(vef_unregister_arg_t *arg,
                    vef_registration_t *registration) {}

}  // extern "C"
