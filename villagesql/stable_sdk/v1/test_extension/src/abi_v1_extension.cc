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

// ABI v1 Test Extension
//
// Compiled against the stable_sdk/v1/ headers to verify that extensions built
// against v1 headers continue to load and run against the current server.
//
// Provides:
//   BYTEARRAY type (8-byte fixed-length binary)
//   from_string(STRING) -> BYTEARRAY
//   to_string(BYTEARRAY) -> STRING
//   mask(BYTEARRAY, INT) -> BYTEARRAY

#include <cstdio>
#include <cstring>

#include <villagesql/extension.h>

// BYTEARRAY: 8-byte fixed-length binary, space-padded on the right.

static const size_t kBytearrayLen = 8;
static constexpr const char *kBytearrayType = "bytearray";

static bool bytearray_encode(unsigned char *buf, size_t buf_size,
                             const char *from, size_t from_len,
                             size_t *length) {
  if (buf_size < kBytearrayLen) return true;
  memset(buf, ' ', kBytearrayLen);
  size_t copy_len = from_len < kBytearrayLen ? from_len : kBytearrayLen;
  if (from && copy_len > 0) memcpy(buf, from, copy_len);
  *length = kBytearrayLen;
  return false;
}

static bool bytearray_decode(const unsigned char *buf, size_t buf_size,
                             char *to, size_t to_size, size_t *to_length) {
  if (to_size < kBytearrayLen) return true;
  memcpy(to, buf, kBytearrayLen);
  *to_length = kBytearrayLen;
  return false;
}

static int bytearray_compare(const unsigned char *a, size_t,
                             const unsigned char *b, size_t) {
  return memcmp(a, b, kBytearrayLen);
}

// mask(BYTEARRAY, INT) -> BYTEARRAY: replaces the byte at offset n with '*'.
// Out-of-range offsets return an error.
static void mask_impl(vef_context_t *, vef_invalue_t *input,
                      vef_invalue_t *offset, vef_vdf_result_t *out) {
  if (input->is_null || offset->is_null) {
    out->type = VEF_RESULT_NULL;
    return;
  }
  long long n = offset->int_value;
  if (n < 0 || static_cast<size_t>(n) >= kBytearrayLen) {
    snprintf(out->error_msg, VEF_MAX_ERROR_LEN,
             "offset %lld is out of range [0, %zu]", n, kBytearrayLen - 1);
    out->type = VEF_RESULT_ERROR;
    return;
  }
  memcpy(out->bin_buf, input->bin_value, kBytearrayLen);
  out->bin_buf[n] = '*';
  out->type = VEF_RESULT_VALUE;
  out->actual_len = kBytearrayLen;
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension("abi_v1_test", "0.0.1")
        .type(make_type(kBytearrayType)
                  .persisted_length(kBytearrayLen)
                  .max_decode_buffer_length(kBytearrayLen)
                  .encode(&bytearray_encode)
                  .decode(&bytearray_decode)
                  .compare(&bytearray_compare)
                  .build())
        .func(make_func("from_string")
                  .from_string<&bytearray_encode>(kBytearrayType))
        .func(
            make_func("to_string").to_string<&bytearray_decode>(kBytearrayType))
        .func(make_func<&mask_impl>("mask")
                  .returns(kBytearrayType)
                  .param(kBytearrayType)
                  .param(INT)
                  .build()))
