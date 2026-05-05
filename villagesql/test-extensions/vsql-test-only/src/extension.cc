// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is designed to work with certain software (including
// but not limited to OpenSSL) that is licensed under separate terms,
// as designated in a particular file or component or in included license
// documentation.  The authors of MySQL hereby grant you an additional
// permission to link the program and your derivative works with the
// separately licensed software that they have either included with
// the program or referenced in the documentation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

// vsql_test_only extension: internal test types for exercising error/edge-case
// code paths that are difficult to reach with normal data.
//
// NOTE: This is an internal testing tool, not an example of how to write a
// VillageSQL extension.  For guidance on writing extensions, see the examples
// under villagesql/examples/ and the SDK documentation.
//
// Types provided:
//
//   FAULT_BLOB  - A fixed 16-byte type whose behaviour is controlled by a
//                 magic prefix in the input string:
//
//                   "OK <payload>"      Encodes and decodes successfully.
//                                       The full string is stored and returned
//                                       verbatim on decode.
//
//                   "DECODE_FAIL"       Encodes successfully (the string is
//                                       stored as-is), but decode detects the
//                                       magic string at read time and returns
//                                       an error.  Exercises the server's
//                                       decode-failure path.
//
//                   "ENCODE_FAIL"       Encode returns an error immediately,
//                                       so the value is never stored.
//                                       Exercises the server's encode-failure
//                                       path.
//
//                 Any other input is rejected by encode with an assert-style
//                 error, so tests cannot silently pass unexpected values.

#include <villagesql/vsql.h>

#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <string_view>

// Storage layout: 16 bytes, zero-padded.
// The input string is stored verbatim (up to 16 bytes); trailing zero bytes
// are stripped on decode to reconstruct the original string.  Decode then
// checks the reconstructed string against the known sentinels to determine
// whether to succeed or fail.

constexpr int64_t kFaultBlobSize = 16;

constexpr std::string_view kPrefixOk = "OK ";
constexpr std::string_view kDecodeFail = "DECODE_FAIL";
constexpr std::string_view kEncodeFail = "ENCODE_FAIL";

bool fault_blob_encode(std::string_view from, vsql::Span<unsigned char> buf,
                       size_t *length) {
  if (from == kEncodeFail) {
    return true;  // encode failure - exercises server encode-error path
  }

  if (from.substr(0, kPrefixOk.size()) == kPrefixOk || from == kDecodeFail) {
    if (from.size() > buf.size()) {
      return true;  // error: input too long
    }
    memset(buf.data(), 0, buf.size());
    memcpy(buf.data(), from.data(), from.size());
    *length = kFaultBlobSize;
    return false;  // success
  }

  // Empty string encodes as all-zeros (used as intrinsic default).
  if (from.empty()) {
    memset(buf.data(), 0, buf.size());
    *length = kFaultBlobSize;
    return false;
  }

  // Unrecognised prefix - crash loudly so tests cannot silently pass bad input.
  assert(false &&
         "fault_blob_encode: unrecognised prefix (expected 'OK ', "
         "'DECODE_FAIL', or 'ENCODE_FAIL')");
  abort();
}

bool fault_blob_decode(vsql::Span<const unsigned char> data,
                       vsql::Span<char> out, size_t *out_len) {
  if (data.size() < static_cast<size_t>(kFaultBlobSize)) {
    return true;  // error: truncated value
  }

  // Reconstruct the string by stripping trailing zero padding.
  size_t len = kFaultBlobSize;
  while (len > 0 && data[len - 1] == 0) --len;

  std::string_view stored(reinterpret_cast<const char *>(data.data()), len);

  if (stored == kDecodeFail) {
    return true;  // decode failure - this is the path under test
  }

  if (out.size() < len) {
    return true;  // error: output buffer too small
  }

  memcpy(out.data(), data.data(), len);
  *out_len = len;
  return false;  // success
}

int fault_blob_compare(vsql::Span<const unsigned char> a,
                       vsql::Span<const unsigned char> b) {
  size_t cmp_len = a.size() < b.size() ? a.size() : b.size();
  int r = memcmp(a.data(), b.data(), cmp_len);
  if (r != 0) return r;
  if (a.size() < b.size()) return -1;
  if (a.size() > b.size()) return 1;
  return 0;
}

// test_result_kind: exercises VEF_RESULT_WARNING vs VEF_RESULT_ERROR.
//
// Input string controls the outcome:
//
//   "WARN <msg>"   Returns VEF_RESULT_WARNING with <msg>: NULL is returned for
//                  this row, a warning is added, and execution continues.
//
//   "ERROR <msg>"  Returns VEF_RESULT_ERROR with <msg>: the statement is
//                  aborted immediately.
//
//   anything else  Returns 42 (success).
//
static void test_result_kind(vsql::StringArg input, vsql::IntResult out) {
  if (input.is_null()) {
    out.set_null();
    return;
  }
  auto sv = input.value();
  constexpr std::string_view kWarnPrefix = "WARN ";
  constexpr std::string_view kErrorPrefix = "ERROR ";
  if (sv.size() > kWarnPrefix.size() &&
      sv.substr(0, kWarnPrefix.size()) == kWarnPrefix) {
    out.warning(sv.substr(kWarnPrefix.size()));
    return;
  }
  if (sv.size() > kErrorPrefix.size() &&
      sv.substr(0, kErrorPrefix.size()) == kErrorPrefix) {
    out.error(sv.substr(kErrorPrefix.size()));
    return;
  }
  out.set(42);
}

static constexpr const char kFaultBlobTypeName[] = "FAULT_BLOB";

constexpr auto FAULT_BLOB = vsql::make_type<kFaultBlobTypeName>()
                                .persisted_length(kFaultBlobSize)
                                .max_decode_buffer_length(kFaultBlobSize)
                                .from_string<&fault_blob_encode>()
                                .to_string<&fault_blob_decode>()
                                .compare<&fault_blob_compare>()
                                .build();

using namespace vsql;

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        // FAULT_BLOB type: behaviour controlled by
        // "OK"/"DECODE_FAIL"/"ENCODE_FAIL" prefix
        .type(FAULT_BLOB)
        // Test VDF: exercises VEF_RESULT_WARNING vs VEF_RESULT_ERROR
        .func(make_func<&test_result_kind>("test_result_kind")
                  .returns(INT)
                  .param(STRING)
                  .build()))
