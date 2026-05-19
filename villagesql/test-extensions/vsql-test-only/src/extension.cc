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
//
//   NO_DEFAULT_TYPE
//              - A fixed 4-byte type with no intrinsic default. Its
//                from_string accepts only "(N)" and rejects empty string.
//
//   NO_DEFAULT_PARAM_TYPE(N)
//              - Parameterized version of NO_DEFAULT_TYPE for exercising
//                parameterized intrinsic-default failures.

#include <villagesql/vsql.h>

#include <cassert>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <string>
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

void fault_blob_encode(std::string_view from, vsql::CustomResult out) {
  if (from == kEncodeFail) {
    return;  // encode failure - wrapper default warning surfaces
  }

  auto buf = out.buffer();
  if (from.substr(0, kPrefixOk.size()) == kPrefixOk || from == kDecodeFail) {
    if (from.size() > buf.size()) return;  // input too long
    memset(buf.data(), 0, buf.size());
    memcpy(buf.data(), from.data(), from.size());
    out.set_length(kFaultBlobSize);
    return;
  }

  // Empty string encodes as all-zeros (used as intrinsic default).
  if (from.empty()) {
    memset(buf.data(), 0, buf.size());
    out.set_length(kFaultBlobSize);
    return;
  }

  // Unrecognised prefix - crash loudly so tests cannot silently pass bad input.
  assert(false &&
         "fault_blob_encode: unrecognised prefix (expected 'OK ', "
         "'DECODE_FAIL', or 'ENCODE_FAIL')");
  abort();
}

void fault_blob_decode(vsql::CustomArg in, vsql::StringResult out) {
  auto data = in.value();
  if (data.size() < static_cast<size_t>(kFaultBlobSize)) {
    return;  // error: truncated value (wrapper default ERROR)
  }

  // Reconstruct the string by stripping trailing zero padding.
  size_t len = kFaultBlobSize;
  while (len > 0 && data[len - 1] == 0) --len;

  std::string_view stored(reinterpret_cast<const char *>(data.data()), len);

  if (stored == kDecodeFail) {
    return;  // decode failure - this is the path under test
  }

  auto buf = out.buffer();
  if (buf.size() < len) {
    return;  // error: output buffer too small
  }

  memcpy(buf.data(), data.data(), len);
  out.set_length(len);
}

int fault_blob_compare(vsql::CustomArg a, vsql::CustomArg b) {
  auto va = a.value();
  auto vb = b.value();
  size_t cmp_len = va.size() < vb.size() ? va.size() : vb.size();
  int r = memcmp(va.data(), vb.data(), cmp_len);
  if (r != 0) return r;
  if (va.size() < vb.size()) return -1;
  if (va.size() > vb.size()) return 1;
  return 0;
}

constexpr int64_t kNoDefaultSize = 4;

void no_default_encode(std::string_view from, vsql::CustomResult out) {
  unsigned int nn = 0;
  char tmp[64];
  size_t copy = from.size() < sizeof(tmp) - 1 ? from.size() : sizeof(tmp) - 1;
  memcpy(tmp, from.data(), copy);
  tmp[copy] = '\0';
  if (sscanf(tmp, "(%u)", &nn) != 1) return;
  auto buffer = out.buffer();
  if (buffer.size() < static_cast<size_t>(kNoDefaultSize)) return;
  buffer[0] = static_cast<unsigned char>(nn);
  buffer[1] = buffer[2] = buffer[3] = 0;
  out.set_length(static_cast<size_t>(kNoDefaultSize));
}

void no_default_decode(vsql::CustomArg in, vsql::StringResult out) {
  auto buffer = in.value();
  if (buffer.size() < static_cast<size_t>(kNoDefaultSize)) return;
  auto buf = out.buffer();
  int written = snprintf(buf.data(), buf.size(), "(%u)", buffer[0]);
  if (written < 0) return;
  out.set_length(static_cast<size_t>(written));
}

int no_default_compare(vsql::CustomArg, vsql::CustomArg) { return 0; }

struct NoDefaultParams {
  int64_t length;

  static NoDefaultParams parse(
      const std::map<std::string, std::string> &params) {
    auto it = params.find("length");
    return NoDefaultParams{std::stoll(it->second)};
  }

  static void to_strings(const NoDefaultParams &p,
                         std::map<std::string, std::string> &out) {
    out["length"] = std::to_string(p.length);
  }
};

bool no_default_int_to_params(int64_t value,
                              std::map<std::string, std::string> &params,
                              char *error_msg) {
  if (value <= 0) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "NO_DEFAULT_PARAM_TYPE length must be positive");
    return true;
  }
  params["length"] = std::to_string(value);
  return false;
}

bool no_default_resolve_params(const std::map<std::string, std::string> &params,
                               vsql::ResolvedTypeParams *result,
                               char *error_msg) {
  auto it = params.find("length");
  if (it == params.end()) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "NO_DEFAULT_PARAM_TYPE length is required");
    return true;
  }
  int64_t length = std::stoll(it->second);
  if (length <= 0) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "NO_DEFAULT_PARAM_TYPE length must be positive");
    return true;
  }
  result->persisted_length = length;
  result->max_decode_buffer_length = 16;
  return false;
}

void no_default_param_encode(vsql::MaybeParams<NoDefaultParams> &params,
                             std::string_view from, vsql::CustomResult out) {
  if (!params.is_known()) params.set(NoDefaultParams{kNoDefaultSize});
  no_default_encode(from, out);
}

void no_default_param_decode(vsql::CustomArgWith<NoDefaultParams> in,
                             vsql::StringResult out) {
  auto buffer = in.value();
  if (buffer.size() < static_cast<size_t>(kNoDefaultSize)) return;
  auto buf = out.buffer();
  int written = snprintf(buf.data(), buf.size(), "(%u)", buffer[0]);
  if (written < 0) return;
  out.set_length(static_cast<size_t>(written));
}

int no_default_param_compare(vsql::CustomArgWith<NoDefaultParams>,
                             vsql::CustomArgWith<NoDefaultParams>) {
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
static constexpr const char kNoDefaultTypeName[] = "NO_DEFAULT_TYPE";
static constexpr const char kNoDefaultParamTypeName[] = "NO_DEFAULT_PARAM_TYPE";

constexpr auto FAULT_BLOB = vsql::make_type<kFaultBlobTypeName>()
                                .persisted_length(kFaultBlobSize)
                                .max_decode_buffer_length(kFaultBlobSize)
                                .from_string<&fault_blob_encode>()
                                .to_string<&fault_blob_decode>()
                                .compare<&fault_blob_compare>()
                                .build();

constexpr auto NO_DEFAULT_TYPE = vsql::make_type<kNoDefaultTypeName>()
                                     .persisted_length(kNoDefaultSize)
                                     .max_decode_buffer_length(16)
                                     .from_string<&no_default_encode>()
                                     .to_string<&no_default_decode>()
                                     .compare<&no_default_compare>()
                                     .build();

constexpr auto NO_DEFAULT_PARAM_TYPE =
    vsql::make_type<kNoDefaultParamTypeName>()
        .persisted_length(-1)
        .max_decode_buffer_length(16)
        .max_persisted_length(kNoDefaultSize)
        .params<NoDefaultParams, &NoDefaultParams::parse,
                &NoDefaultParams::to_strings>()
        .int_to_params<&no_default_int_to_params>()
        .resolve_params<&no_default_resolve_params>()
        .from_string<&no_default_param_encode>()
        .to_string<&no_default_param_decode>()
        .compare<&no_default_param_compare>()
        .build();

using namespace vsql;

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        // FAULT_BLOB type: behaviour controlled by
        // "OK"/"DECODE_FAIL"/"ENCODE_FAIL" prefix
        .type(FAULT_BLOB)
        .type(NO_DEFAULT_TYPE)
        .type(NO_DEFAULT_PARAM_TYPE)
        // Test VDF: exercises VEF_RESULT_WARNING vs VEF_RESULT_ERROR
        .func(make_func<&test_result_kind>("test_result_kind")
                  .returns(INT)
                  .param(STRING)
                  .build()))
