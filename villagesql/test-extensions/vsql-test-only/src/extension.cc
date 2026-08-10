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
//
//   NO_DEFAULT_VAR_PARAM_TYPE(N)
//              - Variable-length version of NO_DEFAULT_PARAM_TYPE (declared
//                .variable_length_type()). Same from_string that rejects the
//                empty string, so it has no intrinsic default; exercises the
//                fatal no-default path through the variable-length branch.
//
//   LARGE_DECODE_TYPE(N)
//              - Parameterized 8-byte type whose decoded string is N 'X'
//                characters.  Used to exercise the server's result-buffer
//                sizing for SQL-callable decode VDFs (e.g. T::to_string),
//                where the output size scales with the input column's
//                max_decode_buffer_length.
//
//   Parameter-validation edge-case types (see block comment further down):
//     BAD_LEN_PARAM_TYPE(N), EMPTY_PARAMS_TYPE(N), LENIENT_PARAM_TYPE(N),
//     STRICT_PARAM_TYPE(N), EXTRA_KEY_PARAM_TYPE(N).
//
//   Intrinsic-default test types (see block comment further down):
//     DEFAULT_STR_TYPE, DEFAULT_VDF_TYPE, DEFAULT_PARAM_TYPE(N),
//     BAD_DEFAULT_LEN_TYPE.
//
//   Parameter-bound test types (see block comment further down):
//     OVERSIZED_PARAM_TYPE(N).
//
//   Storage-size boundary test type (see block comment further down):
//     MAX_WIDTH_FIELD.
//
//   VAR_SETS_PERSISTED(N) - variable-length type whose resolve_params wrongly
//     reports a positive persisted_length (rejected at DDL time).
//   BAD_DECODE_BUF(N) - resolve_params resolves a non-positive
//     max_decode_buffer_length (rejected at DDL time).

#include <villagesql/vsql.h>

#include <cassert>
#include <cinttypes>
#include <cstddef>
#include <cstdint>
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

// resolve_params for the variable-length no-default type.
bool no_default_var_resolve_params(
    const std::map<std::string, std::string> &params,
    vsql::ResolvedTypeParams *result, char *error_msg) {
  auto it = params.find("length");
  if (it == params.end()) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "NO_DEFAULT_VAR_PARAM_TYPE length is required");
    return true;
  }
  int64_t length = std::stoll(it->second);
  if (length <= 0) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "NO_DEFAULT_VAR_PARAM_TYPE length must be positive");
    return true;
  }
  assert(result->persisted_length <= 0);
  result->max_decode_buffer_length = 16;
  return false;
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

// LARGE_DECODE_TYPE(length): parameterized 8-byte type whose decoded string
// is `length` copies of 'X'.  The persisted value is opaque; only the type
// parameter controls the decoded size.  Used to exercise the server's
// result-buffer sizing for STRING-returning VDFs (T::to_string), where the
// output can exceed the historical 256-byte default and must instead be
// sized from the input's max_decode_buffer_length.

constexpr int64_t kLargeDecodePersistedLen = 8;

struct LargeDecodeParams {
  int64_t length;

  static LargeDecodeParams parse(
      const std::map<std::string, std::string> &params) {
    auto it = params.find("length");
    return LargeDecodeParams{std::stoll(it->second)};
  }

  static void to_strings(const LargeDecodeParams &p,
                         std::map<std::string, std::string> &out) {
    out["length"] = std::to_string(p.length);
  }
};

bool large_decode_int_to_params(int64_t value,
                                std::map<std::string, std::string> &params,
                                char *error_msg) {
  if (value <= 0) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "LARGE_DECODE_TYPE length must be positive");
    return true;
  }
  params["length"] = std::to_string(value);
  return false;
}

bool large_decode_resolve_params(
    const std::map<std::string, std::string> &params,
    vsql::ResolvedTypeParams *result, char *error_msg) {
  auto it = params.find("length");
  if (it == params.end()) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "LARGE_DECODE_TYPE length is required");
    return true;
  }
  int64_t length = std::stoll(it->second);
  if (length <= 0) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "LARGE_DECODE_TYPE length must be positive");
    return true;
  }
  result->persisted_length = kLargeDecodePersistedLen;
  result->max_decode_buffer_length = length;
  return false;
}

void large_decode_encode(vsql::MaybeParams<LargeDecodeParams> &params,
                         std::string_view /*from*/, vsql::CustomResult out) {
  // Input is ignored; we just need a valid-sized blob so INSERT works.
  if (!params.is_known()) params.set(LargeDecodeParams{1});
  auto buf = out.buffer();
  if (buf.size() < static_cast<size_t>(kLargeDecodePersistedLen)) return;
  memset(buf.data(), 0, kLargeDecodePersistedLen);
  out.set_length(static_cast<size_t>(kLargeDecodePersistedLen));
}

void large_decode_decode(vsql::CustomArgWith<LargeDecodeParams> in,
                         vsql::StringResult out) {
  int64_t length = in.params().length;
  auto buf = out.buffer();
  if (length < 0 || buf.size() < static_cast<size_t>(length)) {
    // Wrapper-default failure surfaces: result buffer too small.
    return;
  }
  memset(buf.data(), 'X', static_cast<size_t>(length));
  out.set_length(static_cast<size_t>(length));
}

int large_decode_compare(vsql::CustomArgWith<LargeDecodeParams>,
                         vsql::CustomArgWith<LargeDecodeParams>) {
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
static constexpr const char kNoDefaultVarParamTypeName[] =
    "NO_DEFAULT_VAR_PARAM_TYPE";
static constexpr const char kLargeDecodeTypeName[] = "LARGE_DECODE_TYPE";

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

// A variable-length version of NO_DEFAULT_PARAM_TYPE: same parameterized
// int_to_params/from_string (from_string only accepts "(N)" and rejects the
// empty string), but declared .variable_length_type() so it is classified as
// variable-length. Exercises the fatal "no intrinsic default" path through the
// variable-length branch of TypeContext::init_intrinsic_default (a variable
// type that cannot encode a default is unusable, same as a fixed-length type).
constexpr auto NO_DEFAULT_VAR_PARAM_TYPE =
    vsql::make_type<kNoDefaultVarParamTypeName>()
        .variable_length_type()
        .max_decode_buffer_length(16)
        .max_persisted_length(kNoDefaultSize)
        .params<NoDefaultParams, &NoDefaultParams::parse,
                &NoDefaultParams::to_strings>()
        .int_to_params<&no_default_int_to_params>()
        .resolve_params<&no_default_var_resolve_params>()
        .from_string<&no_default_param_encode>()
        .to_string<&no_default_param_decode>()
        .compare<&no_default_param_compare>()
        .build();

constexpr auto LARGE_DECODE_TYPE =
    vsql::make_type<kLargeDecodeTypeName>()
        .persisted_length(-1)
        .max_decode_buffer_length(1)
        .max_persisted_length(kLargeDecodePersistedLen)
        .params<LargeDecodeParams, &LargeDecodeParams::parse,
                &LargeDecodeParams::to_strings>()
        .int_to_params<&large_decode_int_to_params>()
        .resolve_params<&large_decode_resolve_params>()
        .from_string<&large_decode_encode>()
        .to_string<&large_decode_decode>()
        .compare<&large_decode_compare>()
        .build();

// ---- PVEC type: parameterized vector of N int16 values ------------------
//
// Exercises parameterized types, varargs VDFs, and aggregate VDFs together.
//
// PVEC(N) stores N*2 bytes, little-endian int16. Text format: "[v1,...,vN]".
// int_sum_all(INT, ...) -> INT  sums non-NULL INT varargs.
// pvec_norm_sq(PVEC)    -> INT  aggregate: sum of element squares per group.

constexpr int64_t kPVecMaxDim = 256;
constexpr int64_t kPVecMaxPersistedLen = kPVecMaxDim * 2;

struct PVecParams {
  int64_t dimension;

  static PVecParams parse(const std::map<std::string, std::string> &params) {
    auto it = params.find("dimension");
    return PVecParams{std::stoll(it->second)};
  }

  static void to_strings(const PVecParams &p,
                         std::map<std::string, std::string> &out) {
    out["dimension"] = std::to_string(p.dimension);
  }
};

bool pvec_int_to_params(int64_t value,
                        std::map<std::string, std::string> &params, char *err) {
  if (value <= 0 || value > kPVecMaxDim) {
    snprintf(err, VEF_MAX_ERROR_LEN,
             "PVEC dimension must be 1..%" PRId64 ", got %" PRId64, kPVecMaxDim,
             value);
    return true;
  }
  params["dimension"] = std::to_string(value);
  return false;
}

bool pvec_resolve_params(const std::map<std::string, std::string> &params,
                         vsql::ResolvedTypeParams *result, char *err) {
  auto it = params.find("dimension");
  if (it == params.end()) {
    snprintf(err, VEF_MAX_ERROR_LEN, "PVEC requires dimension parameter");
    return true;
  }
  int64_t dim = std::stoll(it->second);
  if (dim <= 0 || dim > kPVecMaxDim) {
    snprintf(err, VEF_MAX_ERROR_LEN, "PVEC dimension must be 1..%" PRId64,
             kPVecMaxDim);
    return true;
  }
  result->persisted_length = dim * 2;
  result->max_decode_buffer_length = dim * 8;  // "-32768," max per int16
  return false;
}

static void store_i16(unsigned char *buf, int16_t v) {
  buf[0] = static_cast<unsigned char>(v & 0xFF);
  buf[1] = static_cast<unsigned char>((v >> 8) & 0xFF);
}

static int16_t load_i16(const unsigned char *buf) {
  return static_cast<int16_t>(static_cast<uint16_t>(buf[0]) |
                              (static_cast<uint16_t>(buf[1]) << 8));
}

void pvec_from_string(vsql::MaybeParams<PVecParams> &p, std::string_view from,
                      vsql::CustomResult out) {
  // Empty string → zero vector. The server calls from_string('') as an
  // intrinsic-default probe when initializing a column of this type.
  if (from.empty()) {
    if (!p.is_known()) {
      out.warning("PVEC: cannot infer dimension from empty string");
      return;
    }
    auto buf = out.buffer();
    size_t byte_len = static_cast<size_t>(p.value().dimension) * 2;
    memset(buf.data(), 0, byte_len);
    out.set_length(byte_len);
    return;
  }

  std::string input(from);
  const char *s = input.c_str();
  while (*s == ' ') s++;
  if (*s != '[') {
    out.warning("PVEC: expected '['");
    return;
  }
  s++;

  auto buf = out.buffer();
  const size_t max_elem = buf.size() / 2;
  size_t count = 0;

  while (*s != '\0') {
    while (*s == ' ') s++;
    if (*s == ']') break;
    if (count >= max_elem) {
      out.warning("PVEC: too many elements");
      return;
    }
    char *endptr = nullptr;
    long val = strtol(s, &endptr, 10);
    if (endptr == s) {
      out.warning("PVEC: parse error");
      return;
    }
    store_i16(buf.data() + count * 2, static_cast<int16_t>(val));
    count++;
    s = endptr;
    while (*s == ' ') s++;
    if (*s == ',') s++;
  }
  if (*s != ']') {
    out.warning("PVEC: missing ']'");
    return;
  }

  if (p.is_known()) {
    if (count != static_cast<size_t>(p.value().dimension)) {
      out.warning("PVEC: dimension mismatch");
      return;
    }
  } else {
    p.set(PVecParams{static_cast<int64_t>(count)});
  }
  out.set_length(count * 2);
}

void pvec_to_string(vsql::CustomArgWith<PVecParams> in,
                    vsql::StringResult out) {
  const PVecParams &p = in.params();
  auto data = in.value();
  auto buf = out.buffer();
  size_t pos = 0;
  if (pos >= buf.size()) return;
  buf[pos++] = '[';
  for (int64_t i = 0; i < p.dimension; i++) {
    if (i > 0) {
      if (pos >= buf.size()) return;
      buf[pos++] = ',';
    }
    int16_t v = load_i16(data.data() + i * 2);
    int written =
        snprintf(buf.data() + pos, buf.size() - pos, "%d", static_cast<int>(v));
    if (written < 0 || pos + static_cast<size_t>(written) >= buf.size()) return;
    pos += static_cast<size_t>(written);
  }
  if (pos >= buf.size()) return;
  buf[pos++] = ']';
  out.set_length(pos);
}

int pvec_compare(vsql::CustomArgWith<PVecParams> a,
                 vsql::CustomArgWith<PVecParams> b) {
  const PVecParams &pa = a.params();
  for (int64_t i = 0; i < pa.dimension; i++) {
    int16_t va = load_i16(a.value().data() + i * 2);
    int16_t vb = load_i16(b.value().data() + i * 2);
    if (va < vb) return -1;
    if (va > vb) return 1;
  }
  return 0;
}

void int_sum_all_prerun(vsql::PrerunArgs args, vsql::PrerunResult result) {
  if (args.size() == 0) {
    result.error("int_sum_all requires at least one argument");
    return;
  }
  for (size_t i = 0; i < args.size(); i++) {
    if (!args.type_at(i).is_int()) {
      result.error("int_sum_all: argument " + std::to_string(i) +
                   " must be INT");
      return;
    }
  }
}

void int_sum_all_impl(vsql::VarArgs args, vsql::IntResult out) {
  long long total = 0;
  for (auto a : args) {
    if (!a.is_null() && a.is_int()) total += a.as_int();
  }
  out.set(total);
}

using NormSqState = int64_t;

void pvec_norm_sq_clear(NormSqState &state) { state = 0; }

void pvec_norm_sq_accumulate(NormSqState &state,
                             vsql::CustomArgWith<PVecParams> v) {
  if (v.is_null()) return;
  const PVecParams &p = v.params();
  auto data = v.value();
  for (int64_t i = 0; i < p.dimension; i++) {
    int16_t elem = load_i16(data.data() + i * 2);
    state += static_cast<int64_t>(elem) * static_cast<int64_t>(elem);
  }
}

void pvec_norm_sq_result(const NormSqState &state, vsql::IntResult out) {
  out.set(state);
}

static constexpr const char kPVecTypeName[] = "PVEC";

constexpr auto PVEC =
    vsql::make_type<kPVecTypeName>()
        .persisted_length(-1)
        .max_decode_buffer_length(16)
        .max_persisted_length(kPVecMaxPersistedLen)
        .params<PVecParams, &PVecParams::parse, &PVecParams::to_strings>()
        .int_to_params<&pvec_int_to_params>()
        .resolve_params<&pvec_resolve_params>()
        .from_string<&pvec_from_string>()
        .to_string<&pvec_to_string>()
        .compare<&pvec_compare>()
        .build();

// ---- Parameter-validation edge-case types -------------------------------
//
// The following fixed-length parameterized types exist solely to drive the
// error/edge branches of PT_custom_type's parameter-resolution gates. They
// share a 4-byte storage layout and a single 'length' parameter; each varies
// only in how its int_to_params/resolve_params callbacks (mis)behave.
//
//   BAD_LEN_PARAM_TYPE(N)   - resolve_params returns persisted_length = -1 for
//                             a fixed-length type; every DDL use is rejected.
//   EMPTY_PARAMS_TYPE(N)    - int_to_params reports success but adds no params;
//                             the (N) form resolves to an empty parameter
//                             string.
//   LENIENT_PARAM_TYPE(N)   - resolve_params ignores unrecognized keys, so an
//                             extra key in the parameter string is accepted.
//   STRICT_PARAM_TYPE(N)    - resolve_params rejects any unrecognized key.
//   EXTRA_KEY_PARAM_TYPE(N) - int_to_params emits an extra key alongside the
//                             real one; used to check the (N) round-trip.

constexpr int64_t kParamTestSize = 4;

struct LenParam {
  int64_t length;

  static LenParam parse(const std::map<std::string, std::string> &params) {
    auto it = params.find("length");
    return LenParam{it == params.end() ? 0 : std::stoll(it->second)};
  }

  static void to_strings(const LenParam &p,
                         std::map<std::string, std::string> &out) {
    out["length"] = std::to_string(p.length);
  }
};

// Working 4-byte codec shared by the parameter-validation types. Text form is
// "(N)"; the empty string encodes to all-zeros so the types have a usable
// intrinsic default and their columns can be created.
void param_test_encode(vsql::MaybeParams<LenParam> &params,
                       std::string_view from, vsql::CustomResult out) {
  if (!params.is_known()) params.set(LenParam{kParamTestSize});
  auto buf = out.buffer();
  if (buf.size() < static_cast<size_t>(kParamTestSize)) return;
  memset(buf.data(), 0, kParamTestSize);
  unsigned int nn = 0;
  if (!from.empty()) {
    char tmp[64];
    size_t copy = from.size() < sizeof(tmp) - 1 ? from.size() : sizeof(tmp) - 1;
    memcpy(tmp, from.data(), copy);
    tmp[copy] = '\0';
    if (sscanf(tmp, "(%u)", &nn) != 1) return;  // encode failure
  }
  buf[0] = static_cast<unsigned char>(nn);
  out.set_length(static_cast<size_t>(kParamTestSize));
}

void param_test_decode(vsql::CustomArgWith<LenParam> in,
                       vsql::StringResult out) {
  auto data = in.value();
  if (data.size() < static_cast<size_t>(kParamTestSize)) return;
  auto buf = out.buffer();
  int written = snprintf(buf.data(), buf.size(), "(%u)", data[0]);
  if (written < 0) return;
  out.set_length(static_cast<size_t>(written));
}

int param_test_compare(vsql::CustomArgWith<LenParam>,
                       vsql::CustomArgWith<LenParam>) {
  return 0;
}

// BAD_LEN_PARAM_TYPE: resolve_params deliberately reports an invalid
// persisted_length (-1) for a fixed-length type.
bool bad_len_int_to_params(int64_t value,
                           std::map<std::string, std::string> &params, char *) {
  params["length"] = std::to_string(value);
  return false;
}

bool bad_len_resolve_params(const std::map<std::string, std::string> &,
                            vsql::ResolvedTypeParams *result, char *) {
  result->persisted_length = -1;  // invalid for a fixed-length type
  result->max_decode_buffer_length = 16;
  return false;
}

// EMPTY_PARAMS_TYPE: int_to_params succeeds but adds nothing to the map.
bool empty_int_to_params(int64_t, std::map<std::string, std::string> &,
                         char *) {
  return false;  // success, but params stays empty
}

bool empty_resolve_params(const std::map<std::string, std::string> &params,
                          vsql::ResolvedTypeParams *result, char *error_msg) {
  auto it = params.find("length");
  if (it == params.end()) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN, "EMPTY_PARAMS_TYPE requires length");
    return true;
  }
  result->persisted_length = kParamTestSize;
  result->max_decode_buffer_length = 16;
  return false;
}

// LENIENT_PARAM_TYPE: resolve_params reads only 'length' and silently ignores
// any other key.
bool lenient_int_to_params(int64_t value,
                           std::map<std::string, std::string> &params, char *) {
  params["length"] = std::to_string(value);
  return false;
}

bool lenient_resolve_params(const std::map<std::string, std::string> &params,
                            vsql::ResolvedTypeParams *result, char *error_msg) {
  auto it = params.find("length");
  if (it == params.end()) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "LENIENT_PARAM_TYPE requires length");
    return true;
  }
  result->persisted_length = kParamTestSize;
  result->max_decode_buffer_length = 16;
  return false;
}

// STRICT_PARAM_TYPE: resolve_params rejects any key other than 'length'.
bool strict_int_to_params(int64_t value,
                          std::map<std::string, std::string> &params, char *) {
  params["length"] = std::to_string(value);
  return false;
}

bool strict_resolve_params(const std::map<std::string, std::string> &params,
                           vsql::ResolvedTypeParams *result, char *error_msg) {
  for (const auto &kv : params) {
    if (kv.first != "length") {
      snprintf(error_msg, VEF_MAX_ERROR_LEN,
               "STRICT_PARAM_TYPE: unknown parameter '%s'", kv.first.c_str());
      return true;
    }
  }
  if (params.find("length") == params.end()) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN, "STRICT_PARAM_TYPE requires length");
    return true;
  }
  result->persisted_length = kParamTestSize;
  result->max_decode_buffer_length = 16;
  return false;
}

// EXTRA_KEY_PARAM_TYPE: int_to_params emits an extra 'bogus' key alongside the
// real 'length' key.
bool extra_key_int_to_params(int64_t value,
                             std::map<std::string, std::string> &params,
                             char *) {
  params["length"] = std::to_string(value);
  params["bogus"] = "1";
  return false;
}

bool extra_key_resolve_params(const std::map<std::string, std::string> &params,
                              vsql::ResolvedTypeParams *result,
                              char *error_msg) {
  auto it = params.find("length");
  if (it == params.end()) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "EXTRA_KEY_PARAM_TYPE requires length");
    return true;
  }
  result->persisted_length = kParamTestSize;
  result->max_decode_buffer_length = 16;
  return false;
}

static constexpr const char kBadLenParamTypeName[] = "BAD_LEN_PARAM_TYPE";
static constexpr const char kEmptyParamsTypeName[] = "EMPTY_PARAMS_TYPE";
static constexpr const char kLenientParamTypeName[] = "LENIENT_PARAM_TYPE";
static constexpr const char kStrictParamTypeName[] = "STRICT_PARAM_TYPE";
static constexpr const char kExtraKeyParamTypeName[] = "EXTRA_KEY_PARAM_TYPE";

constexpr auto BAD_LEN_PARAM_TYPE =
    vsql::make_type<kBadLenParamTypeName>()
        .persisted_length(-1)
        .max_decode_buffer_length(16)
        .max_persisted_length(kParamTestSize)
        .params<LenParam, &LenParam::parse, &LenParam::to_strings>()
        .int_to_params<&bad_len_int_to_params>()
        .resolve_params<&bad_len_resolve_params>()
        .from_string<&param_test_encode>()
        .to_string<&param_test_decode>()
        .compare<&param_test_compare>()
        .build();

constexpr auto EMPTY_PARAMS_TYPE =
    vsql::make_type<kEmptyParamsTypeName>()
        .persisted_length(-1)
        .max_decode_buffer_length(16)
        .max_persisted_length(kParamTestSize)
        .params<LenParam, &LenParam::parse, &LenParam::to_strings>()
        .int_to_params<&empty_int_to_params>()
        .resolve_params<&empty_resolve_params>()
        .from_string<&param_test_encode>()
        .to_string<&param_test_decode>()
        .compare<&param_test_compare>()
        .build();

constexpr auto LENIENT_PARAM_TYPE =
    vsql::make_type<kLenientParamTypeName>()
        .persisted_length(-1)
        .max_decode_buffer_length(16)
        .max_persisted_length(kParamTestSize)
        .params<LenParam, &LenParam::parse, &LenParam::to_strings>()
        .int_to_params<&lenient_int_to_params>()
        .resolve_params<&lenient_resolve_params>()
        .from_string<&param_test_encode>()
        .to_string<&param_test_decode>()
        .compare<&param_test_compare>()
        .build();

constexpr auto STRICT_PARAM_TYPE =
    vsql::make_type<kStrictParamTypeName>()
        .persisted_length(-1)
        .max_decode_buffer_length(16)
        .max_persisted_length(kParamTestSize)
        .params<LenParam, &LenParam::parse, &LenParam::to_strings>()
        .int_to_params<&strict_int_to_params>()
        .resolve_params<&strict_resolve_params>()
        .from_string<&param_test_encode>()
        .to_string<&param_test_decode>()
        .compare<&param_test_compare>()
        .build();

constexpr auto EXTRA_KEY_PARAM_TYPE =
    vsql::make_type<kExtraKeyParamTypeName>()
        .persisted_length(-1)
        .max_decode_buffer_length(16)
        .max_persisted_length(kParamTestSize)
        .params<LenParam, &LenParam::parse, &LenParam::to_strings>()
        .int_to_params<&extra_key_int_to_params>()
        .resolve_params<&extra_key_resolve_params>()
        .from_string<&param_test_encode>()
        .to_string<&param_test_decode>()
        .compare<&param_test_compare>()
        .build();

// ---- Intrinsic-default test types ---------------------------------------
//
//   DEFAULT_STR_TYPE     - fixed non-parameterized type with a valid
//                          intrinsic_default_str.
//   DEFAULT_VDF_TYPE     - fixed non-parameterized type whose default is
//                          computed by an intrinsic_default VDF.
//   DEFAULT_PARAM_TYPE(N)- parameterized type with an intrinsic_default_str,
//                          exercising per-TypeContext default pre-encoding.
//   BAD_DEFAULT_LEN_TYPE - intrinsic_default_str that encodes to the wrong
//                          length, hitting the length-mismatch error.

constexpr int64_t kDefaultTestSize = 4;

// Non-parameterized 4-byte codec: "(N)" text form; empty string -> zero.
void default_test_encode(std::string_view from, vsql::CustomResult out) {
  auto buf = out.buffer();
  if (buf.size() < static_cast<size_t>(kDefaultTestSize)) return;
  memset(buf.data(), 0, kDefaultTestSize);
  unsigned int nn = 0;
  if (!from.empty()) {
    char tmp[64];
    size_t copy = from.size() < sizeof(tmp) - 1 ? from.size() : sizeof(tmp) - 1;
    memcpy(tmp, from.data(), copy);
    tmp[copy] = '\0';
    if (sscanf(tmp, "(%u)", &nn) != 1) return;
  }
  buf[0] = static_cast<unsigned char>(nn);
  out.set_length(static_cast<size_t>(kDefaultTestSize));
}

void default_test_decode(vsql::CustomArg in, vsql::StringResult out) {
  auto data = in.value();
  if (data.size() < static_cast<size_t>(kDefaultTestSize)) return;
  auto buf = out.buffer();
  int written = snprintf(buf.data(), buf.size(), "(%u)", data[0]);
  if (written < 0) return;
  out.set_length(static_cast<size_t>(written));
}

int default_test_compare(vsql::CustomArg a, vsql::CustomArg b) {
  auto va = a.value();
  auto vb = b.value();
  size_t n = va.size() < vb.size() ? va.size() : vb.size();
  return memcmp(va.data(), vb.data(), n);
}

// BAD_DEFAULT_LEN_TYPE encode: the magic default string "SHORT" encodes to
// only 2 bytes, which does not match persisted_length (4).
void bad_default_len_encode(std::string_view from, vsql::CustomResult out) {
  auto buf = out.buffer();
  if (from == "SHORT") {
    if (buf.size() < 2) return;
    buf[0] = buf[1] = 0;
    out.set_length(2);  // wrong: expected persisted_length (4)
    return;
  }
  default_test_encode(from, out);
}

// intrinsic_default VDF for DEFAULT_VDF_TYPE: computes the text form "(9)".
std::string default_vdf_default(char * /*error_msg*/) { return "(9)"; }

static constexpr const char kDefaultStrTypeName[] = "DEFAULT_STR_TYPE";
static constexpr const char kDefaultVdfTypeName[] = "DEFAULT_VDF_TYPE";
static constexpr const char kDefaultParamTypeName[] = "DEFAULT_PARAM_TYPE";
static constexpr const char kBadDefaultLenTypeName[] = "BAD_DEFAULT_LEN_TYPE";

constexpr auto DEFAULT_STR_TYPE = vsql::make_type<kDefaultStrTypeName>()
                                      .persisted_length(kDefaultTestSize)
                                      .max_decode_buffer_length(16)
                                      .from_string<&default_test_encode>()
                                      .to_string<&default_test_decode>()
                                      .compare<&default_test_compare>()
                                      .intrinsic_default_str("(7)")
                                      .build();

constexpr auto DEFAULT_VDF_TYPE =
    vsql::make_type<kDefaultVdfTypeName>()
        .persisted_length(kDefaultTestSize)
        .max_decode_buffer_length(16)
        .from_string<&default_test_encode>()
        .to_string<&default_test_decode>()
        .compare<&default_test_compare>()
        .intrinsic_default_vdf("default_vdf_type_default")
        .build();

constexpr auto DEFAULT_PARAM_TYPE =
    vsql::make_type<kDefaultParamTypeName>()
        .persisted_length(-1)
        .max_decode_buffer_length(16)
        .max_persisted_length(kParamTestSize)
        .params<LenParam, &LenParam::parse, &LenParam::to_strings>()
        .int_to_params<&lenient_int_to_params>()
        .resolve_params<&lenient_resolve_params>()
        .from_string<&param_test_encode>()
        .to_string<&param_test_decode>()
        .compare<&param_test_compare>()
        .intrinsic_default_str("(5)")
        .build();

constexpr auto BAD_DEFAULT_LEN_TYPE =
    vsql::make_type<kBadDefaultLenTypeName>()
        .persisted_length(kDefaultTestSize)
        .max_decode_buffer_length(16)
        .from_string<&bad_default_len_encode>()
        .to_string<&default_test_decode>()
        .compare<&default_test_compare>()
        .intrinsic_default_str("SHORT")
        .build();

// ---- Parameter-bound test types ------------------------------------------
//
//   OVERSIZED_PARAM_TYPE(N) - resolve_params resolves a persisted_length larger
//                             than max_persisted_length; exercises the DDL-time
//                             upper-bound check.

// OVERSIZED_PARAM_TYPE: resolves to twice its max_persisted_length.
constexpr int64_t kOversizedResolved = kParamTestSize * 2;

bool oversized_int_to_params(int64_t value,
                             std::map<std::string, std::string> &params,
                             char *) {
  params["length"] = std::to_string(value);
  return false;
}

bool oversized_resolve_params(const std::map<std::string, std::string> &params,
                              vsql::ResolvedTypeParams *result,
                              char *error_msg) {
  if (params.find("length") == params.end()) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "OVERSIZED_PARAM_TYPE requires length");
    return true;
  }
  result->persisted_length =
      kOversizedResolved;  // exceeds max_persisted_length
  result->max_decode_buffer_length = 16;
  return false;
}

static constexpr const char kOversizedParamTypeName[] = "OVERSIZED_PARAM_TYPE";

constexpr auto OVERSIZED_PARAM_TYPE =
    vsql::make_type<kOversizedParamTypeName>()
        .persisted_length(-1)
        .max_decode_buffer_length(16)
        .max_persisted_length(kParamTestSize)
        .params<LenParam, &LenParam::parse, &LenParam::to_strings>()
        .int_to_params<&oversized_int_to_params>()
        .resolve_params<&oversized_resolve_params>()
        .from_string<&param_test_encode>()
        .to_string<&param_test_decode>()
        .compare<&param_test_compare>()
        .build();

// ---- Storage-size boundary test type -------------------------------------
//
// A custom column is backed by a VARBINARY field, so a type may declare at most
// 65532 bytes of storage per value.
//
// MAX_WIDTH_FIELD sits exactly on that boundary, pinning the check as "greater
// than" rather than "greater or equal": if the cap ever became exclusive, this
// extension would stop installing and every test using vsql_test_only would
// fail.
//
// None of this is custom-type specific -- a plain VARBINARY behaves identically
// at every width. 65532 is the widest that works whether or not the column is
// nullable, which is why the cap sits here rather than at 65533 (NOT NULL only)
// or 65535 (never usable).

constexpr int64_t kMaxWidthFieldLen = 65532;  // exactly the supported maximum

void max_width_encode(std::string_view /*from*/, vsql::CustomResult out) {
  auto buf = out.buffer();
  memset(buf.data(), 0, buf.size());
  out.set_length(buf.size());
}

static constexpr const char kMaxWidthFieldTypeName[] = "MAX_WIDTH_FIELD";

constexpr auto MAX_WIDTH_FIELD = vsql::make_type<kMaxWidthFieldTypeName>()
                                     .persisted_length(kMaxWidthFieldLen)
                                     .max_decode_buffer_length(16)
                                     .from_string<&max_width_encode>()
                                     .to_string<&default_test_decode>()
                                     .compare<&default_test_compare>()
                                     .build();

// VAR_SETS_PERSISTED: a variable-length type whose resolve_params wrongly
// reports a positive persisted_length.
bool var_sets_int_to_params(int64_t value,
                            std::map<std::string, std::string> &params,
                            char *) {
  params["length"] = std::to_string(value);
  return false;
}

bool var_sets_resolve_params(const std::map<std::string, std::string> &params,
                             vsql::ResolvedTypeParams *result,
                             char *error_msg) {
  if (params.find("length") == params.end()) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "VAR_SETS_PERSISTED requires length");
    return true;
  }
  result->persisted_length = kParamTestSize;  // wrong: must stay <= 0 here
  result->max_decode_buffer_length = 16;
  return false;
}

// BAD_DECODE_BUF: resolve_params resolves a valid persisted_length but a
// non-positive max_decode_buffer_length, which must be > 0. Exercises the
// post-resolve_params max_decode_buffer_length check.
bool bad_decode_buf_int_to_params(int64_t value,
                                  std::map<std::string, std::string> &params,
                                  char *) {
  params["length"] = std::to_string(value);
  return false;
}

bool bad_decode_buf_resolve_params(
    const std::map<std::string, std::string> &params,
    vsql::ResolvedTypeParams *result, char *error_msg) {
  if (params.find("length") == params.end()) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN, "BAD_DECODE_BUF requires length");
    return true;
  }
  result->persisted_length = kParamTestSize;  // valid
  result->max_decode_buffer_length = 0;       // invalid: must be > 0
  return false;
}

static constexpr const char kVarSetsPersistedTypeName[] = "VAR_SETS_PERSISTED";
static constexpr const char kBadDecodeBufTypeName[] = "BAD_DECODE_BUF";

constexpr auto VAR_SETS_PERSISTED =
    vsql::make_type<kVarSetsPersistedTypeName>()
        .variable_length_type()
        .max_persisted_length(kParamTestSize)
        .max_decode_buffer_length(16)
        .params<LenParam, &LenParam::parse, &LenParam::to_strings>()
        .int_to_params<&var_sets_int_to_params>()
        .resolve_params<&var_sets_resolve_params>()
        .from_string<&param_test_encode>()
        .to_string<&param_test_decode>()
        .compare<&param_test_compare>()
        .build();

constexpr auto BAD_DECODE_BUF =
    vsql::make_type<kBadDecodeBufTypeName>()
        .persisted_length(-1)
        .max_decode_buffer_length(16)
        .max_persisted_length(kParamTestSize)
        .params<LenParam, &LenParam::parse, &LenParam::to_strings>()
        .int_to_params<&bad_decode_buf_int_to_params>()
        .resolve_params<&bad_decode_buf_resolve_params>()
        .from_string<&param_test_encode>()
        .to_string<&param_test_decode>()
        .compare<&param_test_compare>()
        .build();

using namespace vsql;

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        // FAULT_BLOB type: behaviour controlled by
        // "OK"/"DECODE_FAIL"/"ENCODE_FAIL" prefix
        .type(FAULT_BLOB)
        .type(NO_DEFAULT_TYPE)
        .type(NO_DEFAULT_PARAM_TYPE)
        .type(NO_DEFAULT_VAR_PARAM_TYPE)
        .type(LARGE_DECODE_TYPE)
        .type(PVEC)
        // Parameter-validation edge-case types
        .type(BAD_LEN_PARAM_TYPE)
        .type(EMPTY_PARAMS_TYPE)
        .type(LENIENT_PARAM_TYPE)
        .type(STRICT_PARAM_TYPE)
        .type(EXTRA_KEY_PARAM_TYPE)
        // Intrinsic-default test types
        .type(DEFAULT_STR_TYPE)
        .type(DEFAULT_VDF_TYPE)
        .type(DEFAULT_PARAM_TYPE)
        .type(BAD_DEFAULT_LEN_TYPE)
        // Parameter-bound test types
        .type(OVERSIZED_PARAM_TYPE)
        // Storage-size boundary test type
        .type(MAX_WIDTH_FIELD)
        .type(VAR_SETS_PERSISTED)
        .type(BAD_DECODE_BUF)
        .func(make_intrinsic_default<&default_vdf_default>(
            "default_vdf_type_default"))
        // Test VDF: exercises VEF_RESULT_WARNING vs VEF_RESULT_ERROR
        .func(make_func<&test_result_kind>("test_result_kind")
                  .returns(INT)
                  .param(STRING)
                  .build())
        .func(make_func<&int_sum_all_impl>("int_sum_all")
                  .returns(INT)
                  .varargs()
                  .prerun<&int_sum_all_prerun>()
                  .build())
        .func(make_aggregate_func<NormSqState, &pvec_norm_sq_result>(
                  "pvec_norm_sq")
                  .returns(INT)
                  .param(PVEC)
                  .clear<&pvec_norm_sq_clear>()
                  .accumulate<&pvec_norm_sq_accumulate>()
                  .build()))
