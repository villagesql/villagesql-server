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

// VillageSQL TVECTOR extension demonstrating parameterized custom types.
//
// TVECTOR supports two element types via the "type" parameter:
//   TVECTOR(N)                          -- N float32 elements (default)
//   TVECTOR('dimension=N,type=float')   -- same as above
//   TVECTOR('dimension=N,type=double')  -- N float64 elements
//
// TVECTOR(3) with float stores 12 bytes (3 * 4). With double, 24 bytes (3 * 8).
// Text format: "[1.0,2.0,3.0]" (comma-separated values in brackets)

#include <villagesql/vsql.h>

#include <cinttypes>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <map>
#include <string>
#include <string_view>

// Largest dimension a TVECTOR may declare. Enforced in resolve_params and
// int_to_params; also drives kTVectorMaxPersistedLength below for the
// constant-string inference path.
constexpr int64_t kTVectorMaxDimension = 4096;

// Parsed representation of TVECTOR type parameters.
// The static parse() method is used automatically by make_type_encode,
// make_type_decode, and make_intrinsic_default when the operation function
// takes const TVectorParams& as its first parameter.
struct TVectorParams {
  int64_t dimension;
  size_t bytes_per_elem;  // 4 for float, 8 for double

  static TVectorParams parse(const std::map<std::string, std::string> &params) {
    auto dim_it = params.find("dimension");
    int64_t dim = strtoll(dim_it->second.c_str(), nullptr, 10);
    size_t bytes = 4;
    auto type_it = params.find("type");
    if (type_it != params.end() && type_it->second == "double") bytes = 8;
    return TVectorParams{.dimension = dim, .bytes_per_elem = bytes};
  }

  // Inverse of parse: render a typed TVectorParams back into the canonical
  // key/value string form. Used by the SDK when the server needs to publish
  // inferred params (e.g., from a constant-string from_string) back in the
  // same shape that parse() consumes.
  static void to_strings(const TVectorParams &p,
                         std::map<std::string, std::string> &out) {
    out["dimension"] = std::to_string(p.dimension);
    out["type"] = (p.bytes_per_elem == 8) ? "double" : "float";
  }
};

// Little-endian store/load helpers.

void store_float(unsigned char *buf, float val) {
  uint32_t bits;
  memcpy(&bits, &val, sizeof(float));
  buf[0] = static_cast<unsigned char>(bits);
  buf[1] = static_cast<unsigned char>(bits >> 8);
  buf[2] = static_cast<unsigned char>(bits >> 16);
  buf[3] = static_cast<unsigned char>(bits >> 24);
}

float load_float(const unsigned char *buf) {
  uint32_t bits = static_cast<uint32_t>(buf[0]) |
                  (static_cast<uint32_t>(buf[1]) << 8) |
                  (static_cast<uint32_t>(buf[2]) << 16) |
                  (static_cast<uint32_t>(buf[3]) << 24);
  float val;
  memcpy(&val, &bits, sizeof(float));
  return val;
}

void store_double(unsigned char *buf, double val) {
  uint64_t bits;
  memcpy(&bits, &val, sizeof(double));
  buf[0] = static_cast<unsigned char>(bits);
  buf[1] = static_cast<unsigned char>(bits >> 8);
  buf[2] = static_cast<unsigned char>(bits >> 16);
  buf[3] = static_cast<unsigned char>(bits >> 24);
  buf[4] = static_cast<unsigned char>(bits >> 32);
  buf[5] = static_cast<unsigned char>(bits >> 40);
  buf[6] = static_cast<unsigned char>(bits >> 48);
  buf[7] = static_cast<unsigned char>(bits >> 56);
}

double load_double(const unsigned char *buf) {
  uint64_t bits = static_cast<uint64_t>(buf[0]) |
                  (static_cast<uint64_t>(buf[1]) << 8) |
                  (static_cast<uint64_t>(buf[2]) << 16) |
                  (static_cast<uint64_t>(buf[3]) << 24) |
                  (static_cast<uint64_t>(buf[4]) << 32) |
                  (static_cast<uint64_t>(buf[5]) << 40) |
                  (static_cast<uint64_t>(buf[6]) << 48) |
                  (static_cast<uint64_t>(buf[7]) << 56);
  double val;
  memcpy(&val, &bits, sizeof(double));
  return val;
}

// Returns bytes per element based on the "type" parameter.
// Absent or "float" -> 4, "double" -> 8.
size_t bytes_per_element(const std::map<std::string, std::string> &params) {
  auto it = params.find("type");
  if (it != params.end() && it->second == "double") return 8;
  return 4;
}

// Convert TYPE(N) integer to parameter key-value pairs.
// Populates params map with {"dimension": "<N>", "type": "float"}.
// Setting type explicitly ensures TVECTOR(3) produces the same canonical
// params as TVECTOR('dimension=3,type=float').
bool tvector_int_to_params(int64_t value,
                           std::map<std::string, std::string> &params,
                           char *error_msg) {
  if (value <= 0 || value > kTVectorMaxDimension) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "TVECTOR dimension must be in 1..%" PRId64 ", got %" PRId64,
             kTVectorMaxDimension, value);
    return true;
  }
  params["dimension"] = std::to_string(value);
  params["type"] = "float";
  return false;
}

// Validate type parameters and compute storage characteristics.
bool tvector_resolve_params(const std::map<std::string, std::string> &params,
                            vsql::ResolvedTypeParams *result, char *error_msg) {
  auto it = params.find("dimension");
  if (it == params.end()) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "TVECTOR: dimension must be a positive integer");
    return true;
  }
  char *endptr = nullptr;
  int64_t dimension = strtoll(it->second.c_str(), &endptr, 10);
  if (endptr == it->second.c_str() || *endptr != '\0') {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "TVECTOR: invalid dimension value '%s'", it->second.c_str());
    return true;
  }
  if (dimension <= 0 || dimension > kTVectorMaxDimension) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "TVECTOR: dimension must be in 1..%" PRId64 ", got %" PRId64,
             kTVectorMaxDimension, dimension);
    return true;
  }

  // Validate "type" parameter if present.
  auto type_it = params.find("type");
  if (type_it != params.end() && type_it->second != "float" &&
      type_it->second != "double") {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "TVECTOR: type must be 'float' or 'double', got '%s'",
             type_it->second.c_str());
    return true;
  }

  size_t bpe = bytes_per_element(params);
  result->persisted_length = dimension * static_cast<int64_t>(bpe);
  // %.17g for double can produce up to ~24 chars; use 32 per element to be safe
  result->max_decode_buffer_length = dimension * (bpe == 8 ? 32 : 16);
  return false;
}

// When p is known, dimension and element type are taken from p; the parsed
// element count must match p.dimension. When p is unknown, the element count
// from the string is used to set p.dimension, and bytes_per_elem defaults to 4
// (float) since the string itself does not disambiguate float from double.
//
// Single-pass: each parsed element is written directly into out.buffer().
// The loop only checks against max_supportable (what the buffer can hold).
// Mismatch with the expected dimension is checked once at the end.
void tvector_from_string(vsql::MaybeParams<TVectorParams> &p,
                         std::string_view from, vsql::CustomResult out) {
  // strtof/strtod require a null-terminated string.
  std::string input(from);
  const char *s = input.c_str();
  while (*s == ' ') s++;
  if (*s != '[') {
    out.warning("tvector_from_string: missing '['");
    return;
  }
  s++;

  auto buf = out.buffer();
  // bpe is fixed if known; defaults to 4 (float) when inferring.
  const size_t bpe = (p.is_known() && p.value().bytes_per_elem > 0)
                         ? p.value().bytes_per_elem
                         : 4;
  // Cap the loop on what the output buffer can hold; expected-dimension
  // mismatch is reported once at the end.
  const size_t max_supportable = buf.size() / bpe;

  size_t count = 0;
  while (*s != '\0') {
    while (*s == ' ') s++;
    if (*s == ']') break;

    if (count >= max_supportable) {
      out.warning("tvector_from_string: buffer too small");
      return;
    }

    char *endptr = nullptr;
    if (bpe == 8) {
      double val = strtod(s, &endptr);
      if (endptr == s) {
        out.warning("tvector_from_string: parse error");
        return;
      }
      store_double(buf.data() + count * 8, val);
    } else {
      float val = strtof(s, &endptr);
      if (endptr == s) {
        out.warning("tvector_from_string: parse error");
        return;
      }
      store_float(buf.data() + count * 4, val);
    }
    count++;
    s = endptr;

    while (*s == ' ') s++;
    if (*s == ',') s++;
  }
  if (*s != ']') {
    out.warning("tvector_from_string: missing ']'");
    return;
  }

  if (p.is_known()) {
    if (count != static_cast<size_t>(p.value().dimension)) {
      out.warning("tvector_from_string: dimension mismatch");
      return;
    }
  } else {
    p.set(TVectorParams{.dimension = static_cast<int64_t>(count),
                        .bytes_per_elem = 4});
  }

  out.set_length(count * bpe);
}

// Decode: N * bpe bytes binary -> "[v1,v2,...,vN]" string.
// TVECTOR -> STRING
// Dimension and element type are read from type parameters.
bool tvector_to_string(const TVectorParams &p,
                       vsql::Span<const unsigned char> data,
                       vsql::Span<char> out, size_t *out_len) {
  const size_t bpe = p.bytes_per_elem;
  if (data.size() != static_cast<size_t>(p.dimension) * bpe) return true;

  size_t pos = 0;
  if (pos >= out.size()) return true;
  out[pos++] = '[';

  for (size_t i = 0; i < static_cast<size_t>(p.dimension); i++) {
    if (i > 0) {
      if (pos >= out.size()) return true;
      out[pos++] = ',';
    }
    int written;
    if (bpe == 8) {
      double val = load_double(data.data() + i * bpe);
      written = snprintf(out.data() + pos, out.size() - pos, "%.17g", val);
    } else {
      float val = load_float(data.data() + i * bpe);
      written = snprintf(out.data() + pos, out.size() - pos, "%g", val);
    }
    if (written < 0 || pos + static_cast<size_t>(written) >= out.size())
      return true;
    pos += static_cast<size_t>(written);
  }

  if (pos >= out.size()) return true;
  out[pos++] = ']';

  *out_len = pos;
  return false;
}

// Compare: (TVECTOR, TVECTOR) -> INT for ORDER BY, indexes.
// Lexicographic element-by-element comparison.
// TODO(villagesql-performance): we can also consider having templated versions
// of these functions instead of using branches, then selecting the version to
// use with one branch.
int tvector_compare(const TVectorParams &p, vsql::Span<const unsigned char> a,
                    vsql::Span<const unsigned char> b) {
  for (int64_t i = 0; i < p.dimension; i++) {
    if (p.bytes_per_elem == 8) {
      double v1 = load_double(a.data() + i * p.bytes_per_elem);
      double v2 = load_double(b.data() + i * p.bytes_per_elem);
      if (v1 < v2) return -1;
      if (v1 > v2) return 1;
    } else {
      float v1 = load_float(a.data() + i * p.bytes_per_elem);
      float v2 = load_float(b.data() + i * p.bytes_per_elem);
      if (v1 < v2) return -1;
      if (v1 > v2) return 1;
    }
  }
  return 0;
}

// Implicit default for TVECTOR: returns "[0,0,...,0]" with p.dimension zeros.
// The server converts this string using the type's from_string function.
std::string tvector_default(const TVectorParams &p, char * /*error_msg*/) {
  std::string buf = "[";
  for (int64_t i = 0; i < p.dimension; i++) {
    if (i > 0) buf += ",";
    buf += "0";
  }
  buf += "]";
  return buf;
}

// Dot product: (TVECTOR, TVECTOR) -> REAL
// Returns the sum of element-wise products of two vectors of the same type.
void tvector_dot_product(vsql::CustomArgWith<TVectorParams> a,
                         vsql::CustomArgWith<TVectorParams> b,
                         vsql::RealResult out) {
  if (a.is_null() || b.is_null()) {
    out.set_null();
    return;
  }
  const TVectorParams &pa = a.params();
  const TVectorParams &pb = b.params();
  if (pa.dimension != pb.dimension || pa.bytes_per_elem != pb.bytes_per_elem) {
    out.error(
        "tvector_dot_product: vectors must have the same dimension and type");
    return;
  }
  const unsigned char *da = a.value().data();
  const unsigned char *db = b.value().data();
  double sum = 0.0;
  for (int64_t i = 0; i < pa.dimension; i++) {
    if (pa.bytes_per_elem == 8) {
      sum += load_double(da + i * 8) * load_double(db + i * 8);
    } else {
      sum += static_cast<double>(load_float(da + i * 4)) *
             static_cast<double>(load_float(db + i * 4));
    }
  }
  out.set(sum);
}

// Element-wise add: (TVECTOR, TVECTOR) -> TVECTOR
// Vectors must have the same dimension and element type.
void tvector_add(vsql::CustomArgWith<TVectorParams> a,
                 vsql::CustomArgWith<TVectorParams> b,
                 vsql::CustomResultWith<TVectorParams> out) {
  if (a.is_null() || b.is_null()) {
    out.set_null();
    return;
  }
  const TVectorParams &pa = a.params();
  const TVectorParams &pb = b.params();
  if (pa.dimension != pb.dimension || pa.bytes_per_elem != pb.bytes_per_elem) {
    out.error("tvector_add: vectors must have the same dimension and type");
    return;
  }
  auto buf = out.buffer();
  size_t byte_size = static_cast<size_t>(pa.dimension) * pa.bytes_per_elem;
  if (buf.size() < byte_size) {
    out.error("tvector_add: output buffer too small");
    return;
  }
  const unsigned char *da = a.value().data();
  const unsigned char *db = b.value().data();
  for (int64_t i = 0; i < pa.dimension; i++) {
    if (pa.bytes_per_elem == 8) {
      double v = load_double(da + i * 8) + load_double(db + i * 8);
      store_double(buf.data() + i * 8, v);
    } else {
      float v = load_float(da + i * 4) + load_float(db + i * 4);
      store_float(buf.data() + i * 4, v);
    }
  }
  out.set_length(byte_size);
}

// Scalar multiply: (TVECTOR, REAL) -> TVECTOR
void tvector_scale(vsql::CustomArgWith<TVectorParams> a, vsql::RealArg scalar,
                   vsql::CustomResultWith<TVectorParams> out) {
  if (a.is_null() || scalar.is_null()) {
    out.set_null();
    return;
  }
  const TVectorParams &pa = a.params();
  double s = scalar.value();
  auto buf = out.buffer();
  size_t byte_size = static_cast<size_t>(pa.dimension) * pa.bytes_per_elem;
  if (buf.size() < byte_size) {
    out.error("tvector_scale: output buffer too small");
    return;
  }
  const unsigned char *da = a.value().data();
  for (int64_t i = 0; i < pa.dimension; i++) {
    if (pa.bytes_per_elem == 8) {
      double v = load_double(da + i * 8) * s;
      store_double(buf.data() + i * 8, v);
    } else {
      float v = load_float(da + i * 4) * static_cast<float>(s);
      store_float(buf.data() + i * 4, v);
    }
  }
  out.set_length(byte_size);
}

static constexpr const char kTVectorTypeName[] = "TVECTOR";

// Upper bound on TVECTOR's persisted byte size: kTVectorMaxDimension
// elements at 8 bytes each (double, the wider of the two supported element
// types). Used only on the fix_fields-time constant-string inference path;
// row-time encoding uses the params-resolved persisted_length set by
// tvector_resolve_params.
constexpr int64_t kTVectorMaxPersistedLength = kTVectorMaxDimension * 8;

constexpr auto TVECTOR = vsql::make_type<kTVectorTypeName>()
                             .persisted_length(-1)
                             .max_decode_buffer_length(16)
                             .max_persisted_length(kTVectorMaxPersistedLength)
                             .params<TVectorParams, &TVectorParams::parse,
                                     &TVectorParams::to_strings>()
                             .int_to_params<&tvector_int_to_params>()
                             .resolve_params<&tvector_resolve_params>()
                             .from_string<&tvector_from_string>()
                             .to_string<&tvector_to_string>()
                             .compare<&tvector_compare>()
                             .intrinsic_default_vdf("tvector_intrinsic_default")
                             .build();

using namespace vsql;

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .type(TVECTOR)
        .func(make_intrinsic_default<&tvector_default>(
            "tvector_intrinsic_default"))
        .func(make_func<&tvector_dot_product>("tvector_dot_product")
                  .returns(REAL)
                  .param(TVECTOR)
                  .param(TVECTOR)
                  .deterministic()
                  .build())
        .func(make_func<&tvector_add>("tvector_add")
                  .returns(TVECTOR)
                  .param(TVECTOR)
                  .param(TVECTOR)
                  .deterministic()
                  .build())
        .func(make_func<&tvector_scale>("tvector_scale")
                  .returns(TVECTOR)
                  .param(TVECTOR)
                  .param(REAL)
                  .deterministic()
                  .build()))
