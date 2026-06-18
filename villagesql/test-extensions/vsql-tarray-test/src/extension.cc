// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

// Test fixture for issue #535: a variable-length typed array (TARRAY).
//
// TARRAY is a typed array where the element WIDTH is a 'type' parameter
// (int16/float/double) and the element COUNT is decided per value,
// bounded by a 'max_size' parameter that caps how many elements
// a value may hold:
//   TARRAY(N)                          -> max_size=N, type defaults to int16
//   TARRAY('type=float,max_size=10')   -> float elements, at most 10
// The integer shorthand TARRAY(N) is mapped to max_size by int_to_params. A
// column must specify the parameters (it is a parameterized type).
//
// resolve_params enforces max_size * element_size <= max_persisted_length (the
// type's static upper bound, the count is still variable per value, just
// capped at max_size). from_string rejects a value with more than max_size
// elements.
//
// The backing field is a VARBINARY(max_persisted_length); only the actual
// encoded bytes are stored per value. Element widths are stored little-endian
// for portable persistence.

#include <villagesql/vsql.h>

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <string>
#include <string_view>

// Static upper bound on a stored TARRAY value in bytes. resolve_params requires
// max_size * element_size to stay within this.
constexpr int64_t kTarrayMaxLen = 512;
// max decoded double element (a double in %g). At most 25 characters + 1 for
// comma.
constexpr int64_t kMaxDecodedDoubleElem = 25 + 1;

// Element widths in bytes for the supported element types.
constexpr int64_t kElemInt16 = 2;
constexpr int64_t kElemFloat = 4;
constexpr int64_t kElemDouble = 8;

// The element type selected by the 'type' parameter.
enum class TarrayElemType { kInt16, kFloat, kDouble };

// Parameters: element width (from 'type') and element-count cap (from
// 'max_size').
struct TarrayParams {
  int64_t bytes_per_elem = kElemInt16;
  TarrayElemType elem_type = TarrayElemType::kInt16;
  int64_t max_size = 0;  // 0 = unset; resolve_params rejects a non-positive max

  static TarrayParams parse(const std::map<std::string, std::string> &params) {
    TarrayParams p;
    auto t = params.find("type");
    if (t != params.end()) {
      if (t->second == "float") {
        p.bytes_per_elem = kElemFloat;
        p.elem_type = TarrayElemType::kFloat;
      } else if (t->second == "double") {
        p.bytes_per_elem = kElemDouble;
        p.elem_type = TarrayElemType::kDouble;
      } else {
        assert(t->second == "int16");
        p.bytes_per_elem = kElemInt16;
        p.elem_type = TarrayElemType::kInt16;
      }
    }
    auto m = params.find("max_size");
    if (m != params.end()) p.max_size = strtoll(m->second.c_str(), nullptr, 10);
    return p;
  }

  static void to_strings(const TarrayParams &p,
                         std::map<std::string, std::string> &out) {
    switch (p.elem_type) {
      case TarrayElemType::kInt16:
        out["type"] = "int16";
        break;
      case TarrayElemType::kFloat:
        out["type"] = "float";
        break;
      case TarrayElemType::kDouble:
        out["type"] = "double";
        break;
    }
    out["max_size"] = std::to_string(p.max_size);
  }
};

// Little-endian byte serialization primitives, shared by all element widths.
static void store_bytes(unsigned char *buf, uint64_t bits, int n) {
  for (int i = 0; i < n; i++) {
    buf[i] = static_cast<unsigned char>(bits >> (i * 8));
  }
}

static uint64_t load_bytes(const unsigned char *buf, int n) {
  uint64_t bits = 0;
  for (int i = 0; i < n; i++) {
    bits |= static_cast<uint64_t>(buf[i]) << (i * 8);
  }
  return bits;
}

static void store_i16(unsigned char *buf, int16_t v) {
  store_bytes(buf, static_cast<uint16_t>(v), 2);
}

static int16_t load_i16(const unsigned char *buf) {
  return static_cast<int16_t>(static_cast<uint16_t>(load_bytes(buf, 2)));
}

static void store_f32(unsigned char *buf, float v) {
  uint32_t bits;
  memcpy(&bits, &v, sizeof(float));
  store_bytes(buf, bits, 4);
}

static float load_f32(const unsigned char *buf) {
  uint32_t bits = static_cast<uint32_t>(load_bytes(buf, 4));
  float v;
  memcpy(&v, &bits, sizeof(float));
  return v;
}

static void store_d64(unsigned char *buf, double v) {
  uint64_t bits;
  memcpy(&bits, &v, sizeof(double));
  store_bytes(buf, bits, 8);
}

static double load_d64(const unsigned char *buf) {
  uint64_t bits = load_bytes(buf, 8);
  double v;
  memcpy(&v, &bits, sizeof(double));
  return v;
}

// int_to_params: TARRAY(N) maps the integer N to the max_size parameter. The
// element type defaults to int16 unless given via the string parameter form.
bool tarray_int_to_params(int64_t value,
                          std::map<std::string, std::string> &params,
                          char *error_msg) {
  if (value <= 0) {
    snprintf(
        error_msg, VEF_MAX_ERROR_LEN,
        "tarray_int_to_params: TARRAY max_size must be positive (got %lld)",
        static_cast<long long>(value));
    return true;
  }
  params["max_size"] = std::to_string(value);
  return false;
}

// resolve_params: validate the element width and the max_size cap, and enforce
// max_size * element_size <= max_persisted_length (the count is variable per
// value, capped at max_size).
bool tarray_resolve_params(const std::map<std::string, std::string> &params,
                           vsql::ResolvedTypeParams *result, char *error_msg) {
  // Validate "type" parameter if present.
  auto type_it = params.find("type");
  if (type_it != params.end() && type_it->second != "float" &&
      type_it->second != "double" && type_it->second != "int16") {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "tarray_resolve_params: type must be 'float' or 'double' or "
             "'int16', got '%s'",
             type_it->second.c_str());
    return true;
  }

  auto it = params.find("max_size");
  if (it == params.end()) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "tarray_resolve_params: TARRAY requires a max_size parameter");
    return true;
  }
  char *endptr = nullptr;
  int64_t max_size = strtoll(it->second.c_str(), &endptr, 10);
  if (endptr == it->second.c_str() || *endptr != '\0') {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "tarray_resolve_params: invalid max_size '%s'",
             it->second.c_str());
    return true;
  }
  if (max_size <= 0) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "tarray_resolve_params: TARRAY max_size must be positive");
    return true;
  }

  int64_t bpe = TarrayParams::parse(params).bytes_per_elem;

  if (max_size * bpe > kTarrayMaxLen) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "tarray_resolve_params: TARRAY max_size %lld * element size %lld "
             "exceeds the %lld-byte "
             "limit",
             static_cast<long long>(max_size), static_cast<long long>(bpe),
             static_cast<long long>(kTarrayMaxLen));
    return true;
  }

  assert(result->persisted_length <= 0);
  // Upper bound on the text form: at most max_size elements, each printing to
  // at most kMaxDecodedDoubleElem chars plus the brackets.
  result->max_decode_buffer_length = max_size * kMaxDecodedDoubleElem + 2;
  return false;
}

// STRING -> binary: parse "[v1,v2,...]" into elements of the parameterized
// width, rejecting more than max_size elements.
void tarray_from_string(vsql::MaybeParams<TarrayParams> &p,
                        std::string_view from, vsql::CustomResult out) {
  int64_t bpe = kElemInt16;
  TarrayElemType elem_type = TarrayElemType::kInt16;
  if (p.is_known()) {
    bpe = p.value().bytes_per_elem;
    elem_type = p.value().elem_type;
  }
  assert(bpe > 0);
  auto buf = out.buffer();

  // out buffer size is equal to max_persisted_size. resolve_params guarantees
  // that max_size*bpe is smaller than max_persisted_size. We want to parse at
  // most max_size elements from the string into out buffer. In case string has
  // more than max_size elements - we will return a warning and not write more
  // than max_size elements into out buffer.
  assert(!p.is_known() || p.value().max_size > 0);
  assert(!p.is_known() ||
         (p.value().max_size * bpe <= static_cast<int64_t>(buf.size())));
  const size_t cap = (p.is_known() && p.value().max_size > 0)
                         ? static_cast<size_t>(p.value().max_size)
                         : buf.size() / static_cast<size_t>(bpe);

  std::string input(from);
  const char *s = input.c_str();
  while (*s == ' ') s++;
  if (*s != '[') {
    out.warning("tarray_from_string: expected '['");
    return;
  }
  s++;

  size_t count = 0;
  while (*s != '\0') {
    while (*s == ' ') s++;
    if (*s == ']') break;
    if (count >= cap) {
      out.warning("TARRAY: more elements than max_size");
      return;
    }
    char *endptr = nullptr;
    unsigned char *slot = buf.data() + count * static_cast<size_t>(bpe);
    switch (elem_type) {
      case TarrayElemType::kInt16: {
        long v = strtol(s, &endptr, 10);
        if (endptr == s) {
          out.warning("tarray_from_string: parse error when parsing int16");
          return;
        }
        store_i16(slot, static_cast<int16_t>(v));
        break;
      }
      case TarrayElemType::kFloat: {
        float v = strtof(s, &endptr);
        if (endptr == s) {
          out.warning("tarray_from_string: parse error when parsing float");
          return;
        }
        store_f32(slot, v);
        break;
      }
      case TarrayElemType::kDouble: {
        double v = strtod(s, &endptr);
        if (endptr == s) {
          out.warning("tarray_from_string: parse error when parsing double");
          return;
        }
        store_d64(slot, v);
        break;
      }
      default:
        out.warning("tarray_from_string: invalid element type");
        assert(false);
        return;
    }
    count++;
    s = endptr;
    while (*s == ' ') s++;
    if (*s == ',') s++;
  }
  if (*s != ']') {
    out.warning("tarray_from_string: missing ']'");
    return;
  }
  out.set_length(count * static_cast<size_t>(bpe));
}

// binary -> STRING: the element count is the value's byte length divided by the
// parameterized element width.
void tarray_to_string(vsql::CustomArgWith<TarrayParams> in,
                      vsql::StringResult out) {
  const int64_t bpe = in.params().bytes_per_elem;
  TarrayElemType elem_type = in.params().elem_type;
  assert(bpe > 0);
  auto data = in.value();
  const size_t count = data.size() / static_cast<size_t>(bpe);
  auto buf = out.buffer();
  size_t pos = 0;
  if (pos >= buf.size()) return;
  buf[pos++] = '[';
  for (size_t i = 0; i < count; i++) {
    if (i > 0) {
      if (pos >= buf.size()) return;
      buf[pos++] = ',';
    }
    const unsigned char *slot = data.data() + i * static_cast<size_t>(bpe);
    int written = 0;
    switch (elem_type) {
      case TarrayElemType::kInt16:
        written = snprintf(buf.data() + pos, buf.size() - pos, "%d",
                           static_cast<int>(load_i16(slot)));
        break;
      case TarrayElemType::kFloat:
        written = snprintf(buf.data() + pos, buf.size() - pos, "%g",
                           static_cast<double>(load_f32(slot)));
        break;
      case TarrayElemType::kDouble:
        written =
            snprintf(buf.data() + pos, buf.size() - pos, "%g", load_d64(slot));
        break;
      default:
        out.warning("tarray_to_string: invalid element type");
        out.set_length(0);
        assert(written == 0);
    }
    if (written < 0 || pos + static_cast<size_t>(written) >= buf.size()) return;
    pos += static_cast<size_t>(written);
  }
  if (pos >= buf.size()) return;
  buf[pos++] = ']';
  out.set_length(pos);
}

// Element-wise comparison; a shorter array sorts first on a common prefix.
int tarray_compare(vsql::CustomArgWith<TarrayParams> a,
                   vsql::CustomArgWith<TarrayParams> b) {
  const int64_t bpe = a.params().bytes_per_elem;
  assert(bpe > 0);
  auto va = a.value();
  auto vb = b.value();
  size_t na = va.size() / static_cast<size_t>(bpe);
  size_t nb = vb.size() / static_cast<size_t>(bpe);
  size_t n = na < nb ? na : nb;
  for (size_t i = 0; i < n; i++) {
    const unsigned char *pa = va.data() + i * static_cast<size_t>(bpe);
    const unsigned char *pb = vb.data() + i * static_cast<size_t>(bpe);
    TarrayElemType elem_type = a.params().elem_type;
    switch (elem_type) {
      case TarrayElemType::kInt16: {
        int16_t ea = load_i16(pa), eb = load_i16(pb);
        if (ea != eb) return ea < eb ? -1 : 1;
        break;
      }
      case TarrayElemType::kFloat: {
        float ea = load_f32(pa), eb = load_f32(pb);
        if (ea != eb) return ea < eb ? -1 : 1;
        break;
      }
      case TarrayElemType::kDouble: {
        double ea = load_d64(pa), eb = load_d64(pb);
        if (ea != eb) return ea < eb ? -1 : 1;
        break;
      }
      default:
        assert(false);
    }
  }
  if (na != nb) return na < nb ? -1 : 1;
  return 0;
}

// Number of elements in this TARRAY value.
void tarray_dim(vsql::CustomArgWith<TarrayParams> in, vsql::IntResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }
  const int64_t bpe = in.params().bytes_per_elem;
  assert(bpe > 0);
  out.set(static_cast<long long>(in.value().size() / static_cast<size_t>(bpe)));
}

// Concatenate two TARRAY values: (TARRAY, TARRAY) -> TARRAY. Both share the
// same parameters (type and max_size, guaranteed equal by the
// parameter-conflict check during VDF argument validation). The result must
// still respect max_size, so a concatenation whose combined element count
// exceeds max_size is rejected with a warning.
void tarray_concat(vsql::CustomArgWith<TarrayParams> a,
                   vsql::CustomArgWith<TarrayParams> b,
                   vsql::CustomResultWith<TarrayParams> out) {
  if (a.is_null() || b.is_null()) {
    out.set_null();
    return;
  }
  const int64_t bpe = a.params().bytes_per_elem;
  assert(bpe > 0);
  assert(
      bpe ==
      b.params().bytes_per_elem);  // guaranteed equal by the parameter-conflict
                                   // check during VDF argument validation
  const int64_t max_size = a.params().max_size;
  assert(max_size ==
         b.params().max_size);  // guaranteed equal by the parameter-conflict
                                // check during VDF argument validation
  auto va = a.value();
  auto vb = b.value();
  const size_t total = va.size() + vb.size();
  const size_t total_elems = total / static_cast<size_t>(bpe);
  if (static_cast<int64_t>(total_elems) > max_size) {
    out.warning("tarray_concat: concatenated size exceeds max_size");
    return;
  }
  auto buf = out.buffer();
  if (va.size() > 0) memcpy(buf.data(), va.data(), va.size());
  if (vb.size() > 0) memcpy(buf.data() + va.size(), vb.data(), vb.size());
  out.set_length(total);
}

static constexpr const char kTarrayTypeName[] = "TARRAY";

constexpr auto TARRAY =
    vsql::make_type<kTarrayTypeName>()
        .variable_length_type()
        .max_persisted_length(kTarrayMaxLen)
        .max_decode_buffer_length(kTarrayMaxLen / kElemInt16 * 8 + 2)
        .params<TarrayParams, &TarrayParams::parse, &TarrayParams::to_strings>()
        .int_to_params<&tarray_int_to_params>()
        .resolve_params<&tarray_resolve_params>()
        .from_string<&tarray_from_string>()
        .to_string<&tarray_to_string>()
        .compare<&tarray_compare>()
        .build();

using namespace vsql;

VEF_GENERATE_ENTRY_POINTS(make_extension()
                              .type(TARRAY)
                              .func(make_func<&tarray_dim>("tarray_dim")
                                        .returns(INT)
                                        .param(TARRAY)
                                        .deterministic()
                                        .build())
                              .func(make_func<&tarray_concat>("tarray_concat")
                                        .returns(TARRAY)
                                        .param(TARRAY)
                                        .param(TARRAY)
                                        .deterministic()
                                        .build()))
