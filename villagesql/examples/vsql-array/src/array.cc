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

// ARRAY: a variable-length typed array.
//
// Built on the same VEF protocol 4 variable-length type support as the TARRAY
// test fixture (villagesql/test-extensions/vsql-tarray-test). The element type
// is a 'type' parameter and the element count is decided per value, capped by
// a 'max_size' parameter:
//   ARRAY(N)                          -> max_size=N, type=int64
//   ARRAY('type=int32,max_size=10')   -> int32 elements, at most 10
// Element types: int32, int64, double. 'type' defaults to int64;
// resolve_params writes the default back so ARRAY(N) and
// ARRAY('type=int64,max_size=N') are the same type.
//
// Text form: '[1,2,3]'; '[]' is the empty array. Whitespace is allowed around
// elements. Arrays hold no NULL elements; a whole ARRAY value may be NULL.
//
// Stored form (variable length, up to kArrayMaxLen bytes):
//   byte 0         element-type tag (ArrayElem); lets every reader check the
//                  bytes against the column's parameters, and keeps the empty
//                  array a non-empty value (the intrinsic default must be).
//   bytes 1..      count * element width, little-endian.
//
// Functions (indexes are 1-based):
//   array_length(a)        INT    element count; NULL if a is NULL
//   array_get(a, i)        INT    i-th element of an int32/int64 array;
//                                 NULL if a or i is NULL or i is out of range
//   array_get_double(a, i) REAL   same, any element type, as DOUBLE
//   array_contains(a, v)   INT    1/0; v is parsed as the element type
//   array_append(a, v)     ARRAY  a with v appended; a NULL a is treated as
//                                 the empty array; error past max_size
//   array_rows_json2(a, b)    STRING  '[[a1,b1],[a2,b2],...]': the arrays zipped
//   array_rows_json3(a, b, c) STRING  position by position as a JSON array of rows,
//                                 to read arrays as a table in one call:
//                                 JSON_TABLE(array_rows_json3(a, b, c), '$[*]'
//                                 COLUMNS (n FOR ORDINALITY, x INT PATH '$[0]',
//                                 ...)). NULL if any argument is NULL; error if
//                                 the arrays differ in length
//   array_agg(x)           ARRAY  aggregate of INT x -> type=int64
//   array_agg_int32(x)     ARRAY  aggregate of INT x -> type=int32
//   array_agg_double(x)    ARRAY  aggregate of REAL x -> type=double
// The aggregates skip NULL inputs, return NULL for a group with no rows, and
// return ARRAY('type=<elem>,max_size=kAggMaxSize'); see array_agg_bind.

#include <villagesql/vsql.h>

#include <algorithm>
#include <cassert>
#include <cctype>
#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <limits>
#include <map>
#include <string>
#include <string_view>
#include <vector>

// ---------------------------------------------------------------------------
// Bounds
// ---------------------------------------------------------------------------

// Design bound: an ARRAY value holds at least this many int64 elements (the
// TPC-C use case collects INT ids per district/warehouse).
constexpr int64_t kArrayMaxInt64Elems = 1024;

// Element widths in bytes.
constexpr int64_t kElemInt32 = 4;
constexpr int64_t kElemInt64 = 8;
constexpr int64_t kElemDouble = 8;

// One tag byte in front of the elements (see the format note above).
constexpr int64_t kHeaderBytes = 1;

// Largest element payload: kArrayMaxInt64Elems int64 values = 8192 bytes, which
// is also 2048 int32 or 1024 double elements.
constexpr int64_t kArrayMaxPayload = kArrayMaxInt64Elems * kElemInt64;

// Static upper bound on a stored ARRAY value (the backing VARBINARY width).
constexpr int64_t kArrayMaxLen = kHeaderBytes + kArrayMaxPayload;

// Longest text of one element: "-2147483648", "-9223372036854775808",
// "-2.2250738585072014e-308".
constexpr int64_t kMaxTextInt32 = 11;
constexpr int64_t kMaxTextInt64 = 20;
constexpr int64_t kMaxTextDouble = 24;
// One ',' after each element, '[' and ']' around them.
constexpr int64_t kSeparatorChars = 1;
constexpr int64_t kBracketChars = 2;

// Static upper bound on the text form, the largest over the element types.
constexpr int64_t kArrayMaxDecodeLen = std::max(
    {(kArrayMaxPayload / kElemInt32) * (kMaxTextInt32 + kSeparatorChars),
     (kArrayMaxPayload / kElemInt64) * (kMaxTextInt64 + kSeparatorChars),
     (kArrayMaxPayload / kElemDouble) * (kMaxTextDouble + kSeparatorChars)}) +
    kBracketChars;

// Doubles print with the shortest of these precisions that reads back to the
// same value (DBL_DIG, then max_digits10).
constexpr int kDoubleShortDigits = std::numeric_limits<double>::digits10;
constexpr int kDoubleRoundTripDigits = std::numeric_limits<double>::max_digits10;

// max_size of the ARRAY returned by the array_agg family.
constexpr int64_t kAggMaxSize = kArrayMaxInt64Elems;

// array_rows_json*: the most arrays zipped into one row, and the longest
// text they can produce: per row the brackets, a separator after it, and per
// element its longest text plus a separator; plus the outer brackets.
constexpr int64_t kRowsJsonMaxArrays = 3;
constexpr int64_t kRowsJsonMaxLen =
    kBracketChars +
    kArrayMaxInt64Elems * (kBracketChars + kSeparatorChars +
                           kRowsJsonMaxArrays *
                               (kMaxTextDouble + kSeparatorChars));

// Room for one formatted element plus its terminating NUL.
constexpr size_t kElemTextBuf = 32;

// Parameter keys and the default element type.
constexpr const char kParamType[] = "type";
constexpr const char kParamMaxSize[] = "max_size";
constexpr const char kDefaultElemTypeName[] = "int64";

// ---------------------------------------------------------------------------
// Element types and parameters
// ---------------------------------------------------------------------------

// Tag values are persisted in byte 0 of every value: never renumber.
enum class ArrayElem : uint8_t { kInt32 = 1, kInt64 = 2, kDouble = 3 };

static int64_t elem_width(ArrayElem e) {
  switch (e) {
    case ArrayElem::kInt32:
      return kElemInt32;
    case ArrayElem::kInt64:
      return kElemInt64;
    case ArrayElem::kDouble:
      return kElemDouble;
  }
  assert(false);
  return kElemInt64;
}

static int64_t elem_max_text(ArrayElem e) {
  switch (e) {
    case ArrayElem::kInt32:
      return kMaxTextInt32;
    case ArrayElem::kInt64:
      return kMaxTextInt64;
    case ArrayElem::kDouble:
      return kMaxTextDouble;
  }
  assert(false);
  return kMaxTextDouble;
}

static const char *elem_name(ArrayElem e) {
  switch (e) {
    case ArrayElem::kInt32:
      return "int32";
    case ArrayElem::kInt64:
      return "int64";
    case ArrayElem::kDouble:
      return "double";
  }
  assert(false);
  return kDefaultElemTypeName;
}

// Returns false for an unknown element type name.
static bool elem_from_name(const std::string &name, ArrayElem *out) {
  if (name == "int32") {
    *out = ArrayElem::kInt32;
  } else if (name == "int64") {
    *out = ArrayElem::kInt64;
  } else if (name == "double") {
    *out = ArrayElem::kDouble;
  } else {
    return false;
  }
  return true;
}

static bool elem_is_integer(ArrayElem e) { return e != ArrayElem::kDouble; }

// Largest max_size the stored form allows for an element type.
static int64_t elem_max_count(ArrayElem e) {
  return kArrayMaxPayload / elem_width(e);
}

struct ArrayParams {
  ArrayElem elem = ArrayElem::kInt64;
  int64_t max_size = 0;  // 0 = unset; resolve_params rejects it

  static ArrayParams parse(const std::map<std::string, std::string> &params) {
    ArrayParams p;
    auto t = params.find(kParamType);
    if (t != params.end()) {
      bool known = elem_from_name(t->second, &p.elem);
      assert(known);  // resolve_params validated it
      (void)known;
    }
    auto m = params.find(kParamMaxSize);
    if (m != params.end()) p.max_size = strtoll(m->second.c_str(), nullptr, 10);
    return p;
  }

  static void to_strings(const ArrayParams &p,
                         std::map<std::string, std::string> &out) {
    out[kParamType] = elem_name(p.elem);
    out[kParamMaxSize] = std::to_string(p.max_size);
  }
};

// ---------------------------------------------------------------------------
// Little-endian element storage
// ---------------------------------------------------------------------------

static void store_bytes(unsigned char *buf, uint64_t bits, int64_t n) {
  for (int64_t i = 0; i < n; i++) {
    buf[i] = static_cast<unsigned char>(bits >> (i * 8));
  }
}

static uint64_t load_bytes(const unsigned char *buf, int64_t n) {
  uint64_t bits = 0;
  for (int64_t i = 0; i < n; i++) {
    bits |= static_cast<uint64_t>(buf[i]) << (i * 8);
  }
  return bits;
}

// One element value, wide enough for every element type.
struct ElemValue {
  int64_t i = 0;    // int32 / int64
  double d = 0.0;   // double
};

static void store_elem(unsigned char *slot, ArrayElem e, const ElemValue &v) {
  switch (e) {
    case ArrayElem::kInt32:
      store_bytes(slot, static_cast<uint32_t>(static_cast<int32_t>(v.i)),
                  kElemInt32);
      return;
    case ArrayElem::kInt64:
      store_bytes(slot, static_cast<uint64_t>(v.i), kElemInt64);
      return;
    case ArrayElem::kDouble: {
      uint64_t bits;
      memcpy(&bits, &v.d, sizeof(double));
      store_bytes(slot, bits, kElemDouble);
      return;
    }
  }
  assert(false);
}

static ElemValue load_elem(const unsigned char *slot, ArrayElem e) {
  ElemValue v;
  switch (e) {
    case ArrayElem::kInt32:
      v.i = static_cast<int32_t>(
          static_cast<uint32_t>(load_bytes(slot, kElemInt32)));
      v.d = static_cast<double>(v.i);
      break;
    case ArrayElem::kInt64:
      v.i = static_cast<int64_t>(load_bytes(slot, kElemInt64));
      v.d = static_cast<double>(v.i);
      break;
    case ArrayElem::kDouble: {
      uint64_t bits = load_bytes(slot, kElemDouble);
      memcpy(&v.d, &bits, sizeof(double));
      break;
    }
  }
  return v;
}

// A checked view of one stored value.
struct ArrayView {
  ArrayElem elem = ArrayElem::kInt64;
  const unsigned char *data = nullptr;  // first element
  size_t count = 0;

  ElemValue at(size_t idx) const {
    return load_elem(data + idx * static_cast<size_t>(elem_width(elem)), elem);
  }
};

// Validates the stored bytes against the expected element type. Returns false
// for a value that is not a well-formed ARRAY of that element type.
static bool make_view(vsql::Span<const unsigned char> bytes, ArrayElem expected,
                      ArrayView *out) {
  if (bytes.size() < static_cast<size_t>(kHeaderBytes)) return false;
  if (bytes[0] != static_cast<unsigned char>(expected)) return false;
  const size_t payload = bytes.size() - static_cast<size_t>(kHeaderBytes);
  const size_t width = static_cast<size_t>(elem_width(expected));
  if (payload % width != 0) return false;
  out->elem = expected;
  out->data = bytes.data() + kHeaderBytes;
  out->count = payload / width;
  return true;
}

// ---------------------------------------------------------------------------
// Text parsing and formatting
// ---------------------------------------------------------------------------

static const char *skip_space(const char *s) {
  while (*s != '\0' && isspace(static_cast<unsigned char>(*s))) s++;
  return s;
}

// Parses one element of type e at s. On success fills *v, sets *end past the
// number and returns nullptr; on failure returns an error message.
static const char *parse_elem(const char *s, ArrayElem e, ElemValue *v,
                              const char **end) {
  char *endptr = nullptr;
  errno = 0;
  if (elem_is_integer(e)) {
    long long x = strtoll(s, &endptr, 10);
    if (endptr == s) return "expected an integer element";
    if (errno == ERANGE) return "integer element out of range";
    if (e == ArrayElem::kInt32 &&
        (x < std::numeric_limits<int32_t>::min() ||
         x > std::numeric_limits<int32_t>::max())) {
      return "element out of range for int32";
    }
    v->i = x;
  } else {
    double x = strtod(s, &endptr);
    if (endptr == s) return "expected a numeric element";
    if (!std::isfinite(x)) return "element is not a finite double";
    v->d = x;
  }
  *end = endptr;
  return nullptr;
}

// Parses a whole string as one element (used by array_append/array_contains).
static const char *parse_single(std::string_view text, ArrayElem e,
                                ElemValue *v) {
  std::string input(text);
  const char *s = skip_space(input.c_str());
  const char *end = nullptr;
  if (const char *err = parse_elem(s, e, v, &end)) return err;
  if (*skip_space(end) != '\0') return "unexpected characters after element";
  return nullptr;
}

// Formats one element into buf (size kElemTextBuf). Returns the length.
static int format_elem(char *buf, ArrayElem e, const ElemValue &v) {
  if (elem_is_integer(e)) {
    return snprintf(buf, kElemTextBuf, "%lld", static_cast<long long>(v.i));
  }
  int n = snprintf(buf, kElemTextBuf, "%.*g", kDoubleShortDigits, v.d);
  if (strtod(buf, nullptr) != v.d) {
    n = snprintf(buf, kElemTextBuf, "%.*g", kDoubleRoundTripDigits, v.d);
  }
  return n;
}

// ---------------------------------------------------------------------------
// Type operations
// ---------------------------------------------------------------------------

// ARRAY(N) maps N to max_size; the element type is then the default.
bool array_int_to_params(int64_t value,
                         std::map<std::string, std::string> &params,
                         char *error_msg) {
  if (value <= 0) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "ARRAY max_size must be positive (got %lld)",
             static_cast<long long>(value));
    return true;
  }
  params[kParamMaxSize] = std::to_string(value);
  return false;
}

// Validates 'type' and 'max_size', rejects unknown keys, writes the default
// element type back (so the canonical form always names it), and sizes the
// text form for this max_size.
bool array_resolve_params(std::map<std::string, std::string> &params,
                          vsql::ResolvedTypeParams *result, char *error_msg) {
  for (const auto &kv : params) {
    if (kv.first != kParamType && kv.first != kParamMaxSize) {
      snprintf(error_msg, VEF_MAX_ERROR_LEN,
               "ARRAY: unknown parameter '%s' (expected 'type' and "
               "'max_size')",
               kv.first.c_str());
      return true;
    }
  }

  ArrayElem elem = ArrayElem::kInt64;
  auto type_it = params.find(kParamType);
  if (type_it == params.end()) {
    params[kParamType] = kDefaultElemTypeName;
  } else if (!elem_from_name(type_it->second, &elem)) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "ARRAY: type must be 'int32', 'int64' or 'double', got '%s'",
             type_it->second.c_str());
    return true;
  }

  auto it = params.find(kParamMaxSize);
  if (it == params.end()) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "ARRAY requires a max_size parameter");
    return true;
  }
  char *endptr = nullptr;
  errno = 0;
  int64_t max_size = strtoll(it->second.c_str(), &endptr, 10);
  if (endptr == it->second.c_str() || *endptr != '\0' || errno == ERANGE) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN, "ARRAY: invalid max_size '%s'",
             it->second.c_str());
    return true;
  }
  if (max_size <= 0) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "ARRAY max_size must be positive");
    return true;
  }
  if (max_size > elem_max_count(elem)) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "ARRAY max_size %lld exceeds the limit of %lld %s elements "
             "(%lld bytes)",
             static_cast<long long>(max_size),
             static_cast<long long>(elem_max_count(elem)), elem_name(elem),
             static_cast<long long>(kArrayMaxPayload));
    return true;
  }
  // Canonical decimal form, so 'max_size=010' and 'max_size=10' match.
  it->second = std::to_string(max_size);

  assert(result->persisted_length <= 0);
  result->max_decode_buffer_length =
      max_size * (elem_max_text(elem) + kSeparatorChars) + kBracketChars;
  return false;
}

// STRING -> binary. Rejects (warning, no value) malformed text, elements that
// do not parse as the element type, and more than max_size elements.
void array_from_string(vsql::MaybeParams<ArrayParams> &p,
                       std::string_view from, vsql::CustomResult out) {
  if (!p.is_known()) {
    // Only reached when the server asks the type to infer parameters from a
    // constant string: a decimal point or exponent means double, otherwise
    // int64; max_size is the largest the element type allows.
    ArrayParams inferred;
    inferred.elem = from.find_first_of(".eE") != std::string_view::npos
                        ? ArrayElem::kDouble
                        : ArrayElem::kInt64;
    inferred.max_size = elem_max_count(inferred.elem);
    p.set(inferred);
  }
  const ArrayElem elem = p.value().elem;
  const int64_t max_size = p.value().max_size;
  const size_t width = static_cast<size_t>(elem_width(elem));
  auto buf = out.buffer();
  if (max_size <= 0 ||
      buf.size() < static_cast<size_t>(kHeaderBytes) +
                       static_cast<size_t>(max_size) * width) {
    out.error("ARRAY: output buffer smaller than max_size elements");
    return;
  }

  char msg[VEF_MAX_ERROR_LEN];
  std::string input(from);
  const char *s = skip_space(input.c_str());
  if (*s != '[') {
    out.warning("ARRAY: expected '['");
    return;
  }
  s = skip_space(s + 1);

  size_t count = 0;
  if (*s == ']') {
    s++;
  } else {
    while (true) {
      if (static_cast<int64_t>(count) >= max_size) {
        snprintf(msg, sizeof(msg), "ARRAY: more than max_size (%lld) elements",
                 static_cast<long long>(max_size));
        out.warning(msg);
        return;
      }
      ElemValue v;
      const char *end = nullptr;
      if (const char *err = parse_elem(s, elem, &v, &end)) {
        snprintf(msg, sizeof(msg), "ARRAY(%s): %s at element %zu",
                 elem_name(elem), err, count + 1);
        out.warning(msg);
        return;
      }
      store_elem(buf.data() + kHeaderBytes + count * width, elem, v);
      count++;
      s = skip_space(end);
      if (*s == ',') {
        s = skip_space(s + 1);
        continue;
      }
      if (*s == ']') {
        s++;
        break;
      }
      snprintf(msg, sizeof(msg),
               "ARRAY(%s): expected ',' or ']' after element %zu",
               elem_name(elem), count);
      out.warning(msg);
      return;
    }
  }
  if (*skip_space(s) != '\0') {
    out.warning("ARRAY: unexpected characters after ']'");
    return;
  }
  buf[0] = static_cast<unsigned char>(elem);
  out.set_length(static_cast<size_t>(kHeaderBytes) + count * width);
}

// binary -> STRING: '[e1,e2,...]'.
void array_to_string(vsql::CustomArgWith<ArrayParams> in,
                     vsql::StringResult out) {
  ArrayView view;
  if (!make_view(in.value(), in.params().elem, &view)) {
    out.error("ARRAY: stored value does not match the column's element type");
    return;
  }
  auto buf = out.buffer();
  size_t pos = 0;
  if (pos >= buf.size()) {
    out.error("ARRAY: text buffer too small");
    return;
  }
  buf[pos++] = '[';
  char text[kElemTextBuf];
  for (size_t i = 0; i < view.count; i++) {
    int n = format_elem(text, view.elem, view.at(i));
    const size_t need = static_cast<size_t>(n) + (i > 0 ? 1 : 0);
    if (n <= 0 || pos + need >= buf.size()) {
      out.error("ARRAY: text buffer too small");
      return;
    }
    if (i > 0) buf[pos++] = ',';
    memcpy(buf.data() + pos, text, static_cast<size_t>(n));
    pos += static_cast<size_t>(n);
  }
  buf[pos++] = ']';
  out.set_length(pos);
}

// Element-wise comparison; on a common prefix the shorter array sorts first.
int array_compare(vsql::CustomArgWith<ArrayParams> a,
                  vsql::CustomArgWith<ArrayParams> b) {
  ArrayView va, vb;
  const bool ok_a = make_view(a.value(), a.params().elem, &va);
  const bool ok_b = make_view(b.value(), b.params().elem, &vb);
  // Same type parameters are guaranteed by the server; a malformed value
  // sorts first so the comparison stays a total order.
  if (!ok_a || !ok_b) return ok_a == ok_b ? 0 : (ok_a ? 1 : -1);
  const size_t n = std::min(va.count, vb.count);
  for (size_t i = 0; i < n; i++) {
    ElemValue ea = va.at(i), eb = vb.at(i);
    if (elem_is_integer(va.elem)) {
      if (ea.i != eb.i) return ea.i < eb.i ? -1 : 1;
    } else {
      if (ea.d != eb.d) return ea.d < eb.d ? -1 : 1;
    }
  }
  if (va.count != vb.count) return va.count < vb.count ? -1 : 1;
  return 0;
}

// ---------------------------------------------------------------------------
// Scalar functions
// ---------------------------------------------------------------------------

void array_length(vsql::CustomArgWith<ArrayParams> in, vsql::IntResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }
  ArrayView view;
  if (!make_view(in.value(), in.params().elem, &view)) {
    out.error("array_length: malformed ARRAY value");
    return;
  }
  out.set(static_cast<long long>(view.count));
}

// Resolves the 1-based index; false when the element does not exist.
static bool element_at(const ArrayView &view, long long index,
                       ElemValue *out) {
  if (index < 1 || static_cast<unsigned long long>(index) > view.count) {
    return false;
  }
  *out = view.at(static_cast<size_t>(index - 1));
  return true;
}

void array_get(vsql::CustomArgWith<ArrayParams> in, vsql::IntArg index,
               vsql::IntResult out) {
  if (in.is_null() || index.is_null()) {
    out.set_null();
    return;
  }
  if (!elem_is_integer(in.params().elem)) {
    out.error("array_get: element type is double; use array_get_double");
    return;
  }
  ArrayView view;
  if (!make_view(in.value(), in.params().elem, &view)) {
    out.error("array_get: malformed ARRAY value");
    return;
  }
  ElemValue v;
  if (!element_at(view, index.value(), &v)) {
    out.set_null();
    return;
  }
  out.set(static_cast<long long>(v.i));
}

void array_get_double(vsql::CustomArgWith<ArrayParams> in, vsql::IntArg index,
                      vsql::RealResult out) {
  if (in.is_null() || index.is_null()) {
    out.set_null();
    return;
  }
  ArrayView view;
  if (!make_view(in.value(), in.params().elem, &view)) {
    out.error("array_get_double: malformed ARRAY value");
    return;
  }
  ElemValue v;
  if (!element_at(view, index.value(), &v)) {
    out.set_null();
    return;
  }
  out.set(v.d);
}

// The probe value is passed as text and parsed as the array's element type,
// so int64 values compare exactly and double arrays accept '1.5'.
void array_contains(vsql::CustomArgWith<ArrayParams> in, vsql::StringArg value,
                    vsql::IntResult out) {
  if (in.is_null() || value.is_null()) {
    out.set_null();
    return;
  }
  ArrayView view;
  if (!make_view(in.value(), in.params().elem, &view)) {
    out.error("array_contains: malformed ARRAY value");
    return;
  }
  ElemValue probe;
  if (const char *err = parse_single(value.value(), view.elem, &probe)) {
    char msg[VEF_MAX_ERROR_LEN];
    snprintf(msg, sizeof(msg), "array_contains(%s): %s", elem_name(view.elem),
             err);
    out.warning(msg);
    return;
  }
  for (size_t i = 0; i < view.count; i++) {
    ElemValue e = view.at(i);
    if (elem_is_integer(view.elem) ? e.i == probe.i : e.d == probe.d) {
      out.set(1);
      return;
    }
  }
  out.set(0);
}

// The result has the parameters of the first argument (built-in rule: the
// return ARRAY takes the parameters of the ARRAY argument), so they are read
// from out.params(), which also works when the input value is NULL.
void array_append(vsql::CustomArgWith<ArrayParams> in, vsql::StringArg value,
                  vsql::CustomResultWith<ArrayParams> out) {
  if (value.is_null()) {
    out.set_null();
    return;
  }
  const ArrayElem elem = out.params().elem;
  const int64_t max_size = out.params().max_size;
  const size_t width = static_cast<size_t>(elem_width(elem));
  char msg[VEF_MAX_ERROR_LEN];
  if (max_size <= 0) {
    // A bare NULL literal as the first argument carries no ARRAY type, so the
    // result type has no parameters.
    out.error(
        "array_append: the first argument has no ARRAY type parameters; pass "
        "a typed ARRAY column or variable");
    return;
  }

  ArrayView view;  // NULL input = empty array
  view.elem = elem;
  if (!in.is_null() && !make_view(in.value(), elem, &view)) {
    out.error("array_append: malformed ARRAY value");
    return;
  }
  if (static_cast<int64_t>(view.count) + 1 > max_size) {
    snprintf(msg, sizeof(msg),
             "array_append: result would exceed max_size (%lld)",
             static_cast<long long>(max_size));
    out.error(msg);
    return;
  }
  ElemValue v;
  if (const char *err = parse_single(value.value(), elem, &v)) {
    snprintf(msg, sizeof(msg), "array_append(%s): %s", elem_name(elem), err);
    out.error(msg);
    return;
  }
  auto buf = out.buffer();
  const size_t old_payload = view.count * width;
  if (buf.size() < static_cast<size_t>(kHeaderBytes) + old_payload + width) {
    out.error("array_append: output buffer too small");
    return;
  }
  buf[0] = static_cast<unsigned char>(elem);
  if (old_payload > 0) {
    memmove(buf.data() + kHeaderBytes, view.data, old_payload);
  }
  store_elem(buf.data() + kHeaderBytes + old_payload, elem, v);
  out.set_length(static_cast<size_t>(kHeaderBytes) + old_payload + width);
}

// Writes the arrays zipped position by position as a JSON array of rows,
// '[[a1,b1,...],[a2,b2,...],...]', so one call hands several arrays to
// JSON_TABLE as a table. NULL if any array is NULL; error if they differ in
// length or the text does not fit.
static void rows_json(const vsql::CustomArgWith<ArrayParams> *arrays,
                      size_t n_arrays, vsql::StringResult out) {
  ArrayView views[kRowsJsonMaxArrays];
  for (size_t a = 0; a < n_arrays; a++) {
    if (arrays[a].is_null()) {
      out.set_null();
      return;
    }
    if (!make_view(arrays[a].value(), arrays[a].params().elem, &views[a])) {
      out.error("array_rows_json: malformed ARRAY value");
      return;
    }
    if (views[a].count != views[0].count) {
      out.error("array_rows_json: the arrays differ in length");
      return;
    }
  }
  auto buf = out.buffer();
  size_t pos = 0;
  char text[kElemTextBuf];
  const size_t row_overhead =
      static_cast<size_t>(kBracketChars + kSeparatorChars);
  if (buf.size() < static_cast<size_t>(kBracketChars)) {
    out.error("array_rows_json: text buffer too small");
    return;
  }
  buf[pos++] = '[';
  for (size_t i = 0; i < views[0].count; i++) {
    if (pos + row_overhead >= buf.size()) {
      out.error("array_rows_json: text buffer too small");
      return;
    }
    if (i > 0) buf[pos++] = ',';
    buf[pos++] = '[';
    for (size_t a = 0; a < n_arrays; a++) {
      const int n = format_elem(text, views[a].elem, views[a].at(i));
      // Room for the element, its separator, and the two closing brackets.
      if (n <= 0 || pos + static_cast<size_t>(n) + row_overhead >= buf.size()) {
        out.error("array_rows_json: text buffer too small");
        return;
      }
      if (a > 0) buf[pos++] = ',';
      memcpy(buf.data() + pos, text, static_cast<size_t>(n));
      pos += static_cast<size_t>(n);
    }
    buf[pos++] = ']';
  }
  buf[pos++] = ']';
  out.set_length(pos);
}

void array_rows_json2(vsql::CustomArgWith<ArrayParams> a,
                      vsql::CustomArgWith<ArrayParams> b,
                      vsql::StringResult out) {
  const vsql::CustomArgWith<ArrayParams> arrays[] = {a, b};
  rows_json(arrays, std::size(arrays), out);
}

void array_rows_json3(vsql::CustomArgWith<ArrayParams> a,
                      vsql::CustomArgWith<ArrayParams> b,
                      vsql::CustomArgWith<ArrayParams> c,
                      vsql::StringResult out) {
  const vsql::CustomArgWith<ArrayParams> arrays[] = {a, b, c};
  rows_json(arrays, std::size(arrays), out);
}

// ---------------------------------------------------------------------------
// Aggregates: array_agg (int64), array_agg_int32, array_agg_double
// ---------------------------------------------------------------------------

struct ArrayAggState {
  std::vector<ElemValue> values;
  bool saw_row = false;       // false -> the group had no rows -> NULL
  bool overflow = false;      // more than kAggMaxSize non-NULL inputs
  bool out_of_range = false;  // an input does not fit the element type
};

void array_agg_clear(ArrayAggState &s) { s = ArrayAggState(); }

static void agg_push(ArrayAggState &s, const ElemValue &v) {
  if (static_cast<int64_t>(s.values.size()) >= kAggMaxSize) {
    s.overflow = true;
    return;
  }
  s.values.push_back(v);
}

void array_agg_accumulate_int64(ArrayAggState &s, vsql::IntArg x) {
  s.saw_row = true;
  if (x.is_null()) return;
  ElemValue v;
  v.i = x.value();
  agg_push(s, v);
}

void array_agg_accumulate_int32(ArrayAggState &s, vsql::IntArg x) {
  s.saw_row = true;
  if (x.is_null()) return;
  if (x.value() < std::numeric_limits<int32_t>::min() ||
      x.value() > std::numeric_limits<int32_t>::max()) {
    s.out_of_range = true;
    return;
  }
  ElemValue v;
  v.i = x.value();
  agg_push(s, v);
}

void array_agg_accumulate_double(ArrayAggState &s, vsql::RealArg x) {
  s.saw_row = true;
  if (x.is_null()) return;
  if (!std::isfinite(x.value())) {
    s.out_of_range = true;
    return;
  }
  ElemValue v;
  v.d = x.value();
  agg_push(s, v);
}

void array_agg_result(const ArrayAggState &s,
                      vsql::CustomResultWith<ArrayParams> out) {
  if (!s.saw_row) {
    out.set_null();
    return;
  }
  const ArrayElem elem = out.params().elem;
  const int64_t max_size = out.params().max_size;
  char msg[VEF_MAX_ERROR_LEN];
  if (s.overflow || static_cast<int64_t>(s.values.size()) > max_size) {
    snprintf(msg, sizeof(msg),
             "array_agg: more than max_size (%lld) non-NULL values",
             static_cast<long long>(max_size));
    out.error(msg);
    return;
  }
  if (s.out_of_range) {
    snprintf(msg, sizeof(msg), "array_agg: value out of range for %s",
             elem_name(elem));
    out.error(msg);
    return;
  }
  const size_t width = static_cast<size_t>(elem_width(elem));
  auto buf = out.buffer();
  if (buf.size() < static_cast<size_t>(kHeaderBytes) + s.values.size() * width) {
    out.error("array_agg: output buffer too small");
    return;
  }
  buf[0] = static_cast<unsigned char>(elem);
  for (size_t i = 0; i < s.values.size(); i++) {
    store_elem(buf.data() + kHeaderBytes + i * width, elem, s.values[i]);
  }
  out.set_length(static_cast<size_t>(kHeaderBytes) + s.values.size() * width);
}

// Decides the result type of an array_agg variant: the element type is fixed
// per function (the SQL argument is INT or REAL, which cannot tell INT from
// BIGINT), and max_size is kAggMaxSize. Without this hook the built-in rule
// would need an ARRAY argument to copy parameters from, and array_agg has none.
template <ArrayElem E>
void array_agg_bind(vsql::BindArgs /*args*/, vsql::BindResult out) {
  ArrayParams p;
  p.elem = E;
  p.max_size = kAggMaxSize;
  out.set_return(p);
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

static constexpr const char kArrayTypeName[] = "ARRAY";

constexpr auto ARRAY =
    vsql::make_type<kArrayTypeName>()
        .variable_length_type()
        .max_persisted_length(kArrayMaxLen)
        .max_decode_buffer_length(kArrayMaxDecodeLen)
        .params<ArrayParams, &ArrayParams::parse, &ArrayParams::to_strings>()
        .int_to_params<&array_int_to_params>()
        .resolve_params<&array_resolve_params>()
        .from_string<&array_from_string>()
        .to_string<&array_to_string>()
        .compare<&array_compare>()
        .intrinsic_default_str("[]")
        .build();

using namespace vsql;

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .type(ARRAY)
        .func(make_func<&array_length>("array_length")
                  .returns(INT)
                  .param(ARRAY)
                  .deterministic()
                  .build())
        .func(make_func<&array_get>("array_get")
                  .returns(INT)
                  .param(ARRAY)
                  .param(INT)
                  .deterministic()
                  .build())
        .func(make_func<&array_get_double>("array_get_double")
                  .returns(REAL)
                  .param(ARRAY)
                  .param(INT)
                  .deterministic()
                  .build())
        .func(make_func<&array_contains>("array_contains")
                  .returns(INT)
                  .param(ARRAY)
                  .param(STRING)
                  .deterministic()
                  .build())
        .func(make_func<&array_append>("array_append")
                  .returns(ARRAY)
                  .param(ARRAY)
                  .param(STRING)
                  .deterministic()
                  .build())
        .func(make_func<&array_rows_json2>("array_rows_json2")
                  .returns(STRING)
                  .param(ARRAY)
                  .param(ARRAY)
                  .buffer_size(static_cast<size_t>(kRowsJsonMaxLen))
                  .deterministic()
                  .build())
        .func(make_func<&array_rows_json3>("array_rows_json3")
                  .returns(STRING)
                  .param(ARRAY)
                  .param(ARRAY)
                  .param(ARRAY)
                  .buffer_size(static_cast<size_t>(kRowsJsonMaxLen))
                  .deterministic()
                  .build())
        .func(make_aggregate_func<ArrayAggState, &array_agg_result>("array_agg")
                  .returns(ARRAY)
                  .param(INT)
                  .clear<&array_agg_clear>()
                  .accumulate<&array_agg_accumulate_int64>()
                  .bind_and_check_types<&array_agg_bind<ArrayElem::kInt64>>()
                  .build())
        .func(make_aggregate_func<ArrayAggState, &array_agg_result>(
                "array_agg_int32")
                  .returns(ARRAY)
                  .param(INT)
                  .clear<&array_agg_clear>()
                  .accumulate<&array_agg_accumulate_int32>()
                  .bind_and_check_types<&array_agg_bind<ArrayElem::kInt32>>()
                  .build())
        .func(make_aggregate_func<ArrayAggState, &array_agg_result>(
                "array_agg_double")
                  .returns(ARRAY)
                  .param(REAL)
                  .clear<&array_agg_clear>()
                  .accumulate<&array_agg_accumulate_double>()
                  .bind_and_check_types<&array_agg_bind<ArrayElem::kDouble>>()
                  .build()))
