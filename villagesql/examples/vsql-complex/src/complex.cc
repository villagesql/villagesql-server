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

// VillageSQL COMPLEX extension demonstrating custom types with the new API.
//
// The COMPLEX type stores complex numbers as two IEEE 754 doubles (16 bytes).
// Format: "(real,imaginary)" e.g., "(3.14,2.71)"

#include <villagesql/extension.h>

#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>

struct Complex {
  double re, im;

  // Canonicalize -0 to +0 for consistent binary representation.
  // This ensures equal values have identical bytes, enabling binary hashing.
  // Uses IEEE 754 property: -0.0 == 0.0 is true.
  void canonicalize() {
    if (re == 0.0) re = 0.0;
    if (im == 0.0) im = 0.0;
  }
};

constexpr int64_t kComplexSize = sizeof(double) * 2;  // 16 bytes

// Platform-independent functions for storing/loading doubles as bytes.
// Uses little-endian format for cross-platform compatibility.

void store_double(unsigned char *buf, double val) {
  // Use memcpy to respect strict aliasing rules
  uint64_t bits;
  memcpy(&bits, &val, sizeof(double));
  // Write in little-endian order
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

void store_complex(unsigned char *buf, const Complex &cx) {
  store_double(buf, cx.re);
  store_double(buf + 8, cx.im);
}

Complex load_complex(const unsigned char *buf) {
  return Complex{load_double(buf), load_double(buf + 8)};
}

// Simple FNV-1a hash
size_t fnv1a_hash(const unsigned char *data, size_t len) {
  size_t hash = 2166136261u;
  for (size_t i = 0; i < len; i++) {
    hash ^= data[i];
    hash *= 16777619u;
  }
  return hash;
}

std::optional<Complex> TryLoadFromInValue(const vef_invalue_t *v) {
  if (v->bin_len != kComplexSize) {
    return std::nullopt;
  }
  return load_complex(v->bin_value);
}

void ReturnError(std::string_view err_msg, vef_vdf_result_t *result) {
  result->type = VEF_RESULT_ERROR;
  if (err_msg.size() >= VEF_MAX_ERROR_LEN) {
    err_msg = err_msg.substr(0, VEF_MAX_ERROR_LEN - 1);
  }
  err_msg.copy(result->error_msg, err_msg.size());
  result->error_msg[err_msg.size()] = 0;
}

void ReturnComplex(const Complex &cx, vef_vdf_result_t *result) {
  result->type = VEF_RESULT_VALUE;
  store_complex(result->bin_buf, cx);
  result->actual_len = kComplexSize;
}

// Implicit default for COMPLEX: writes (0,0) into the buffer.
// Called when INSERT IGNORE / UPDATE IGNORE assigns NULL to a NOT NULL column.
bool complex_default(villagesql::Span<unsigned char> buffer, size_t *length,
                     char * /*error_msg*/) {
  if (buffer.size() < kComplexSize) {
    *length = 0;
    return true;
  }
  Complex cx{0.0, 0.0};
  store_complex(buffer.data(), cx);
  *length = kComplexSize;
  return false;
}

// COMPLEX encode: "(real,imag)" -> 16 bytes (with canonicalization of -0.0)
// STRING -> COMPLEX
bool complex_from_string(std::string_view from,
                         villagesql::Span<unsigned char> buf, size_t *length) {
  if (buf.size() < kComplexSize) return true;
  Complex cx;
  std::string from_str(from);
  if (sscanf(from_str.c_str(), " ( %lg , %lg )", &cx.re, &cx.im) != 2) {
    return true;
  }
  cx.canonicalize();
  store_complex(buf.data(), cx);
  *length = kComplexSize;
  return false;
}

// COMPLEX2 encode: "(real,imag)" -> 16 bytes (without canonicalization,
// preserves -0.0 in binary form)
// STRING -> COMPLEX2
bool complex2_from_string(std::string_view from,
                          villagesql::Span<unsigned char> buf, size_t *length) {
  if (buf.size() < kComplexSize) return true;
  Complex cx;
  std::string from_str(from);
  if (sscanf(from_str.c_str(), " ( %lg , %lg )", &cx.re, &cx.im) != 2) {
    return true;
  }
  // No canonicalization - -0.0 is preserved in binary representation.
  // The custom hash function will canonicalize on the fly.
  store_complex(buf.data(), cx);
  *length = kComplexSize;
  return false;
}

// Decode: 16 bytes -> "(real,imag)" string
// COMPLEX -> STRING
bool complex_to_string(villagesql::Span<const unsigned char> data,
                       villagesql::Span<char> out, size_t *out_len) {
  if (data.size() != kComplexSize) return true;
  Complex cx = load_complex(data.data());
  int written = snprintf(out.data(), out.size(), "(%g,%g)", cx.re, cx.im);
  if (written < 0 || static_cast<size_t>(written) >= out.size()) return true;
  *out_len = static_cast<size_t>(written);
  return false;
}

// Compare: (COMPLEX, COMPLEX) -> INT for ORDER BY, indexes
int complex_compare(villagesql::Span<const unsigned char> a,
                    villagesql::Span<const unsigned char> b) {
  if (a.size() != kComplexSize || b.size() != kComplexSize) return 0;
  Complex lhs = load_complex(a.data());
  Complex rhs = load_complex(b.data());

  // Compare real parts first
  if (lhs.re < rhs.re) return -1;
  if (lhs.re > rhs.re) return 1;
  // Real parts equal, compare imaginary parts
  if (lhs.im < rhs.im) return -1;
  if (lhs.im > rhs.im) return 1;
  return 0;
}

// Hash VDF: COMPLEX2 -> INT
// Canonicalizes -0 to +0 before hashing so that -0.0 and +0.0 hash to the
// same bucket. This allows COMPLEX2 to preserve -0 in storage while still
// working correctly with hash joins and EXCEPT operations.
void complex2_hash(vef_context_t *ctx, vef_invalue_t *in,
                   vef_vdf_result_t *out) {
  auto cx = TryLoadFromInValue(in);
  if (!cx.has_value()) {
    ReturnError("argument malformed", out);
    return;
  }
  cx->canonicalize();

  // Hash the canonicalized values
  unsigned char canonical[kComplexSize];
  store_complex(canonical, *cx);
  out->int_value = static_cast<long long>(fnv1a_hash(canonical, kComplexSize));
  out->type = VEF_RESULT_VALUE;
}

// Arithmetic: complex_add(a, b) -> COMPLEX
void complex_add_impl(vef_context_t *ctx, vef_invalue_t *in_l,
                      vef_invalue_t *in_r, vef_vdf_result_t *out) {
  if (in_l->is_null || in_r->is_null) {
    out->type = VEF_RESULT_NULL;
    return;
  }

  const auto lhs = TryLoadFromInValue(in_l);
  if (!lhs.has_value()) {
    ReturnError("left argument malformed", out);
    return;
  }

  const auto rhs = TryLoadFromInValue(in_r);
  if (!rhs.has_value()) {
    ReturnError("right argument malformed", out);
    return;
  }

  if (out->max_bin_len < kComplexSize) {
    ReturnError("response buffer too small", out);
    return;
  }

  ReturnComplex(Complex{lhs->re + rhs->re, lhs->im + rhs->im}, out);
}

// Arithmetic: complex_subtract(a, b) -> COMPLEX
void complex_subtract_impl(vef_context_t *ctx, vef_invalue_t *in_l,
                           vef_invalue_t *in_r, vef_vdf_result_t *out) {
  if (in_l->is_null || in_r->is_null) {
    out->type = VEF_RESULT_NULL;
    return;
  }

  const auto lhs = TryLoadFromInValue(in_l);
  if (!lhs.has_value()) {
    ReturnError("left argument malformed", out);
    return;
  }

  const auto rhs = TryLoadFromInValue(in_r);
  if (!rhs.has_value()) {
    ReturnError("right argument malformed", out);
    return;
  }

  if (out->max_bin_len < kComplexSize) {
    ReturnError("response buffer too small", out);
    return;
  }

  ReturnComplex(Complex{lhs->re - rhs->re, lhs->im - rhs->im}, out);
}

// Arithmetic: complex_multiply(a, b) -> COMPLEX
void complex_multiply_impl(vef_context_t *ctx, vef_invalue_t *in_l,
                           vef_invalue_t *in_r, vef_vdf_result_t *out) {
  if (in_l->is_null || in_r->is_null) {
    out->type = VEF_RESULT_NULL;
    return;
  }

  const auto lhs = TryLoadFromInValue(in_l);
  if (!lhs.has_value()) {
    ReturnError("left argument malformed", out);
    return;
  }

  const auto rhs = TryLoadFromInValue(in_r);
  if (!rhs.has_value()) {
    ReturnError("right argument malformed", out);
    return;
  }

  if (out->max_bin_len < kComplexSize) {
    ReturnError("response buffer too small", out);
    return;
  }

  // (a + bi) * (c + di) = (ac - bd) + (ad + bc)i
  ReturnComplex(Complex{lhs->re * rhs->re - lhs->im * rhs->im,
                        lhs->re * rhs->im + lhs->im * rhs->re},
                out);
}

// Arithmetic: complex_divide(a, b) -> COMPLEX
void complex_divide_impl(vef_context_t *ctx, vef_invalue_t *in_l,
                         vef_invalue_t *in_r, vef_vdf_result_t *out) {
  if (in_l->is_null || in_r->is_null) {
    out->type = VEF_RESULT_NULL;
    return;
  }

  const auto lhs = TryLoadFromInValue(in_l);
  if (!lhs.has_value()) {
    ReturnError("left argument malformed", out);
    return;
  }

  const auto rhs = TryLoadFromInValue(in_r);
  if (!rhs.has_value()) {
    ReturnError("right argument malformed", out);
    return;
  }

  if (out->max_bin_len < kComplexSize) {
    ReturnError("response buffer too small", out);
    return;
  }

  // Check for division by zero
  double denominator = rhs->re * rhs->re + rhs->im * rhs->im;
  if (denominator == 0.0) {
    ReturnError("division by 0", out);
    return;
  }

  // (a + bi) / (c + di) = [(ac + bd) + (bc - ad)i] / (c^2 + d^2)
  ReturnComplex(Complex{(lhs->re * rhs->re + lhs->im * rhs->im) / denominator,
                        (lhs->im * rhs->re - lhs->re * rhs->im) / denominator},
                out);
}

// Utility: complex_real(c) -> REAL
void complex_real_impl(vef_context_t *ctx, vef_invalue_t *in,
                       vef_vdf_result_t *out) {
  if (in->is_null) {
    out->type = VEF_RESULT_NULL;
    return;
  }

  const auto cx = TryLoadFromInValue(in);
  if (!cx.has_value()) {
    ReturnError("argument malformed", out);
    return;
  }

  out->real_value = cx->re;
  out->type = VEF_RESULT_VALUE;
}

// Utility: complex_imag(c) -> REAL
void complex_imag_impl(vef_context_t *ctx, vef_invalue_t *in,
                       vef_vdf_result_t *out) {
  if (in->is_null) {
    out->type = VEF_RESULT_NULL;
    return;
  }

  const auto cx = TryLoadFromInValue(in);
  if (!cx.has_value()) {
    ReturnError("argument malformed", out);
    return;
  }

  out->real_value = cx->im;
  out->type = VEF_RESULT_VALUE;
}

// Utility: complex_abs(c) -> REAL
void complex_abs_impl(vef_context_t *ctx, vef_invalue_t *in,
                      vef_vdf_result_t *out) {
  if (in->is_null) {
    out->type = VEF_RESULT_NULL;
    return;
  }

  const auto cx = TryLoadFromInValue(in);
  if (!cx.has_value()) {
    ReturnError("argument malformed", out);
    return;
  }

  out->real_value = sqrt(cx->re * cx->re + cx->im * cx->im);
  out->type = VEF_RESULT_VALUE;
}

// Utility: complex_conjugate(c) -> COMPLEX
void complex_conjugate_impl(vef_context_t *ctx, vef_invalue_t *in,
                            vef_vdf_result_t *out) {
  if (in->is_null) {
    out->type = VEF_RESULT_NULL;
    return;
  }

  const auto cx = TryLoadFromInValue(in);
  if (!cx.has_value()) {
    ReturnError("argument malformed", out);
    return;
  }

  if (out->max_bin_len < kComplexSize) {
    ReturnError("response buffer too small", out);
    return;
  }

  ReturnComplex(Complex{cx->re, -cx->im}, out);
}

constexpr const char *COMPLEX = "COMPLEX";
constexpr const char *COMPLEX2 = "COMPLEX2";

VEF_GENERATE_ENTRY_POINTS(
    make_extension("vsql_complex", "0.0.1")
        // COMPLEX type with canonicalization (normalizes -0.0 to +0.0)
        .type(make_type(COMPLEX)
                  .persisted_length(kComplexSize)
                  .max_decode_buffer_length(64)
                  .encode("complex_from_string")
                  .decode("complex_to_string")
                  .compare("complex_compare")
                  .intrinsic_default("complex_intrinsic_default")
                  .build())
        // COMPLEX2 type without canonicalization (preserves -0.0)
        // Requires custom hash that canonicalizes -0 to +0 before hashing
        .type(make_type(COMPLEX2)
                  .persisted_length(kComplexSize)
                  .max_decode_buffer_length(64)
                  .encode("complex2_from_string")
                  .decode("complex2_to_string")
                  .compare("complex2_compare")
                  .hash("complex2_hash")
                  .build())
        // Type conversion functions (also serve as encode/decode VDFs)
        .func(make_type_encode<&complex_from_string>("complex_from_string",
                                                     COMPLEX))
        .func(make_type_decode<&complex_to_string>("complex_to_string",
                                                   COMPLEX))
        .func(make_type_encode<&complex2_from_string>("complex2_from_string",
                                                      COMPLEX2))
        .func(make_type_decode<&complex_to_string>("complex2_to_string",
                                                   COMPLEX2))
        // Compare and hash VDFs
        .func(make_type_compare<&complex_compare>("complex_compare", COMPLEX))
        .func(make_type_compare<&complex_compare>("complex2_compare", COMPLEX2))
        .func(make_func<&complex2_hash>("complex2_hash")
                  .returns(INT)
                  .param(COMPLEX2)
                  .deterministic()
                  .build())
        // Arithmetic functions
        .func(make_func<&complex_add_impl>("complex_add")
                  .returns(COMPLEX)
                  .param(COMPLEX)
                  .param(COMPLEX)
                  .deterministic()
                  .build())
        .func(make_func<&complex_subtract_impl>("complex_subtract")
                  .returns(COMPLEX)
                  .param(COMPLEX)
                  .param(COMPLEX)
                  .deterministic()
                  .build())
        .func(make_func<&complex_multiply_impl>("complex_multiply")
                  .returns(COMPLEX)
                  .param(COMPLEX)
                  .param(COMPLEX)
                  .deterministic()
                  .build())
        .func(make_func<&complex_divide_impl>("complex_divide")
                  .returns(COMPLEX)
                  .param(COMPLEX)
                  .param(COMPLEX)
                  .deterministic()
                  .build())
        // Utility functions
        .func(make_func<&complex_real_impl>("complex_real")
                  .returns(REAL)
                  .param(COMPLEX)
                  .deterministic()
                  .build())
        .func(make_func<&complex_imag_impl>("complex_imag")
                  .returns(REAL)
                  .param(COMPLEX)
                  .deterministic()
                  .build())
        .func(make_func<&complex_abs_impl>("complex_abs")
                  .returns(REAL)
                  .param(COMPLEX)
                  .deterministic()
                  .build())
        .func(make_func<&complex_conjugate_impl>("complex_conjugate")
                  .returns(COMPLEX)
                  .param(COMPLEX)
                  .deterministic()
                  .build())
        .func(make_intrinsic_default<&complex_default>(
            "complex_intrinsic_default", COMPLEX)))
