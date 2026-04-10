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

#include <villagesql/vsql.h>

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <string>
#include <string_view>

using namespace vsql;

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

// Hash: COMPLEX2 -> size_t
// Canonicalizes -0 to +0 before hashing so that -0.0 and +0.0 hash to the
// same bucket. This allows COMPLEX2 to preserve -0 in storage while still
// working correctly with hash joins and EXCEPT operations.
size_t complex2_hash(villagesql::Span<const unsigned char> data) {
  if (data.size() != kComplexSize) return 0;
  Complex cx = load_complex(data.data());
  cx.canonicalize();

  unsigned char canonical[kComplexSize];
  store_complex(canonical, cx);
  return fnv1a_hash(canonical, kComplexSize);
}

// Arithmetic: complex_add(a, b) -> COMPLEX
void complex_add(CustomArg in_l, CustomArg in_r, CustomResult out) {
  if (in_l.is_null() || in_r.is_null()) {
    out.set_null();
    return;
  }
  if (in_l.value().size() != kComplexSize) {
    out.error("left argument malformed");
    return;
  }
  if (in_r.value().size() != kComplexSize) {
    out.error("right argument malformed");
    return;
  }
  if (out.buffer().size() < kComplexSize) {
    out.error("response buffer too small");
    return;
  }
  Complex lhs = load_complex(in_l.value().data());
  Complex rhs = load_complex(in_r.value().data());
  store_complex(out.buffer().data(), Complex{lhs.re + rhs.re, lhs.im + rhs.im});
  out.set_length(kComplexSize);
}

// Arithmetic: complex_subtract(a, b) -> COMPLEX
void complex_subtract(CustomArg in_l, CustomArg in_r, CustomResult out) {
  if (in_l.is_null() || in_r.is_null()) {
    out.set_null();
    return;
  }
  if (in_l.value().size() != kComplexSize) {
    out.error("left argument malformed");
    return;
  }
  if (in_r.value().size() != kComplexSize) {
    out.error("right argument malformed");
    return;
  }
  if (out.buffer().size() < kComplexSize) {
    out.error("response buffer too small");
    return;
  }
  Complex lhs = load_complex(in_l.value().data());
  Complex rhs = load_complex(in_r.value().data());
  store_complex(out.buffer().data(), Complex{lhs.re - rhs.re, lhs.im - rhs.im});
  out.set_length(kComplexSize);
}

// Arithmetic: complex_multiply(a, b) -> COMPLEX
void complex_multiply(CustomArg in_l, CustomArg in_r, CustomResult out) {
  if (in_l.is_null() || in_r.is_null()) {
    out.set_null();
    return;
  }
  if (in_l.value().size() != kComplexSize) {
    out.error("left argument malformed");
    return;
  }
  if (in_r.value().size() != kComplexSize) {
    out.error("right argument malformed");
    return;
  }
  if (out.buffer().size() < kComplexSize) {
    out.error("response buffer too small");
    return;
  }
  Complex lhs = load_complex(in_l.value().data());
  Complex rhs = load_complex(in_r.value().data());
  // (a + bi) * (c + di) = (ac - bd) + (ad + bc)i
  store_complex(out.buffer().data(),
                Complex{lhs.re * rhs.re - lhs.im * rhs.im,
                        lhs.re * rhs.im + lhs.im * rhs.re});
  out.set_length(kComplexSize);
}

// Arithmetic: complex_divide(a, b) -> COMPLEX
void complex_divide(CustomArg in_l, CustomArg in_r, CustomResult out) {
  if (in_l.is_null() || in_r.is_null()) {
    out.set_null();
    return;
  }
  if (in_l.value().size() != kComplexSize) {
    out.error("left argument malformed");
    return;
  }
  if (in_r.value().size() != kComplexSize) {
    out.error("right argument malformed");
    return;
  }
  if (out.buffer().size() < kComplexSize) {
    out.error("response buffer too small");
    return;
  }
  Complex lhs = load_complex(in_l.value().data());
  Complex rhs = load_complex(in_r.value().data());
  // Check for division by zero
  double denominator = rhs.re * rhs.re + rhs.im * rhs.im;
  if (denominator == 0.0) {
    out.warning("division by 0");
    return;
  }
  // (a + bi) / (c + di) = [(ac + bd) + (bc - ad)i] / (c^2 + d^2)
  store_complex(out.buffer().data(),
                Complex{(lhs.re * rhs.re + lhs.im * rhs.im) / denominator,
                        (lhs.im * rhs.re - lhs.re * rhs.im) / denominator});
  out.set_length(kComplexSize);
}

// Utility: complex_real(c) -> REAL
void complex_real(CustomArg in, RealResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }
  if (in.value().size() != kComplexSize) {
    out.error("argument malformed");
    return;
  }
  out.set(load_complex(in.value().data()).re);
}

// Utility: complex_imag(c) -> REAL
void complex_imag(CustomArg in, RealResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }
  if (in.value().size() != kComplexSize) {
    out.error("argument malformed");
    return;
  }
  out.set(load_complex(in.value().data()).im);
}

// Utility: complex_abs(c) -> REAL
void complex_abs(CustomArg in, RealResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }
  if (in.value().size() != kComplexSize) {
    out.error("argument malformed");
    return;
  }
  Complex cx = load_complex(in.value().data());
  out.set(sqrt(cx.re * cx.re + cx.im * cx.im));
}

// Utility: complex_conjugate(c) -> COMPLEX
void complex_conjugate(CustomArg in, CustomResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }
  if (in.value().size() != kComplexSize) {
    out.error("argument malformed");
    return;
  }
  if (out.buffer().size() < kComplexSize) {
    out.error("response buffer too small");
    return;
  }
  Complex cx = load_complex(in.value().data());
  store_complex(out.buffer().data(), Complex{cx.re, -cx.im});
  out.set_length(kComplexSize);
}

// Aggregate: complex_sum(COMPLEX) -> COMPLEX
// Sums complex values component-wise. Returns NULL for empty groups.

using ComplexSumState = std::optional<Complex>;

void complex_sum_clear(ComplexSumState &state) { state = std::nullopt; }

void complex_sum_accumulate(ComplexSumState &state, CustomArg val) {
  if (val.is_null() || val.value().size() != kComplexSize) return;
  Complex cx = load_complex(val.value().data());
  Complex total = state.value_or(Complex{0.0, 0.0});
  total.re += cx.re;
  total.im += cx.im;
  state = total;
}

// TODO(villagesql-beta): convert to typed style: void(const ComplexSumState&,
// CustomResult)
void complex_sum_result(vef_context_t *ctx, vef_vdf_args_t *args,
                        vef_vdf_result_t *out) {
  auto *state = static_cast<ComplexSumState *>(args->user_data);
  if (!state->has_value()) {
    out->type = VEF_RESULT_NULL;
    return;
  }
  if (out->max_bin_len < kComplexSize) {
    out->type = VEF_RESULT_ERROR;
    snprintf(out->error_msg, VEF_MAX_ERROR_LEN, "response buffer too small");
    return;
  }
  Complex total = state->value();
  total.canonicalize();
  store_complex(out->bin_buf, total);
  out->actual_len = kComplexSize;
  out->type = VEF_RESULT_VALUE;
}

// Type name NTTPs — required for auto-generating VDF names like
// "COMPLEX::from_string" without macros. Each make_type<kName>() call uses
// the pointer identity of kName to key independent static name buffers, so
// two types sharing a function pointer (e.g. complex_to_string) still get
// separate "COMPLEX::to_string" and "COMPLEX2::to_string" buffers.
static constexpr const char kComplexTypeName[] = "COMPLEX";
static constexpr const char kComplex2TypeName[] = "COMPLEX2";

// Type objects: encode/decode/compare/hash VDFs are embedded with
// auto-generated names ("COMPLEX::from_string" etc.) — no manual string
// matching required.
constexpr auto COMPLEX =
    vsql::make_type<kComplexTypeName>()
        .persisted_length(kComplexSize)
        .max_decode_buffer_length(64)
        .from_string<&complex_from_string>()  // auto: "COMPLEX::from_string"
        .to_string<&complex_to_string>()      // auto: "COMPLEX::to_string"
        .compare<&complex_compare>()          // auto: "COMPLEX::compare"
        .intrinsic_default_str("(0,0)")
        .build();

// COMPLEX2 has a custom hash (to canonicalize -0 before hashing)
constexpr auto COMPLEX2 =
    vsql::make_type<kComplex2TypeName>()
        .persisted_length(kComplexSize)
        .max_decode_buffer_length(64)
        .from_string<&complex2_from_string>()  // auto: "COMPLEX2::from_string"
        .to_string<&complex_to_string>()       // auto: "COMPLEX2::to_string"
                                               // (separate buffer)
        .compare<&complex_compare>()           // auto: "COMPLEX2::compare"
        .hash<&complex2_hash>()                // auto: "COMPLEX2::hash"
        .intrinsic_default_str("(0,0)")
        .build();

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        // COMPLEX type with canonicalization (normalizes -0.0 to +0.0)
        .type(COMPLEX)
        // COMPLEX2 type without canonicalization (preserves -0.0)
        // Requires custom hash that canonicalizes -0 to +0 before hashing
        .type(COMPLEX2)
        // Arithmetic functions
        .func(make_func<&complex_add>("complex_add")
                  .returns(COMPLEX)
                  .param(COMPLEX)
                  .param(COMPLEX)
                  .deterministic()
                  .build())
        .func(make_func<&complex_subtract>("complex_subtract")
                  .returns(COMPLEX)
                  .param(COMPLEX)
                  .param(COMPLEX)
                  .deterministic()
                  .build())
        .func(make_func<&complex_multiply>("complex_multiply")
                  .returns(COMPLEX)
                  .param(COMPLEX)
                  .param(COMPLEX)
                  .deterministic()
                  .build())
        .func(make_func<&complex_divide>("complex_divide")
                  .returns(COMPLEX)
                  .param(COMPLEX)
                  .param(COMPLEX)
                  .deterministic()
                  .build())
        // Utility functions
        .func(make_func<&complex_real>("complex_real")
                  .returns(REAL)
                  .param(COMPLEX)
                  .deterministic()
                  .build())
        .func(make_func<&complex_imag>("complex_imag")
                  .returns(REAL)
                  .param(COMPLEX)
                  .deterministic()
                  .build())
        .func(make_func<&complex_abs>("complex_abs")
                  .returns(REAL)
                  .param(COMPLEX)
                  .deterministic()
                  .build())
        .func(make_func<&complex_conjugate>("complex_conjugate")
                  .returns(COMPLEX)
                  .param(COMPLEX)
                  .deterministic()
                  .build())
        // Aggregate functions
        .func(make_func<&complex_sum_result>("complex_sum")
                  .returns(COMPLEX)
                  .param(COMPLEX)
                  .state<ComplexSumState>()
                  .clear<&complex_sum_clear>()
                  .accumulate<&complex_sum_accumulate>()
                  .build()))
