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

// The VillageSQL SVECTOR extension provides a vector data type (SVECTOR) with
// external column storage for SVECTOR columns. External storage enables more
// efficient construction and traversal of HNSW indexes for ANN search.

#include <villagesql/extension.h>

#include <cctype>
#include <charconv>
#include <cstdint>
#include <cstring>
#include <limits>

#include "native_vector.h"
#include "storage.h"

using villagesql::BinaryArg;
using villagesql::BinaryResult;
using villagesql::IntResult;
using villagesql::RealResult;
using villagesql::StringArg;
using villagesql::StringResult;

namespace native = svector::native;

// Vector type with separate column storage.
constexpr const char *SVECTOR = "SVECTOR";

// Enough for sign + decimal + exponent + round-trip precision + NaN/Inf
constexpr size_t MAX_FLOAT_STR_LENGTH = 16;

// Decode buffer for a N-element vector: '[' + float [',' float]* + ']' + '\0'
template <size_t N>
constexpr size_t DECODE_BUFFER_SIZE =
    2 + N * MAX_FLOAT_STR_LENGTH + (N - 1) + 1;

// Stack buffer size for AlignedBuffer (4KB - sized to handle typical vectors
// on stack while keeping total stack usage reasonable when multiple buffers
// are allocated. Larger vectors automatically use heap allocation.)
constexpr size_t VECTOR_STACK_BUFFER_SIZE = 4096;

// Verify at compile time that MAX_VECTOR_DIMENSION won't overflow
static_assert(native::MAX_VECTOR_DIMENSION <=
                  (INT64_MAX - 2) / (MAX_FLOAT_STR_LENGTH + 1),
              "MAX_VECTOR_DIMENSION would overflow decode_length()");
static_assert(native::MAX_VECTOR_DIMENSION <=
                  (SIZE_MAX - sizeof(native::Data)) / sizeof(float),
              "MAX_VECTOR_DIMENSION would overflow native vector allocation");

static void svector_from_string(StringArg in, BinaryResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }
  std::string_view sv = in.value();

  auto is_space = [](char c) {
    return std::isspace(static_cast<unsigned char>(c));
  };

  auto trim = [&](std::string_view &s) {
    while (!s.empty() && is_space(s.front())) s.remove_prefix(1);
    while (!s.empty() && is_space(s.back())) s.remove_suffix(1);
  };

  auto skip_ws = [&](std::string_view &s) {
    while (!s.empty() && is_space(s.front())) s.remove_prefix(1);
  };

  trim(sv);

  if (sv.size() < 2 || sv.front() != '[' || sv.back() != ']') {
    out.error("vector must be enclosed in '[' and ']'");
    return;
  }
  std::string_view inner = sv.substr(1, sv.size() - 2);

  size_t count = 0;
  std::string_view parse = inner;

  // First pass: count elements and validate syntax
  while (true) {
    skip_ws(parse);
    if (parse.empty()) break;

    float tmp;
    const char *begin = parse.data();
    const char *end = begin + parse.size();

    auto [next, ec] = std::from_chars(begin, end, tmp);
    if (ec != std::errc()) {
      out.error("invalid float value in vector");
      return;
    }
    ++count;
    parse.remove_prefix(next - begin);

    skip_ws(parse);
    if (parse.empty()) break;

    if (parse.front() != ',') {
      out.error("expected ',' between vector elements");
      return;
    }
    parse.remove_prefix(1);
  }

  if (count == 0) {
    out.error("vector must have at least one element");
    return;
  }

  if (count > native::MAX_VECTOR_DIMENSION) {
    out.error("vector dimension exceeds maximum");
    return;
  }
  size_t vector_size = sizeof(vef_storage_ref_t) + count * sizeof(float);
  auto buf = out.buffer();
  if (buf.size() < vector_size) {
    out.error("output buffer too small for vector");
    return;
  }

  std::memset(buf.data(), 0, sizeof(vef_storage_ref_t));
  unsigned char *dst = buf.data() + sizeof(vef_storage_ref_t);

  // Second pass: write float values
  parse = inner;

  while (true) {
    skip_ws(parse);
    if (parse.empty()) break;

    float value;
    const char *begin = parse.data();
    const char *end = begin + parse.size();

    auto [next, ec] = std::from_chars(begin, end, value);
    if (ec != std::errc()) {
      out.error("invalid float value in vector");
      return;
    }
    native::float4store(dst, value);
    dst += sizeof(float);

    parse.remove_prefix(next - begin);

    skip_ws(parse);
    if (parse.empty()) break;

    // skip comma
    parse.remove_prefix(1);
  }
  out.set_length(vector_size);
}

static void svector_to_string(BinaryArg in, StringResult out) {
  if (in.is_null()) {
    out.set_null();
    return;
  }
  auto data = in.value();

  if (data.size() < sizeof(vef_storage_ref_t)) {
    out.error("encoded vector is too short");
    return;
  }
  const size_t payload = data.size() - sizeof(vef_storage_ref_t);

  if (payload % sizeof(float) != 0) {
    out.error("encoded vector has invalid size");
    return;
  }
  const size_t count = payload / sizeof(float);

  if (count == 0 || count > native::MAX_VECTOR_DIMENSION) {
    out.error("invalid vector dimension in encoded data");
    return;
  }
  auto buf = out.buffer();
  size_t pos = 0;

  auto emit = [&](char c) {
    if (pos >= buf.size()) return false;
    buf[pos++] = c;
    return true;
  };
  auto emit_str = [&](const char *s, size_t n) {
    if (pos + n > buf.size()) return false;
    std::memcpy(buf.data() + pos, s, n);
    pos += n;
    return true;
  };
  if (!emit('[')) {
    out.error("output buffer too small");
    return;
  }
  const unsigned char *src = data.data() + sizeof(vef_storage_ref_t);

  for (size_t i = 0; i < count; ++i) {
    if (i > 0) {
      if (!emit(',')) {
        out.error("output buffer too small");
        return;
      }
    }
    float f = native::float4get(src + i * sizeof(float));

    char tmp[MAX_FLOAT_STR_LENGTH];
    auto [ptr, ec] =
        std::to_chars(tmp, tmp + sizeof(tmp), f, std::chars_format::general,
                      std::numeric_limits<float>::max_digits10);

    if (ec != std::errc()) {
      out.error("failed to convert float to string");
      return;
    }
    if (!emit_str(tmp, static_cast<size_t>(ptr - tmp))) {
      out.error("output buffer too small");
      return;
    }
  }
  if (!emit(']')) {
    out.error("output buffer too small");
    return;
  }
  out.set_length(pos);
}

static void svector_compare(BinaryArg in_l, BinaryArg in_r, IntResult out) {
  // NULL comparison decisions are taken by server and the routine should
  // not be called with NULL parameter.
  assert(!in_l.is_null());
  assert(!in_r.is_null());

  auto lhs = in_l.value();
  auto rhs = in_r.value();

  assert(lhs.size() >= sizeof(vef_storage_ref_t));
  assert(rhs.size() >= sizeof(vef_storage_ref_t));

  const size_t dim_l = (lhs.size() - sizeof(vef_storage_ref_t)) / sizeof(float);
  const size_t dim_r = (rhs.size() - sizeof(vef_storage_ref_t)) / sizeof(float);

  const unsigned char *p_l = lhs.data() + sizeof(vef_storage_ref_t);
  const unsigned char *p_r = rhs.data() + sizeof(vef_storage_ref_t);
  const size_t min_dim = std::min(dim_l, dim_r);

  // Vectors of different dimensions are compared lexicographically, treating
  // missing trailing elements of the shorter vector as 0. For example,
  // [1.0, 2.0] and [1.0, 2.0, 0.0] are considered equal, while [1.0, 2.0]
  // is less than [1.0, 2.0, 1.0]. This does not imply geometric equivalence.

  // Compare common prefix
  for (size_t i = 0; i < min_dim; ++i) {
    float f_l = native::float4get(p_l);
    float f_r = native::float4get(p_r);

    if (f_l < f_r) {
      out.set(-1);
      return;
    }
    if (f_l > f_r) {
      out.set(1);
      return;
    }
    p_l += sizeof(float);
    p_r += sizeof(float);
  }
  // Handle remaining elements (treated as 0)
  if (dim_l > dim_r) {
    for (size_t i = min_dim; i < dim_l; ++i) {
      float f_l = native::float4get(p_l);
      if (f_l != 0.0f) {
        out.set(f_l > 0.0f ? 1 : -1);
        return;
      }
      p_l += sizeof(float);
    }
  } else if (dim_r > dim_l) {
    for (size_t i = min_dim; i < dim_r; ++i) {
      float f_r = native::float4get(p_r);
      if (f_r != 0.0f) {
        out.set(f_r > 0.0f ? -1 : 1);
        return;
      }
      p_r += sizeof(float);
    }
  }
  out.set(0);
}

static bool svector_validate_dimension(int64_t dimension, char *error_msg) {
  if (dimension <= 0) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "VECTOR: Dimension must be positive, got %" PRId64, dimension);
    return true;
  }
  if (dimension > native::MAX_VECTOR_DIMENSION) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "VECTOR: Dimension %" PRId64 " exceeds maximum allowed (%d)",
             dimension, native::MAX_VECTOR_DIMENSION);
    return true;
  }
  return false;
}

static bool svector_get_params(int64_t dimension,
                               std::map<std::string, std::string> &params,
                               char *error_msg) {
  if (svector_validate_dimension(dimension, error_msg)) {
    return true;
  }
  params["dimension"] = std::to_string(dimension);
  return false;
}

static bool svector_resolve_params(
    const std::map<std::string, std::string> &params,
    villagesql::ResolvedTypeParams *result, char *error_msg) {
  // Ensure only "dimension" is present
  if (params.size() != 1 || params.find("dimension") == params.end()) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "SVECTOR: Only parameter 'dimension' is allowed");
    return true;
  }
  const std::string &dim_str = params.at("dimension");
  char *endptr = nullptr;
  errno = 0;
  int64_t dimension = strtoll(dim_str.c_str(), &endptr, 10);

  // Check for conversion errors
  if (endptr == dim_str.c_str() || *endptr != '\0' || errno == ERANGE) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "SVECTOR: 'dimension' must be a "
             "valid integer, got '%s'",
             dim_str.c_str());
    return true;
  }
  if (svector_validate_dimension(dimension, error_msg)) {
    return true;
  }
  result->persisted_length =
      sizeof(vef_storage_ref_t) + dimension * sizeof(float);

  // Includes [], floats, commas and terminating '\0'
  result->max_decode_buffer_length =
      2 + dimension * MAX_FLOAT_STR_LENGTH + (dimension - 1) + 1;
  return false;
}

// SQL Functions (VDF) - Implementations

// Calculate euclidean norm (L2) of a vector
void svector_norm(BinaryArg vec, RealResult out) {
  if (vec.is_null()) {
    out.set_null();
    return;
  }
  auto data = vec.value();
  if (data.size() < sizeof(vef_storage_ref_t)) {
    out.error("encoded vector is too short");
    return;
  }
  const unsigned char *floats = data.data() + sizeof(vef_storage_ref_t);
  size_t floats_len = data.size() - sizeof(vef_storage_ref_t);
  size_t count = floats_len / sizeof(float);
  native::Length native_len = native::length(count);
  native::AlignedBuffer<VECTOR_STACK_BUFFER_SIZE> buffer(native_len.length,
                                                         native_len.alignment);
  if (!buffer.is_initialized()) {
    out.error("buffer allocation failed");
    return;
  }
  if (native::from_encoded(floats, floats_len, buffer.get(), buffer.size())) {
    out.error("malformed vector");
    return;
  }
  const native::Data *v = static_cast<const native::Data *>(buffer.get());
  out.set(native::norm_l2(v));
}

// Get the dimension of a vector
void svector_dims(BinaryArg vec, IntResult out) {
  if (vec.is_null()) {
    out.set_null();
    return;
  }
  auto data = vec.value();
  if (data.size() < sizeof(vef_storage_ref_t)) {
    out.error("encoded vector is too short");
    return;
  }
  const unsigned char *floats = data.data() + sizeof(vef_storage_ref_t);
  size_t floats_len = data.size() - sizeof(vef_storage_ref_t);
  size_t count = floats_len / sizeof(float);
  native::Length native_len = native::length(count);
  native::AlignedBuffer<VECTOR_STACK_BUFFER_SIZE> buffer(native_len.length,
                                                         native_len.alignment);
  if (!buffer.is_initialized()) {
    out.error("buffer allocation failed");
    return;
  }
  if (native::from_encoded(floats, floats_len, buffer.get(), buffer.size())) {
    out.error("malformed vector");
    return;
  }
  const native::Data *v = static_cast<const native::Data *>(buffer.get());
  out.set(v->dim);
}

// Get the maximum supported dimension for vectors
void svector_max_dims(IntResult out) { out.set(native::MAX_VECTOR_DIMENSION); }

// Common implementation for vector distance SQL functions
static void svector_distance_impl(BinaryArg vec1, BinaryArg vec2,
                                  RealResult out,
                                  double (*dist_func)(const native::Data *,
                                                      const native::Data *)) {
  if (vec1.is_null() || vec2.is_null()) {
    out.set_null();
    return;
  }

  auto data1 = vec1.value();
  auto data2 = vec2.value();

  const unsigned char *floats1 = data1.data() + sizeof(vef_storage_ref_t);
  size_t floats_len1 = data1.size() - sizeof(vef_storage_ref_t);
  const unsigned char *floats2 = data2.data() + sizeof(vef_storage_ref_t);
  size_t floats_len2 = data2.size() - sizeof(vef_storage_ref_t);

  // Get native lengths for both vectors
  native::Length native_len1 = native::length(floats_len1 / sizeof(float));
  native::Length native_len2 = native::length(floats_len2 / sizeof(float));

  // Allocate aligned buffers for native representations
  native::AlignedBuffer<VECTOR_STACK_BUFFER_SIZE> buffer1(
      native_len1.length, native_len1.alignment);
  native::AlignedBuffer<VECTOR_STACK_BUFFER_SIZE> buffer2(
      native_len2.length, native_len2.alignment);
  if (!buffer1.is_initialized() || !buffer2.is_initialized()) {
    out.error("buffer allocation failed");
    return;
  }

  if (native::from_encoded(floats1, floats_len1, buffer1.get(),
                           buffer1.size())) {
    out.error("malformed vector");
    return;
  }
  if (native::from_encoded(floats2, floats_len2, buffer2.get(),
                           buffer2.size())) {
    out.error("malformed vector");
    return;
  }

  const native::Data *v1 = static_cast<const native::Data *>(buffer1.get());
  const native::Data *v2 = static_cast<const native::Data *>(buffer2.get());

  if (v1->dim != v2->dim) {
    out.error("dimension mismatch");
    return;
  }

  out.set(dist_func(v1, v2));
}

// Calculate L1 distance between two vectors
void svector_l1_distance(BinaryArg vec1, BinaryArg vec2, RealResult out) {
  svector_distance_impl(vec1, vec2, out, native::dist_l1);
}

// Calculate L2 distance between two vectors
void svector_l2_distance(BinaryArg vec1, BinaryArg vec2, RealResult out) {
  svector_distance_impl(vec1, vec2, out, native::dist_l2);
}

// Calculate cosine distance between two vectors
void svector_cosine_distance(BinaryArg vec1, BinaryArg vec2, RealResult out) {
  svector_distance_impl(vec1, vec2, out, native::dist_cosine);
}

// Calculate inner product between two vectors
void svector_inner_product(BinaryArg vec1, BinaryArg vec2, RealResult out) {
  svector_distance_impl(vec1, vec2, out, native::dist_inner_product);
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension("vsql_svector", "0.0.1")
        .type(make_type(SVECTOR)
                  // Data length related functions
                  .persisted_length(-1)
                  .max_decode_buffer_length(DECODE_BUFFER_SIZE<16>)

                  // Data conversion and compare
                  .encode("svector_from_string")
                  .decode("svector_to_string")
                  .compare("svector_compare")
                  .int_to_params("svector_get_params")
                  .resolve_params("svector_resolve_params")

                  // Storage interface
                  .column_storage(
                      make_storage()
                          .create(&svector::ColumnStorage::create)
                          .drop(&svector::ColumnStorage::drop)
                          .load(&svector::ColumnStorage::load)
                          .insert(&svector::ColumnStorage::insert)
                          .select(&svector::ColumnStorage::select)
                          .mark_delete(&svector::ColumnStorage::mark_delete)
                          .purge(&svector::ColumnStorage::purge)
                          .build())
                  .build())

        // Type conversion functions (SQL)
        .func(make_func<&svector_from_string>("svector_from_string")
                  .returns(SVECTOR)
                  .param(STRING)
                  .deterministic()
                  .build())
        .func(make_func<&svector_to_string>("svector_to_string")
                  .returns(STRING)
                  .param(SVECTOR)
                  .deterministic()
                  .build())
        .func(make_func<&svector_compare>("svector_compare")
                  .returns(INT)
                  .param(SVECTOR)
                  .param(SVECTOR)
                  .deterministic()
                  .build())

        // Vector euclidean norm (L2) function (SQL)
        .func(make_func<&svector_norm>("svector_norm")
                  .returns(REAL)
                  .param(SVECTOR)
                  .build())

        // Vector dimension function (SQL)
        .func(make_func<&svector_dims>("svector_dimension")
                  .returns(INT)
                  .param(SVECTOR)
                  .deterministic()
                  .build())

        // Maximum supported dimension function (SQL)
        .func(make_func<&svector_max_dims>("svector_max_dimension")
                  .returns(INT)
                  .deterministic()
                  .build())

        // Vector distance functions (SQL)
        .func(make_func<&svector_l1_distance>("svector_l1_distance")
                  .returns(REAL)
                  .param(SVECTOR)
                  .param(SVECTOR)
                  .deterministic()
                  .build())
        .func(make_func<&svector_l2_distance>("svector_l2_distance")
                  .returns(REAL)
                  .param(SVECTOR)
                  .param(SVECTOR)
                  .deterministic()
                  .build())
        .func(make_func<&svector_cosine_distance>("svector_cosine_distance")
                  .returns(REAL)
                  .param(SVECTOR)
                  .param(SVECTOR)
                  .deterministic()
                  .build())
        .func(make_func<&svector_inner_product>("svector_inner_product")
                  .returns(REAL)
                  .param(SVECTOR)
                  .param(SVECTOR)
                  .deterministic()
                  .build())
        .func(make_int_to_params<&svector_get_params>("svector_get_params"))
        .func(make_resolve_params<&svector_resolve_params>(
            "svector_resolve_params")))
