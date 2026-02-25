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
// The TVECTOR(N) type stores fixed-dimension float32 vectors.
// TVECTOR(3) means 3 float32 elements, stored as 12 bytes.
// Text format: "[1.0,2.0,3.0]" (comma-separated floats in brackets)

#include <villagesql/extension.h>

#include <cinttypes>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>

// Little-endian float32 store/load helpers.

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

// Convert TYPE(N) integer to parameter key-value pairs.
// Produces {key:"dimension", value:"<N>"}.
bool tvector_int_to_params(int64_t value, vef_type_param_t *params,
                           size_t *param_count, char *error_msg) {
  if (value <= 0) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "TVECTOR dimension must be a positive integer, got %" PRId64,
             value);
    return true;
  }
  static char dim_buf[32];
  snprintf(dim_buf, sizeof(dim_buf), "%" PRId64, value);
  params[0] = {"dimension", dim_buf};
  *param_count = 1;
  return false;
}

// Validate type parameters and compute storage characteristics.
bool tvector_resolve_params(const vef_type_param_t *params, size_t param_count,
                            vef_type_resolved_params_t *result,
                            char *error_msg) {
  int64_t dimension = 0;
  for (size_t i = 0; i < param_count; i++) {
    if (strcmp(params[i].key, "dimension") == 0) {
      char *endptr = nullptr;
      dimension = strtoll(params[i].value, &endptr, 10);
      if (endptr == params[i].value || *endptr != '\0') {
        snprintf(error_msg, VEF_MAX_ERROR_LEN,
                 "TVECTOR: invalid dimension value '%s'", params[i].value);
        return true;
      }
      break;
    }
  }
  if (dimension <= 0) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "TVECTOR: dimension must be a positive integer");
    return true;
  }
  result->persisted_length = dimension * 4;
  result->max_decode_buffer_length = dimension * 16;
  return false;
}

void ReturnError(const char *err_msg, vef_vdf_result_t *result) {
  result->type = VEF_RESULT_ERROR;
  snprintf(result->error_msg, VEF_MAX_ERROR_LEN, "%s", err_msg);
}

// Encode: "[f1,f2,...,fN]" -> N * 4 bytes binary.
// STRING -> TVECTOR
// Dimension is inferred from out->max_bin_len / 4.
void tvector_from_string(vef_context_t *ctx, vef_invalue_t *in,
                         vef_vdf_result_t *out) {
  if (in->is_null) {
    out->type = VEF_RESULT_NULL;
    return;
  }
  if (out->max_bin_len == 0) {
    ReturnError("response buffer too small", out);
    return;
  }

  size_t dimension = out->max_bin_len / 4;

  std::string input(in->str_value, in->str_len);
  const char *p = input.c_str();

  // Skip leading whitespace
  while (*p == ' ') p++;
  if (*p != '[') {
    ReturnError("expected '[' at start of vector", out);
    return;
  }
  p++;

  size_t count = 0;
  while (*p != '\0') {
    // Skip whitespace
    while (*p == ' ') p++;
    if (*p == ']') break;

    if (count >= dimension) {
      ReturnError("too many elements", out);
      return;
    }

    char *endptr = nullptr;
    float val = strtof(p, &endptr);
    if (endptr == p) {
      ReturnError("failed to parse float", out);
      return;
    }
    store_float(out->bin_buf + count * 4, val);
    count++;
    p = endptr;

    // Skip whitespace and comma
    while (*p == ' ') p++;
    if (*p == ',') p++;
  }

  if (*p != ']') {
    ReturnError("expected ']' at end of vector", out);
    return;
  }

  if (count != dimension) {
    ReturnError("wrong number of elements", out);
    return;
  }

  out->actual_len = dimension * 4;
  out->type = VEF_RESULT_VALUE;
}

// Decode: N * 4 bytes binary -> "[f1,f2,...,fN]" string.
// TVECTOR -> STRING
void tvector_to_string(vef_context_t *ctx, vef_invalue_t *in,
                       vef_vdf_result_t *out) {
  if (in->is_null) {
    out->type = VEF_RESULT_NULL;
    return;
  }

  size_t dimension = in->bin_len / 4;
  if (dimension == 0 || in->bin_len % 4 != 0) {
    ReturnError("argument malformed", out);
    return;
  }

  size_t pos = 0;
  if (pos >= out->max_str_len) {
    ReturnError("output buffer too small", out);
    return;
  }
  out->str_buf[pos++] = '[';

  for (size_t i = 0; i < dimension; i++) {
    if (i > 0) {
      if (pos >= out->max_str_len) {
        ReturnError("output buffer too small", out);
        return;
      }
      out->str_buf[pos++] = ',';
    }
    float val = load_float(in->bin_value + i * 4);
    int written =
        snprintf(out->str_buf + pos, out->max_str_len - pos, "%g", val);
    if (written < 0 || pos + static_cast<size_t>(written) >= out->max_str_len) {
      ReturnError("output buffer too small", out);
      return;
    }
    pos += static_cast<size_t>(written);
  }

  if (pos >= out->max_str_len) {
    ReturnError("output buffer too small", out);
    return;
  }
  out->str_buf[pos++] = ']';

  out->actual_len = pos;
  out->type = VEF_RESULT_VALUE;
}

// Compare VDF for ORDER BY, indexes: (TVECTOR, TVECTOR) -> INT
// Lexicographic element-by-element comparison.
void tvector_compare(vef_context_t *ctx, vef_invalue_t *in_l,
                     vef_invalue_t *in_r, vef_vdf_result_t *out) {
  size_t dim1 = in_l->bin_len / 4;
  size_t dim2 = in_r->bin_len / 4;
  size_t min_dim = dim1 < dim2 ? dim1 : dim2;

  for (size_t i = 0; i < min_dim; i++) {
    float v1 = load_float(in_l->bin_value + i * 4);
    float v2 = load_float(in_r->bin_value + i * 4);
    if (v1 < v2) {
      out->int_value = -1;
      out->type = VEF_RESULT_VALUE;
      return;
    }
    if (v1 > v2) {
      out->int_value = 1;
      out->type = VEF_RESULT_VALUE;
      return;
    }
  }

  if (dim1 < dim2)
    out->int_value = -1;
  else if (dim1 > dim2)
    out->int_value = 1;
  else
    out->int_value = 0;
  out->type = VEF_RESULT_VALUE;
}

constexpr const char *TVECTOR = "TVECTOR";

VEF_GENERATE_ENTRY_POINTS(
    make_extension("vsql_tvector", "0.0.1")
        .type(make_type(TVECTOR)
                  .persisted_length(-1)
                  .max_decode_buffer_length(16)
                  .encode("tvector_from_string")
                  .decode("tvector_to_string")
                  .compare("tvector_compare")
                  .int_to_params(&tvector_int_to_params)
                  .resolve_params(&tvector_resolve_params)
                  .build())
        .func(make_func<&tvector_from_string>("tvector_from_string")
                  .returns(TVECTOR)
                  .param(STRING)
                  .deterministic()
                  .build())
        .func(make_func<&tvector_to_string>("tvector_to_string")
                  .returns(STRING)
                  .param(TVECTOR)
                  .deterministic()
                  .build())
        .func(make_func<&tvector_compare>("tvector_compare")
                  .returns(INT)
                  .param(TVECTOR)
                  .param(TVECTOR)
                  .deterministic()
                  .build()))
