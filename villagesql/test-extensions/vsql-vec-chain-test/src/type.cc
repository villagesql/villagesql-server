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

#include "type.h"

#include <cinttypes>
#include <cmath>
#include <cstdio>
#include <cstring>

VecChainParams VecChainParams::parse(
    const std::map<std::string, std::string> &params) {
  auto dim_it = params.find("dimension");
  int64_t dim = strtoll(dim_it->second.c_str(), nullptr, 10);
  size_t bytes = 4;
  auto type_it = params.find("type");
  if (type_it != params.end() && type_it->second == "double") bytes = 8;
  return VecChainParams{.dimension = dim, .bytes_per_elem = bytes};
}

void VecChainParams::to_strings(const VecChainParams &p,
                                std::map<std::string, std::string> &out) {
  out["dimension"] = std::to_string(p.dimension);
  out["type"] = (p.bytes_per_elem == 8) ? "double" : "float";
}

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
  for (int i = 0; i < 8; i++) {
    buf[i] = static_cast<unsigned char>(bits >> (i * 8));
  }
}

double load_double(const unsigned char *buf) {
  uint64_t bits = 0;
  for (int i = 0; i < 8; i++) {
    bits |= static_cast<uint64_t>(buf[i]) << (i * 8);
  }
  double val;
  memcpy(&val, &bits, sizeof(double));
  return val;
}

static size_t bytes_per_element(
    const std::map<std::string, std::string> &params) {
  auto it = params.find("type");
  if (it != params.end() && it->second == "double") return 8;
  return 4;
}

bool vec_chain_int_to_params(int64_t value,
                             std::map<std::string, std::string> &params,
                             char *error_msg) {
  if (value <= 0 || value > kVecChainMaxDimension) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "VSQL_VEC_CHAIN dimension must be in 1..%" PRId64 ", got %" PRId64,
             kVecChainMaxDimension, value);
    return true;
  }
  params["dimension"] = std::to_string(value);
  params["type"] = "float";
  return false;
}

bool vec_chain_resolve_params(const std::map<std::string, std::string> &params,
                              vsql::ResolvedTypeParams *result,
                              char *error_msg) {
  auto it = params.find("dimension");
  if (it == params.end()) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "VSQL_VEC_CHAIN: dimension must be a positive integer");
    return true;
  }
  char *endptr = nullptr;
  int64_t dimension = strtoll(it->second.c_str(), &endptr, 10);
  if (endptr == it->second.c_str() || *endptr != '\0') {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "VSQL_VEC_CHAIN: invalid dimension value '%s'",
             it->second.c_str());
    return true;
  }
  if (dimension <= 0 || dimension > kVecChainMaxDimension) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "VSQL_VEC_CHAIN: dimension must be in 1..%" PRId64
             ", got %" PRId64,
             kVecChainMaxDimension, dimension);
    return true;
  }
  auto type_it = params.find("type");
  if (type_it != params.end() && type_it->second != "float" &&
      type_it->second != "double") {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "VSQL_VEC_CHAIN: type must be 'float' or 'double', got '%s'",
             type_it->second.c_str());
    return true;
  }
  size_t bpe = bytes_per_element(params);
  result->persisted_length = dimension * static_cast<int64_t>(bpe);
  result->max_decode_buffer_length = dimension * (bpe == 8 ? 32 : 16);
  return false;
}

void vec_chain_from_string(vsql::MaybeParams<VecChainParams> &p,
                           std::string_view from, vsql::CustomResult out) {
  std::string input(from);
  const char *s = input.c_str();
  while (*s == ' ') s++;
  if (*s != '[') {
    out.warning("vec_chain_from_string: missing '['");
    return;
  }
  s++;

  auto buf = out.buffer();
  const size_t bpe = (p.is_known() && p.value().bytes_per_elem > 0)
                         ? p.value().bytes_per_elem
                         : 4;
  const size_t max_supportable = buf.size() / bpe;

  size_t count = 0;
  while (*s != '\0') {
    while (*s == ' ') s++;
    if (*s == ']') break;

    if (count >= max_supportable) {
      out.warning("vec_chain_from_string: buffer too small");
      return;
    }

    char *endptr = nullptr;
    if (bpe == 8) {
      double val = strtod(s, &endptr);
      if (endptr == s) {
        out.warning("vec_chain_from_string: parse error");
        return;
      }
      store_double(buf.data() + count * 8, val);
    } else {
      float val = strtof(s, &endptr);
      if (endptr == s) {
        out.warning("vec_chain_from_string: parse error");
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
    out.warning("vec_chain_from_string: missing ']'");
    return;
  }

  if (p.is_known()) {
    if (count != static_cast<size_t>(p.value().dimension)) {
      out.warning("vec_chain_from_string: dimension mismatch");
      return;
    }
  } else {
    p.set(VecChainParams{.dimension = static_cast<int64_t>(count),
                         .bytes_per_elem = 4});
  }

  out.set_length(count * bpe);
}

void vec_chain_to_string(vsql::CustomArgWith<VecChainParams> in,
                         vsql::StringResult out) {
  const VecChainParams &p = in.params();
  auto data = in.value();
  const size_t bpe = p.bytes_per_elem;
  if (data.size() != static_cast<size_t>(p.dimension) * bpe) return;

  auto buf = out.buffer();
  size_t pos = 0;
  if (pos >= buf.size()) return;
  buf[pos++] = '[';

  for (size_t i = 0; i < static_cast<size_t>(p.dimension); i++) {
    if (i > 0) {
      if (pos >= buf.size()) return;
      buf[pos++] = ',';
    }
    int written;
    if (bpe == 8) {
      double val = load_double(data.data() + i * bpe);
      written = snprintf(buf.data() + pos, buf.size() - pos, "%.17g", val);
    } else {
      float val = load_float(data.data() + i * bpe);
      written = snprintf(buf.data() + pos, buf.size() - pos, "%g", val);
    }
    if (written < 0 || pos + static_cast<size_t>(written) >= buf.size()) return;
    pos += static_cast<size_t>(written);
  }

  if (pos >= buf.size()) return;
  buf[pos++] = ']';
  out.set_length(pos);
}

int vec_chain_compare(vsql::CustomArgWith<VecChainParams> a,
                      vsql::CustomArgWith<VecChainParams> b) {
  const VecChainParams &p = a.params();
  const unsigned char *da = a.value().data();
  const unsigned char *db = b.value().data();
  for (int64_t i = 0; i < p.dimension; i++) {
    if (p.bytes_per_elem == 8) {
      double v1 = load_double(da + i * p.bytes_per_elem);
      double v2 = load_double(db + i * p.bytes_per_elem);
      if (v1 < v2) return -1;
      if (v1 > v2) return 1;
    } else {
      float v1 = load_float(da + i * p.bytes_per_elem);
      float v2 = load_float(db + i * p.bytes_per_elem);
      if (v1 < v2) return -1;
      if (v1 > v2) return 1;
    }
  }
  return 0;
}

std::string vec_chain_default(const VecChainParams &p, char * /*error_msg*/) {
  std::string buf = "[";
  for (int64_t i = 0; i < p.dimension; i++) {
    if (i > 0) buf += ",";
    buf += "0";
  }
  buf += "]";
  return buf;
}

static bool vec_chain_compatible(vsql::CustomArgWith<VecChainParams> a,
                                 vsql::CustomArgWith<VecChainParams> b,
                                 const char *func_name, vsql::RealResult &out) {
  if (a.is_null() || b.is_null()) {
    out.set_null();
    return false;
  }
  const VecChainParams &pa = a.params();
  const VecChainParams &pb = b.params();
  if (pa.dimension != pb.dimension || pa.bytes_per_elem != pb.bytes_per_elem) {
    char msg[128];
    snprintf(msg, sizeof(msg),
             "%s: vectors must have the same dimension and type", func_name);
    out.error(msg);
    return false;
  }
  return true;
}

double vec_chain_l2_distance_raw(const unsigned char *a, const unsigned char *b,
                                 const VecChainParams &p) {
  double sum = 0.0;
  for (int64_t i = 0; i < p.dimension; i++) {
    double diff;
    if (p.bytes_per_elem == 8) {
      diff = load_double(a + i * 8) - load_double(b + i * 8);
    } else {
      diff = static_cast<double>(load_float(a + i * 4)) -
             static_cast<double>(load_float(b + i * 4));
    }
    sum += diff * diff;
  }
  return std::sqrt(sum);
}

void vec_chain_l2_distance(vsql::CustomArgWith<VecChainParams> a,
                           vsql::CustomArgWith<VecChainParams> b,
                           vsql::RealResult out) {
  if (!vec_chain_compatible(a, b, "vec_chain_l2_distance", out)) return;
  const VecChainParams &p = a.params();
  out.set(vec_chain_l2_distance_raw(a.value().data(), b.value().data(), p));
}

void vec_chain_dot_product(vsql::CustomArgWith<VecChainParams> a,
                           vsql::CustomArgWith<VecChainParams> b,
                           vsql::RealResult out) {
  if (!vec_chain_compatible(a, b, "vec_chain_dot_product", out)) return;
  const VecChainParams &p = a.params();
  const unsigned char *da = a.value().data();
  const unsigned char *db = b.value().data();
  double sum = 0.0;
  for (int64_t i = 0; i < p.dimension; i++) {
    if (p.bytes_per_elem == 8) {
      sum += load_double(da + i * 8) * load_double(db + i * 8);
    } else {
      sum += static_cast<double>(load_float(da + i * 4)) *
             static_cast<double>(load_float(db + i * 4));
    }
  }
  out.set(sum);
}
