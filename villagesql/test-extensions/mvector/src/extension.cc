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

// VillageSQL MVECTOR extension demonstrating parameterized custom types.
//
// MVECTOR supports two element types via the "type" parameter:
//   MVECTOR(N)                          -- N float32 elements (default)
//   MVECTOR('dimension=N,type=float')   -- same as above
//   MVECTOR('dimension=N,type=double')  -- N float64 elements
//
// MVECTOR(3) with float stores 12 bytes (3 * 4). With double, 24 bytes (3 *
// 8). Text format: "[1.0,2.0,3.0]" (comma-separated values in brackets)

#include <villagesql/preview/index_builder.h>
#include <villagesql/vsql.h>

#include <algorithm>
#include <cinttypes>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <map>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

// Largest dimension a MVECTOR may declare. Enforced in resolve_params and
// int_to_params; also drives kMVectorMaxPersistedLength below for the
// constant-string inference path.
constexpr int64_t kMVectorMaxDimension = 4096;

// Parsed representation of MVECTOR type parameters.
// The static parse() method is used automatically by make_type_encode,
// make_type_decode, and make_intrinsic_default when the operation function
// takes const MVectorParams& as its first parameter.
struct MVectorParams {
  int64_t dimension;
  size_t bytes_per_elem;  // 4 for float, 8 for double

  static MVectorParams parse(const std::map<std::string, std::string> &params) {
    auto dim_it = params.find("dimension");
    int64_t dim = strtoll(dim_it->second.c_str(), nullptr, 10);
    size_t bytes = 4;
    auto type_it = params.find("type");
    if (type_it != params.end() && type_it->second == "double") bytes = 8;
    return MVectorParams{.dimension = dim, .bytes_per_elem = bytes};
  }

  // Inverse of parse: render a typed MVectorParams back into the canonical
  // key/value string form. Used by the SDK when the server needs to publish
  // inferred params (e.g., from a constant-string from_string) back in the
  // same shape that parse() consumes.
  static void to_strings(const MVectorParams &p,
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
// Setting type explicitly ensures MVECTOR(3) produces the same canonical
// params as MVECTOR('dimension=3,type=float').
bool mvector_int_to_params(int64_t value,
                           std::map<std::string, std::string> &params,
                           char *error_msg) {
  if (value <= 0 || value > kMVectorMaxDimension) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "MVECTOR dimension must be in 1..%" PRId64 ", got %" PRId64,
             kMVectorMaxDimension, value);
    return true;
  }
  params["dimension"] = std::to_string(value);
  params["type"] = "float";
  return false;
}

// Validate type parameters and compute storage characteristics.
bool mvector_resolve_params(const std::map<std::string, std::string> &params,
                            vsql::ResolvedTypeParams *result, char *error_msg) {
  auto it = params.find("dimension");
  if (it == params.end()) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "MVECTOR: dimension must be a positive integer");
    return true;
  }
  char *endptr = nullptr;
  int64_t dimension = strtoll(it->second.c_str(), &endptr, 10);
  if (endptr == it->second.c_str() || *endptr != '\0') {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "MVECTOR: invalid dimension value '%s'", it->second.c_str());
    return true;
  }
  if (dimension <= 0 || dimension > kMVectorMaxDimension) {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "MVECTOR: dimension must be in 1..%" PRId64 ", got %" PRId64,
             kMVectorMaxDimension, dimension);
    return true;
  }

  // Validate "type" parameter if present.
  auto type_it = params.find("type");
  if (type_it != params.end() && type_it->second != "float" &&
      type_it->second != "double") {
    snprintf(error_msg, VEF_MAX_ERROR_LEN,
             "MVECTOR: type must be 'float' or 'double', got '%s'",
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
void mvector_from_string(vsql::MaybeParams<MVectorParams> &p,
                         std::string_view from, vsql::CustomResult out) {
  // strtof/strtod require a null-terminated string.
  std::string input(from);
  const char *s = input.c_str();
  while (*s == ' ') s++;
  if (*s != '[') {
    out.warning("mvector_from_string: missing '['");
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
      out.warning("mvector_from_string: buffer too small");
      return;
    }

    char *endptr = nullptr;
    if (bpe == 8) {
      double val = strtod(s, &endptr);
      if (endptr == s) {
        out.warning("mvector_from_string: parse error");
        return;
      }
      store_double(buf.data() + count * 8, val);
    } else {
      float val = strtof(s, &endptr);
      if (endptr == s) {
        out.warning("mvector_from_string: parse error");
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
    out.warning("mvector_from_string: missing ']'");
    return;
  }

  if (p.is_known()) {
    if (count != static_cast<size_t>(p.value().dimension)) {
      out.warning("mvector_from_string: dimension mismatch");
      return;
    }
  } else {
    p.set(MVectorParams{.dimension = static_cast<int64_t>(count),
                        .bytes_per_elem = 4});
  }

  out.set_length(count * bpe);
}

// Decode: N * bpe bytes binary -> "[v1,v2,...,vN]" string.
// MVECTOR -> STRING
// Dimension and element type are read from type parameters.
void mvector_to_string(vsql::CustomArgWith<MVectorParams> in,
                       vsql::StringResult out) {
  const MVectorParams &p = in.params();
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

// Compare: (MVECTOR, MVECTOR) -> INT for ORDER BY, indexes.
// Lexicographic element-by-element comparison.
// TODO(villagesql-performance): we can also consider having templated versions
// of these functions instead of using branches, then selecting the version to
// use with one branch.
int mvector_compare(vsql::CustomArgWith<MVectorParams> a,
                    vsql::CustomArgWith<MVectorParams> b) {
  const MVectorParams &p = a.params();
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

// Implicit default for MVECTOR: returns "[0,0,...,0]" with p.dimension zeros.
// The server converts this string using the type's from_string function.
std::string mvector_default(const MVectorParams &p, char * /*error_msg*/) {
  std::string buf = "[";
  for (int64_t i = 0; i < p.dimension; i++) {
    if (i > 0) buf += ",";
    buf += "0";
  }
  buf += "]";
  return buf;
}

// Dot product: (MVECTOR, MVECTOR) -> REAL
// Returns the sum of element-wise products of two vectors of the same type.
void mvector_dot_product(vsql::CustomArgWith<MVectorParams> a,
                         vsql::CustomArgWith<MVectorParams> b,
                         vsql::RealResult out) {
  if (a.is_null() || b.is_null()) {
    out.set_null();
    return;
  }
  const MVectorParams &pa = a.params();
  const MVectorParams &pb = b.params();
  if (pa.dimension != pb.dimension || pa.bytes_per_elem != pb.bytes_per_elem) {
    out.error(
        "mvector_dot_product: vectors must have the same dimension and type");
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

bool mvector_compatible(vsql::CustomArgWith<MVectorParams> a,
                        vsql::CustomArgWith<MVectorParams> b,
                        const char *func_name, vsql::RealResult &out) {
  if (a.is_null() || b.is_null()) {
    out.set_null();
    return false;
  }
  const MVectorParams &pa = a.params();
  const MVectorParams &pb = b.params();
  if (pa.dimension != pb.dimension || pa.bytes_per_elem != pb.bytes_per_elem) {
    char msg[128];
    snprintf(msg, sizeof(msg),
             "%s: vectors must have the same dimension and type", func_name);
    out.error(msg);
    return false;
  }
  return true;
}

double mvector_l2_distance_raw(const unsigned char *a, const unsigned char *b,
                               const MVectorParams &p) {
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

void mvector_l2_distance(vsql::CustomArgWith<MVectorParams> a,
                         vsql::CustomArgWith<MVectorParams> b,
                         vsql::RealResult out) {
  if (!mvector_compatible(a, b, "mvector_l2_distance", out)) return;

  const MVectorParams &p = a.params();
  out.set(mvector_l2_distance_raw(a.value().data(), b.value().data(), p));
}

void mvector_cosine_distance(vsql::CustomArgWith<MVectorParams> a,
                             vsql::CustomArgWith<MVectorParams> b,
                             vsql::RealResult out) {
  if (!mvector_compatible(a, b, "mvector_cosine_distance", out)) return;

  const MVectorParams &p = a.params();
  const unsigned char *da = a.value().data();
  const unsigned char *db = b.value().data();
  double dot = 0.0;
  double norm_a = 0.0;
  double norm_b = 0.0;
  for (int64_t i = 0; i < p.dimension; i++) {
    double va;
    double vb;
    if (p.bytes_per_elem == 8) {
      va = load_double(da + i * 8);
      vb = load_double(db + i * 8);
    } else {
      va = static_cast<double>(load_float(da + i * 4));
      vb = static_cast<double>(load_float(db + i * 4));
    }
    dot += va * vb;
    norm_a += va * va;
    norm_b += vb * vb;
  }
  if (norm_a == 0.0 || norm_b == 0.0) {
    out.set_null();
    return;
  }
  out.set(1.0 - dot / std::sqrt(norm_a * norm_b));
}

// Element-wise add: (MVECTOR, MVECTOR) -> MVECTOR
// Vectors must have the same dimension and element type.
void mvector_add(vsql::CustomArgWith<MVectorParams> a,
                 vsql::CustomArgWith<MVectorParams> b,
                 vsql::CustomResultWith<MVectorParams> out) {
  if (a.is_null() || b.is_null()) {
    out.set_null();
    return;
  }
  const MVectorParams &pa = a.params();
  const MVectorParams &pb = b.params();
  if (pa.dimension != pb.dimension || pa.bytes_per_elem != pb.bytes_per_elem) {
    out.error("mvector_add: vectors must have the same dimension and type");
    return;
  }
  auto buf = out.buffer();
  size_t byte_size = static_cast<size_t>(pa.dimension) * pa.bytes_per_elem;
  if (buf.size() < byte_size) {
    out.error("mvector_add: output buffer too small");
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

// Scalar multiply: (MVECTOR, REAL) -> MVECTOR
void mvector_scale(vsql::CustomArgWith<MVectorParams> a, vsql::RealArg scalar,
                   vsql::CustomResultWith<MVectorParams> out) {
  if (a.is_null() || scalar.is_null()) {
    out.set_null();
    return;
  }
  const MVectorParams &pa = a.params();
  double s = scalar.value();
  auto buf = out.buffer();
  size_t byte_size = static_cast<size_t>(pa.dimension) * pa.bytes_per_elem;
  if (buf.size() < byte_size) {
    out.error("mvector_scale: output buffer too small");
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

struct MVectorIndexEntry {
  long long pk = 0;
  std::vector<unsigned char> vector;
};

struct MVectorIndex {
  bool has_params = false;
  MVectorParams params{0, 4};
  std::unordered_map<long long, MVectorIndexEntry> entries;
  long long scan_begin_count = 0;
  long long scan_fetch_count = 0;
  long long last_scan_entry_count = 0;
  long long last_scan_query_len = 0;
  long long last_scan_hit_count = 0;
};

std::mutex g_mvector_indexes_mu;
std::unordered_map<std::string, MVectorIndex> g_mvector_indexes;

struct MVectorIndexPKeyPart {
  std::vector<unsigned char> data;
};

struct MVectorIndexStorageEntry {
  uint64_t ref = 0;
  std::vector<unsigned char> key;
  std::vector<MVectorIndexPKeyPart> pkey;
  bool delete_marked = false;
};

struct MVectorIndexStorage {
  uint64_t next_ref = 1;
  std::vector<MVectorIndexStorageEntry> entries;
};

struct MVectorIndexCursorHit {
  size_t entry_pos = 0;
  double distance = 0.0;
};

struct MVectorIndexCursor {
  MVectorIndexStorage *storage = nullptr;
  std::string index_name;
  std::vector<MVectorIndexCursorHit> hits;
  size_t pos = 0;
};

using vsql::preview_index_builder::Index;
using vsql::preview_index_builder::IndexScanDesc;
using vsql::preview_index_builder::IndexScanKey;
using vsql::preview_storage::MtrCtx;
using vsql::preview_storage::Segment;
using vsql::preview_storage::Space;

using MVectorIndexCtx = Index::StorageCtx<MVectorIndexStorage>;

bool mvector_memory_index_create(MVectorIndexCtx * /*ctx*/,
                                 const Index & /*index*/,
                                 Space::Ref /*space_ref*/,
                                 Segment::TrxRef /*trx_ref*/,
                                 char * /*error_msg*/,
                                 uint32_t /*error_msg_len*/) {
  return false;
}

bool mvector_memory_index_load(MVectorIndexCtx *ctx, const Index & /*index*/,
                               Index::StorageRef storage_ref,
                               char * /*error_msg*/,
                               uint32_t /*error_msg_len*/) {
  ctx->set_ref(storage_ref);
  ctx->user()->entries.clear();
  ctx->user()->next_ref = 1;
  return false;
}

bool mvector_memory_index_drop(MVectorIndexCtx *ctx, const Index &index,
                               Segment::TrxRef /*trx_ref*/,
                               char * /*error_msg*/,
                               uint32_t /*error_msg_len*/) {
  ctx->user()->entries.clear();
  const std::string index_name =
      index.get_index_ref() == nullptr
          ? ""
          : static_cast<const char *>(index.get_index_ref());
  if (!index_name.empty()) {
    std::lock_guard<std::mutex> guard(g_mvector_indexes_mu);
    g_mvector_indexes.erase(index_name);
  }
  return false;
}

bool mvector_pkey_equal(const MVectorIndexStorageEntry &entry,
                        const Index &index,
                        IndexScanKey::KeyPartData *pkey_columns) {
  if (entry.pkey.size() != index.get_primary_num_key_cols()) return false;
  for (uint32_t i = 0; i < index.get_primary_num_key_cols(); i++) {
    const MVectorIndexPKeyPart &lhs = entry.pkey[i];
    const IndexScanKey::KeyPartData &rhs = pkey_columns[i];
    if (lhs.data.size() != rhs.length) return false;
    if (rhs.length > 0 && memcmp(lhs.data.data(), rhs.data, rhs.length) != 0) {
      return false;
    }
  }
  return true;
}

std::string mvector_runtime_index_name(const Index &index) {
  if (index.get_index_ref() == nullptr) return "";
  return static_cast<const char *>(index.get_index_ref());
}

long long mvector_pkey_as_int(const Index &index,
                              IndexScanKey::KeyPartData *pkey_columns) {
  if (index.get_primary_num_key_cols() != 1 || pkey_columns == nullptr ||
      pkey_columns[0].data == nullptr) {
    return 0;
  }
  if (pkey_columns[0].length == sizeof(int32_t)) {
    int32_t value = 0;
    memcpy(&value, pkey_columns[0].data, sizeof(value));
    return value;
  }
  if (pkey_columns[0].length == sizeof(int64_t)) {
    int64_t value = 0;
    memcpy(&value, pkey_columns[0].data, sizeof(value));
    return value;
  }
  return 0;
}

bool mvector_memory_index_insert(MVectorIndexCtx *ctx, const Index &index,
                                 Segment::TrxRef /*trx_ref*/,
                                 IndexScanKey::KeyPartData *key_columns,
                                 IndexScanKey::KeyPartData *pkey_columns,
                                 IndexScanKey::KeyPartRef *key_ref,
                                 char *error_msg, uint32_t error_msg_len) {
  if (index.get_num_key_cols() != 1 || key_columns == nullptr ||
      key_columns[0].data == nullptr) {
    snprintf(error_msg, error_msg_len,
             "MVECTOR index requires exactly one non-null key column");
    return true;
  }

  MVectorIndexStorageEntry entry;
  entry.ref = ctx->user()->next_ref++;
  entry.key.assign(key_columns[0].data,
                   key_columns[0].data + key_columns[0].length);
  entry.pkey.resize(index.get_primary_num_key_cols());
  for (uint32_t i = 0; i < index.get_primary_num_key_cols(); i++) {
    const IndexScanKey::KeyPartData &part = pkey_columns[i];
    entry.pkey[i].data.assign(part.data, part.data + part.length);
  }
  if (key_ref != nullptr) *key_ref = entry.ref;
  ctx->user()->entries.push_back(std::move(entry));

  const std::string index_name = mvector_runtime_index_name(index);
  const long long pk = mvector_pkey_as_int(index, pkey_columns);
  if (!index_name.empty() && pk != 0) {
    std::lock_guard<std::mutex> guard(g_mvector_indexes_mu);
    MVectorIndex &mirror_index = g_mvector_indexes[index_name];
    mirror_index.has_params = true;
    mirror_index.params = MVectorParams{
        .dimension =
            static_cast<int64_t>(key_columns[0].length / sizeof(float)),
        .bytes_per_elem = sizeof(float)};
    MVectorIndexEntry mirror;
    mirror.pk = pk;
    mirror.vector.assign(key_columns[0].data,
                         key_columns[0].data + key_columns[0].length);
    mirror_index.entries[pk] = std::move(mirror);
  }
  return false;
}

bool mvector_memory_index_mark_delete(
    MVectorIndexCtx *ctx, const Index &index, Segment::TrxRef /*trx_ref*/,
    IndexScanKey::KeyPartRef *key_ref,
    IndexScanKey::KeyPartData * /*key_columns*/,
    IndexScanKey::KeyPartData *pkey_columns, bool delete_mark,
    char * /*error_msg*/, uint32_t /*error_msg_len*/) {
  for (MVectorIndexStorageEntry &entry : ctx->user()->entries) {
    if (key_ref != nullptr && *key_ref != IndexScanKey::EMPTY_REF &&
        entry.ref != *key_ref) {
      continue;
    }
    if ((key_ref == nullptr || *key_ref == IndexScanKey::EMPTY_REF) &&
        !mvector_pkey_equal(entry, index, pkey_columns)) {
      continue;
    }
    entry.delete_marked = delete_mark;
    const std::string index_name = mvector_runtime_index_name(index);
    const long long pk = mvector_pkey_as_int(index, pkey_columns);
    if (delete_mark && !index_name.empty() && pk != 0) {
      std::lock_guard<std::mutex> guard(g_mvector_indexes_mu);
      auto index_it = g_mvector_indexes.find(index_name);
      if (index_it != g_mvector_indexes.end()) {
        index_it->second.entries.erase(pk);
      }
    }
    return false;
  }
  return false;
}

bool mvector_memory_index_purge(MVectorIndexCtx *ctx, const Index &index,
                                Segment::TrxRef /*trx_ref*/,
                                IndexScanKey::KeyPartRef *key_ref,
                                IndexScanKey::KeyPartData * /*key_columns*/,
                                IndexScanKey::KeyPartData *pkey_columns,
                                char * /*error_msg*/,
                                uint32_t /*error_msg_len*/) {
  auto &entries = ctx->user()->entries;
  entries.erase(std::remove_if(entries.begin(), entries.end(),
                               [&](const MVectorIndexStorageEntry &entry) {
                                 if (key_ref != nullptr &&
                                     *key_ref != IndexScanKey::EMPTY_REF) {
                                   return entry.ref == *key_ref;
                                 }
                                 return mvector_pkey_equal(entry, index,
                                                           pkey_columns);
                               }),
                entries.end());
  return false;
}

bool mvector_memory_index_scan_begin(MVectorIndexCtx *ctx, const Index &index,
                                     MtrCtx::Ref /*mctx*/,
                                     const IndexScanDesc &scan_desc,
                                     Index::Cursor *cursor, bool *eof,
                                     char *error_msg, uint32_t error_msg_len) {
  *cursor = nullptr;
  *eof = true;
  if (!scan_desc.is_knn() || scan_desc.num_keys() != 1) {
    snprintf(error_msg, error_msg_len,
             "MVECTOR index only supports one-column KNN scans");
    return true;
  }
  const IndexScanKey scan_key = scan_desc[0];
  if (!scan_key.is_knn() || scan_key.num_columns() != 1 ||
      !scan_key.is_bounded() || scan_key[0].data == nullptr) {
    snprintf(error_msg, error_msg_len,
             "MVECTOR index only supports one-column KNN scans");
    return true;
  }

  const IndexScanKey::KeyPartData &query = scan_key[0];
  auto *c = new (std::nothrow) MVectorIndexCursor();
  if (c == nullptr) {
    snprintf(error_msg, error_msg_len, "out of memory allocating cursor");
    return true;
  }
  c->storage = ctx->user();
  c->index_name = mvector_runtime_index_name(index);
  if (!c->index_name.empty()) {
    std::lock_guard<std::mutex> guard(g_mvector_indexes_mu);
    MVectorIndex &mirror = g_mvector_indexes[c->index_name];
    mirror.scan_begin_count++;
    mirror.last_scan_entry_count =
        static_cast<long long>(c->storage->entries.size());
    mirror.last_scan_query_len = static_cast<long long>(query.length);
  }

  const MVectorParams params{
      .dimension = static_cast<int64_t>(query.length / sizeof(float)),
      .bytes_per_elem = sizeof(float)};
  for (size_t i = 0; i < c->storage->entries.size(); i++) {
    const MVectorIndexStorageEntry &entry = c->storage->entries[i];
    if (entry.delete_marked || entry.key.size() != query.length) continue;
    c->hits.push_back(MVectorIndexCursorHit{
        i, mvector_l2_distance_raw(entry.key.data(), query.data, params)});
  }

  std::sort(c->hits.begin(), c->hits.end(),
            [](const MVectorIndexCursorHit &a, const MVectorIndexCursorHit &b) {
              if (a.distance < b.distance) return true;
              if (a.distance > b.distance) return false;
              return a.entry_pos < b.entry_pos;
            });
  if (scan_desc.limit() > 0 && c->hits.size() > scan_desc.limit()) {
    c->hits.resize(scan_desc.limit());
  }
  if (!c->index_name.empty()) {
    std::lock_guard<std::mutex> guard(g_mvector_indexes_mu);
    g_mvector_indexes[c->index_name].last_scan_hit_count =
        static_cast<long long>(c->hits.size());
  }

  *eof = c->hits.empty();
  *cursor = c;
  return false;
}

bool mvector_memory_index_scan_position(Index::Cursor cursor,
                                        Index::CursorOp op, bool *eof,
                                        char * /*error_msg*/,
                                        uint32_t /*error_msg_len*/) {
  auto *c = static_cast<MVectorIndexCursor *>(cursor);
  if (op == Index::CursorOp::Prev) {
    *eof = true;
    return false;
  }
  if (c->pos < c->hits.size()) c->pos++;
  *eof = c->pos >= c->hits.size();
  return false;
}

bool mvector_memory_index_scan_fetch(Index::Cursor cursor,
                                     IndexScanKey::KeyPartRef *key_ref,
                                     IndexScanKey::KeyPartData *key_columns,
                                     IndexScanKey::KeyPartData *pkey_columns,
                                     char *error_msg, uint32_t error_msg_len) {
  auto *c = static_cast<MVectorIndexCursor *>(cursor);
  if (c->pos >= c->hits.size()) {
    snprintf(error_msg, error_msg_len, "MVECTOR index cursor is at EOF");
    return true;
  }

  const MVectorIndexStorageEntry &entry =
      c->storage->entries[c->hits[c->pos].entry_pos];
  if (!c->index_name.empty()) {
    std::lock_guard<std::mutex> guard(g_mvector_indexes_mu);
    g_mvector_indexes[c->index_name].scan_fetch_count++;
  }
  if (key_ref != nullptr) *key_ref = entry.ref;
  key_columns[0] = IndexScanKey::KeyPartData{
      .data = entry.key.data(),
      .length = static_cast<uint32_t>(entry.key.size())};
  for (size_t i = 0; i < entry.pkey.size(); i++) {
    pkey_columns[i] = IndexScanKey::KeyPartData{
        .data = entry.pkey[i].data.data(),
        .length = static_cast<uint32_t>(entry.pkey[i].data.size())};
  }
  return false;
}

bool mvector_memory_index_scan_save(Index::Cursor /*cursor*/,
                                    char * /*error_msg*/,
                                    uint32_t /*error_msg_len*/) {
  return false;
}

bool mvector_memory_index_scan_restore(Index::Cursor cursor,
                                       MtrCtx::Ref /*mctx*/, bool *eof,
                                       char * /*error_msg*/,
                                       uint32_t /*error_msg_len*/) {
  auto *c = static_cast<MVectorIndexCursor *>(cursor);
  *eof = c->pos >= c->hits.size();
  return false;
}

void mvector_memory_index_scan_end(Index::Cursor *cursor) {
  delete static_cast<MVectorIndexCursor *>(*cursor);
  *cursor = nullptr;
}

bool mvector_index_name(vsql::StringArg index_name, std::string &out,
                        vsql::IntResult &result) {
  if (index_name.is_null()) {
    result.set_null();
    return false;
  }
  auto value = index_name.value();
  out.assign(value.data(), value.size());
  if (out.empty()) {
    result.error("mvector index name must not be empty");
    return false;
  }
  return true;
}

bool mvector_index_name(vsql::StringArg index_name, std::string &out,
                        vsql::StringResult &result) {
  if (index_name.is_null()) {
    result.set_null();
    return false;
  }
  auto value = index_name.value();
  out.assign(value.data(), value.size());
  if (out.empty()) {
    result.error("mvector index name must not be empty");
    return false;
  }
  return true;
}

bool mvector_same_params(const MVectorParams &a, const MVectorParams &b) {
  return a.dimension == b.dimension && a.bytes_per_elem == b.bytes_per_elem;
}

void mvector_index_clear(vsql::StringArg index_name, vsql::IntResult out) {
  std::string name;
  if (!mvector_index_name(index_name, name, out)) return;

  std::lock_guard<std::mutex> guard(g_mvector_indexes_mu);
  g_mvector_indexes.erase(name);
  out.set(1);
}

void mvector_index_upsert(vsql::StringArg index_name, vsql::IntArg pk,
                          vsql::CustomArgWith<MVectorParams> vector,
                          vsql::IntResult out) {
  std::string name;
  if (!mvector_index_name(index_name, name, out)) return;
  if (pk.is_null() || vector.is_null()) {
    out.set_null();
    return;
  }

  const MVectorParams &params = vector.params();
  std::lock_guard<std::mutex> guard(g_mvector_indexes_mu);
  MVectorIndex &index = g_mvector_indexes[name];
  if (!index.has_params) {
    index.has_params = true;
    index.params = params;
  } else if (!mvector_same_params(index.params, params)) {
    out.error("mvector_index_upsert: vector dimension/type mismatch");
    return;
  }

  MVectorIndexEntry entry;
  entry.pk = pk.value();
  auto value = vector.value();
  entry.vector.assign(value.data(), value.data() + value.size());
  index.entries[entry.pk] = std::move(entry);
  out.set(1);
}

void mvector_index_delete(vsql::StringArg index_name, vsql::IntArg pk,
                          vsql::IntResult out) {
  std::string name;
  if (!mvector_index_name(index_name, name, out)) return;
  if (pk.is_null()) {
    out.set_null();
    return;
  }

  std::lock_guard<std::mutex> guard(g_mvector_indexes_mu);
  auto index_it = g_mvector_indexes.find(name);
  if (index_it == g_mvector_indexes.end()) {
    out.set(0);
    return;
  }
  out.set(index_it->second.entries.erase(pk.value()) ? 1 : 0);
}

void mvector_index_count(vsql::StringArg index_name, vsql::IntResult out) {
  std::string name;
  if (!mvector_index_name(index_name, name, out)) return;

  std::lock_guard<std::mutex> guard(g_mvector_indexes_mu);
  auto index_it = g_mvector_indexes.find(name);
  if (index_it == g_mvector_indexes.end()) {
    out.set(0);
    return;
  }
  out.set(static_cast<long long>(index_it->second.entries.size()));
}

void mvector_index_scan_begin_count(vsql::StringArg index_name,
                                    vsql::IntResult out) {
  std::string name;
  if (!mvector_index_name(index_name, name, out)) return;

  std::lock_guard<std::mutex> guard(g_mvector_indexes_mu);
  auto index_it = g_mvector_indexes.find(name);
  if (index_it == g_mvector_indexes.end()) {
    out.set(0);
    return;
  }
  out.set(index_it->second.scan_begin_count);
}

void mvector_index_scan_fetch_count(vsql::StringArg index_name,
                                    vsql::IntResult out) {
  std::string name;
  if (!mvector_index_name(index_name, name, out)) return;

  std::lock_guard<std::mutex> guard(g_mvector_indexes_mu);
  auto index_it = g_mvector_indexes.find(name);
  if (index_it == g_mvector_indexes.end()) {
    out.set(0);
    return;
  }
  out.set(index_it->second.scan_fetch_count);
}

void mvector_index_last_scan_entry_count(vsql::StringArg index_name,
                                         vsql::IntResult out) {
  std::string name;
  if (!mvector_index_name(index_name, name, out)) return;

  std::lock_guard<std::mutex> guard(g_mvector_indexes_mu);
  auto index_it = g_mvector_indexes.find(name);
  if (index_it == g_mvector_indexes.end()) {
    out.set(0);
    return;
  }
  out.set(index_it->second.last_scan_entry_count);
}

void mvector_index_last_scan_query_len(vsql::StringArg index_name,
                                       vsql::IntResult out) {
  std::string name;
  if (!mvector_index_name(index_name, name, out)) return;

  std::lock_guard<std::mutex> guard(g_mvector_indexes_mu);
  auto index_it = g_mvector_indexes.find(name);
  if (index_it == g_mvector_indexes.end()) {
    out.set(0);
    return;
  }
  out.set(index_it->second.last_scan_query_len);
}

void mvector_index_last_scan_hit_count(vsql::StringArg index_name,
                                       vsql::IntResult out) {
  std::string name;
  if (!mvector_index_name(index_name, name, out)) return;

  std::lock_guard<std::mutex> guard(g_mvector_indexes_mu);
  auto index_it = g_mvector_indexes.find(name);
  if (index_it == g_mvector_indexes.end()) {
    out.set(0);
    return;
  }
  out.set(index_it->second.last_scan_hit_count);
}

void mvector_search_nn(vsql::StringArg index_name,
                       vsql::CustomArgWith<MVectorParams> query,
                       vsql::IntArg limit, vsql::StringResult out) {
  std::string name;
  if (!mvector_index_name(index_name, name, out)) return;
  if (query.is_null() || limit.is_null()) {
    out.set_null();
    return;
  }
  if (limit.value() <= 0) {
    out.set("");
    return;
  }

  const MVectorParams &params = query.params();
  struct Hit {
    long long pk;
    double distance;
  };
  std::vector<Hit> hits;
  {
    std::lock_guard<std::mutex> guard(g_mvector_indexes_mu);
    auto index_it = g_mvector_indexes.find(name);
    if (index_it == g_mvector_indexes.end() || !index_it->second.has_params) {
      out.set("");
      return;
    }
    const MVectorIndex &index = index_it->second;
    if (!mvector_same_params(index.params, params)) {
      out.error("mvector_search_nn: vector dimension/type mismatch");
      return;
    }
    hits.reserve(index.entries.size());
    for (const auto &kv : index.entries) {
      const MVectorIndexEntry &entry = kv.second;
      hits.push_back(
          Hit{entry.pk, mvector_l2_distance_raw(entry.vector.data(),
                                                query.value().data(), params)});
    }
  }

  std::sort(hits.begin(), hits.end(), [](const Hit &a, const Hit &b) {
    if (a.distance < b.distance) return true;
    if (a.distance > b.distance) return false;
    return a.pk < b.pk;
  });

  auto buf = out.buffer();
  size_t pos = 0;
  const size_t n = std::min(hits.size(), static_cast<size_t>(limit.value()));
  for (size_t i = 0; i < n; i++) {
    int written = snprintf(buf.data() + pos, buf.size() - pos, "%s%lld",
                           i == 0 ? "" : ",", hits[i].pk);
    if (written < 0 || pos + static_cast<size_t>(written) >= buf.size()) {
      out.error("mvector_search_nn: output buffer too small");
      return;
    }
    pos += static_cast<size_t>(written);
  }
  out.set_length(pos);
}

static constexpr const char kMVectorTypeName[] = "MVECTOR";
static constexpr const char kMVectorIndexTypeName[] = "mvector_nn";

[[maybe_unused]] constexpr auto MVECTOR_MEMORY_INDEX =
    vsql::preview_index_builder::make_index_type<kMVectorIndexTypeName,
                                                 MVectorIndexStorage>()
        .lifecycle()
        .create<&mvector_memory_index_create>()
        .load<&mvector_memory_index_load>()
        .drop<&mvector_memory_index_drop>()
        .dml()
        .insert<&mvector_memory_index_insert>()
        .mark_delete<&mvector_memory_index_mark_delete>()
        .purge<&mvector_memory_index_purge>()
        .scan()
        .begin<&mvector_memory_index_scan_begin>()
        .position<&mvector_memory_index_scan_position>()
        .fetch<&mvector_memory_index_scan_fetch>()
        .save<&mvector_memory_index_scan_save>()
        .restore<&mvector_memory_index_scan_restore>()
        .end<&mvector_memory_index_scan_end>()
        .global()
        .capabilities(Index::Support::KNN)
        .storage_props(Index::Storage::HAS_ROW_REF | Index::Storage::REF_LOOKUP)
        .build();

static auto MVECTOR_INDEXES =
    vsql::preview_index_builder::IndexTypeCapability().index_type(
        MVECTOR_MEMORY_INDEX);

static constexpr const char kMVectorProfileL2[] = "mvector_nn_l2";

static const auto MVECTOR_L2_FN =
    vsql::preview_index_builder::make_index_function<&mvector_l2_distance>(
        "mvector_l2_distance")
        .returns(vsql::REAL)
        .param(kMVectorTypeName)
        .param(kMVectorTypeName)
        .deterministic()
        .build();

static const auto MVECTOR_NN_L2_PROFILE =
    vsql::preview_index_builder::make_index_profile(kMVectorProfileL2)
        .for_type(kMVectorTypeName)
        .using_index(kMVectorIndexTypeName)
        .with_function(1, MVECTOR_L2_FN)
        .ordering(Index::Ordering::ASC)
        .default_for_type(true)
        .build();

static auto MVECTOR_PROFILE =
    vsql::preview_index_builder::IndexProfileCapability().index_profile(
        MVECTOR_NN_L2_PROFILE);

// Upper bound on MVECTOR's persisted byte size: kMVectorMaxDimension
// elements at 8 bytes each (double, the wider of the two supported element
// types). Used only on the fix_fields-time constant-string inference path;
// row-time encoding uses the params-resolved persisted_length set by
// mvector_resolve_params.
constexpr int64_t kMVectorMaxPersistedLength = kMVectorMaxDimension * 8;

constexpr auto MVECTOR = vsql::make_type<kMVectorTypeName>()
                             .persisted_length(-1)
                             .max_decode_buffer_length(16)
                             .max_persisted_length(kMVectorMaxPersistedLength)
                             .params<MVectorParams, &MVectorParams::parse,
                                     &MVectorParams::to_strings>()
                             .int_to_params<&mvector_int_to_params>()
                             .resolve_params<&mvector_resolve_params>()
                             .from_string<&mvector_from_string>()
                             .to_string<&mvector_to_string>()
                             .compare<&mvector_compare>()
                             .intrinsic_default_vdf("mvector_intrinsic_default")
                             .build();

using namespace vsql;

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .with(MVECTOR_INDEXES)
        .with(MVECTOR_PROFILE)
        .type(MVECTOR)
        .func(make_intrinsic_default<&mvector_default>(
            "mvector_intrinsic_default"))
        .func(make_func<&mvector_dot_product>("mvector_dot_product")
                  .returns(REAL)
                  .param(MVECTOR)
                  .param(MVECTOR)
                  .deterministic()
                  .build())
        .func(make_func<&mvector_l2_distance>("mvector_l2_distance")
                  .returns(REAL)
                  .param(MVECTOR)
                  .param(MVECTOR)
                  .deterministic()
                  .build())
        .func(make_func<&mvector_cosine_distance>("mvector_cosine_distance")
                  .returns(REAL)
                  .param(MVECTOR)
                  .param(MVECTOR)
                  .deterministic()
                  .build())
        .func(make_func<&mvector_add>("mvector_add")
                  .returns(MVECTOR)
                  .param(MVECTOR)
                  .param(MVECTOR)
                  .deterministic()
                  .build())
        .func(make_func<&mvector_scale>("mvector_scale")
                  .returns(MVECTOR)
                  .param(MVECTOR)
                  .param(REAL)
                  .deterministic()
                  .build())
        .func(make_func<&mvector_index_clear>("mvector_index_clear")
                  .returns(INT)
                  .param(STRING)
                  .build())
        .func(make_func<&mvector_index_upsert>("mvector_index_upsert")
                  .returns(INT)
                  .param(STRING)
                  .param(INT)
                  .param(MVECTOR)
                  .build())
        .func(make_func<&mvector_index_delete>("mvector_index_delete")
                  .returns(INT)
                  .param(STRING)
                  .param(INT)
                  .build())
        .func(make_func<&mvector_index_count>("mvector_index_count")
                  .returns(INT)
                  .param(STRING)
                  .build())
        .func(make_func<&mvector_index_scan_begin_count>(
                  "mvector_index_scan_begin_count")
                  .returns(INT)
                  .param(STRING)
                  .build())
        .func(make_func<&mvector_index_scan_fetch_count>(
                  "mvector_index_scan_fetch_count")
                  .returns(INT)
                  .param(STRING)
                  .build())
        .func(make_func<&mvector_index_last_scan_entry_count>(
                  "mvector_index_last_scan_entry_count")
                  .returns(INT)
                  .param(STRING)
                  .build())
        .func(make_func<&mvector_index_last_scan_query_len>(
                  "mvector_index_last_scan_query_len")
                  .returns(INT)
                  .param(STRING)
                  .build())
        .func(make_func<&mvector_index_last_scan_hit_count>(
                  "mvector_index_last_scan_hit_count")
                  .returns(INT)
                  .param(STRING)
                  .build())
        .func(make_func<&mvector_search_nn>("mvector_search_nn")
                  .returns(STRING)
                  .param(STRING)
                  .param(MVECTOR)
                  .param(INT)
                  .buffer_size(4096)
                  .build()))
