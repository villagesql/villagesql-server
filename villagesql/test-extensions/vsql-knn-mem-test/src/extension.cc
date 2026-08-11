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

// vsql_knn_mem_test extension: a FUNCTIONAL in-memory KNN custom index, used to
// drive the optimizer's custom distance-scan path end to end.
//
// Where vsql-index-test wires the same preview custom-index builder API with
// no-op stubs (to validate the builder), this extension fills those callbacks
// with a real in-memory implementation so that:
//
//   CREATE TABLE t (id INT PRIMARY KEY, v KVECTOR);
//   CREATE INDEX idx ON t (v) USING EXTENDED(vsql_knn_mem_test.kvec_knn);
//   INSERT INTO t VALUES (...);
//   SELECT id FROM t
//     ORDER BY vsql_knn_mem_test.kvec_l2_distance(v, KVECTOR::from_string(...))
//     LIMIT k;
//
// takes the custom distance scan: INSERT populates the index via ha_write_row;
// the query's ORDER BY distance + LIMIT drives the hypergraph optimizer to a
// custom INDEX_DISTANCE_SCAN, whose iterator calls scan_begin/fetch here.
//
// NOTE: internal testing tool, not an example of how to write an extension.
// The index is a flat in-memory vector list with an exact (brute-force) KNN
// scan; it stores no data on disk and keeps everything in the per-index
// StorageCtx, which the SDK allocates from the InnoDB arena at load and
// destroys at drop, so it survives across the INSERT and SELECT statements.
//
// This extension defines:
//
//   KVECTOR       - A fixed 4-element float32 custom type (16 bytes).
//   kvec_knn      - A KNN custom index type over KVECTOR that maintains a flat
//                   in-memory list of (vector, primary-key) entries and answers
//                   KNN scans by exact L2 distance.
//   kvec_l2_distance - A REAL-returning distance function (KVECTOR, KVECTOR)
//                   bound into the index profile; the optimizer recognizes an
//                   ORDER BY over this function as a candidate KNN scan.

#include <villagesql/preview/index_builder.h>
#include <villagesql/vsql.h>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <vector>

using namespace vsql::preview_index_builder;

using vsql::preview_storage::MtrCtx;
using vsql::preview_storage::Segment;
using vsql::preview_storage::Space;

// Fixed vector shape: 4 float32 elements = 16 bytes.
static constexpr uint32_t kVecDim = 4;
static constexpr uint32_t kVecBytes = kVecDim * sizeof(float);

// Little-endian float load/store so stored bytes are portable (the type's
// persisted form is always little-endian, matching float8store conventions).
static float load_float_le(const unsigned char *p) {
  uint32_t bits =
      static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
      (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
  float f;
  memcpy(&f, &bits, sizeof(f));
  return f;
}

static void store_float_le(unsigned char *p, float f) {
  uint32_t bits;
  memcpy(&bits, &f, sizeof(bits));
  p[0] = static_cast<unsigned char>(bits & 0xff);
  p[1] = static_cast<unsigned char>((bits >> 8) & 0xff);
  p[2] = static_cast<unsigned char>((bits >> 16) & 0xff);
  p[3] = static_cast<unsigned char>((bits >> 24) & 0xff);
}

static double l2_distance_raw(const unsigned char *a, const unsigned char *b) {
  double sum = 0.0;
  for (uint32_t i = 0; i < kVecDim; ++i) {
    const double diff =
        static_cast<double>(load_float_le(a + i * sizeof(float))) -
        static_cast<double>(load_float_le(b + i * sizeof(float)));
    sum += diff * diff;
  }
  return std::sqrt(sum);
}

// Cosine distance = 1 - cosine similarity, range [0, 2]. A second, distinct
// metric so the extension registers more than one distance function bound to
// different index types (see kKVecIndexCos).
static double cosine_distance_raw(const unsigned char *a,
                                  const unsigned char *b) {
  double dot = 0.0, na = 0.0, nb = 0.0;
  for (uint32_t i = 0; i < kVecDim; ++i) {
    const double fa = static_cast<double>(load_float_le(a + i * sizeof(float)));
    const double fb = static_cast<double>(load_float_le(b + i * sizeof(float)));
    dot += fa * fb;
    na += fa * fa;
    nb += fb * fb;
  }
  if (na == 0.0 || nb == 0.0) return 1.0;
  return 1.0 - dot / (std::sqrt(na) * std::sqrt(nb));
}

// One stored index entry: the indexed vector bytes plus the row's primary key
// bytes. The primary key is stored opaquely and handed back verbatim at scan
// time; the server maps it to the base-table row.
struct KVecEntry {
  std::vector<unsigned char> vec;
  std::vector<unsigned char> pkey;
};

// Per-index in-memory state, allocated once per loaded index in the InnoDB
// arena and reused across statements.
struct KVecCtx {
  std::vector<KVecEntry> entries;
};

using Ctx = Index::StorageCtx<KVecCtx>;

// Test observability: correct KNN rows do not by themselves prove the custom
// index was used (an ORDER BY over the distance function sorts correctly even
// with a plain filesort fallback). These process-global counters are bumped
// inside the scan callbacks and read back by the introspection VDFs below, so
// a test can assert the scan callbacks actually fired. Single-index test, so a
// bare global (not a per-index map) is sufficient.
static long long g_scan_begin_count = 0;
static long long g_scan_fetch_count = 0;

// Cursor: the sorted list of primary keys to return, plus the read position.
// scan_begin fills it; fetch walks it. Owns copies of the primary-key bytes so
// it stays valid independent of the index's entry vector.
struct KVecCursor {
  std::vector<std::vector<unsigned char>> pkeys;
  size_t pos = 0;
};

// ============================================================================
// Lifecycle
// ============================================================================

static bool kvec_create(Ctx * /*ctx*/, const Index & /*index*/,
                        Space::Ref /*space_ref*/, Segment::TrxRef /*trx_ref*/,
                        char * /*err*/, uint32_t /*err_len*/) {
  return false;
}

static bool kvec_drop(Ctx *ctx, const Index & /*index*/,
                      Segment::TrxRef /*trx_ref*/, char * /*err*/,
                      uint32_t /*err_len*/) {
  ctx->user()->entries.clear();
  return false;
}

static bool kvec_load(Ctx * /*ctx*/, const Index & /*index*/,
                      Index::StorageRef /*storage_ref*/, char * /*err*/,
                      uint32_t /*err_len*/) {
  return false;
}

// ============================================================================
// DML
// ============================================================================

static bool kvec_insert(Ctx *ctx, const Index & /*index*/,
                        Segment::TrxRef /*trx_ref*/,
                        IndexScanKey::KeyPartData *key_columns,
                        IndexScanKey::KeyPartData *pkey_columns,
                        IndexScanKey::KeyPartRef * /*key_ref*/, char *err,
                        uint32_t err_len) {
  if (key_columns == nullptr || key_columns[0].data == nullptr ||
      key_columns[0].length != kVecBytes) {
    snprintf(err, err_len, "kvec_knn: expected a %u-byte vector key",
             kVecBytes);
    return true;
  }
  if (pkey_columns == nullptr || pkey_columns[0].data == nullptr ||
      pkey_columns[0].length == 0) {
    snprintf(err, err_len, "kvec_knn: missing primary key");
    return true;
  }
  KVecEntry entry;
  entry.vec.assign(key_columns[0].data,
                   key_columns[0].data + key_columns[0].length);
  entry.pkey.assign(pkey_columns[0].data,
                    pkey_columns[0].data + pkey_columns[0].length);
  ctx->user()->entries.push_back(std::move(entry));
  return false;
}

static bool kvec_mark_delete(Ctx * /*ctx*/, const Index & /*index*/,
                             Segment::TrxRef /*trx_ref*/,
                             IndexScanKey::KeyPartRef * /*key_ref*/,
                             IndexScanKey::KeyPartData * /*key_columns*/,
                             IndexScanKey::KeyPartData * /*pkey_columns*/,
                             bool /*delete_mark*/, char * /*err*/,
                             uint32_t /*err_len*/) {
  // This extension exercises the INSERT->scan path only; the server does not
  // invoke delete maintenance for it, so accept the call as a no-op.
  return false;
}

static bool kvec_purge(Ctx * /*ctx*/, const Index & /*index*/,
                       Segment::TrxRef /*trx_ref*/,
                       IndexScanKey::KeyPartRef * /*key_ref*/,
                       IndexScanKey::KeyPartData * /*key_columns*/,
                       IndexScanKey::KeyPartData * /*pkey_columns*/,
                       char * /*err*/, uint32_t /*err_len*/) {
  return false;
}

// ============================================================================
// Scan
// ============================================================================

static bool kvec_begin(Ctx *ctx, const Index & /*index*/, MtrCtx::Ref /*mctx*/,
                       const IndexScanDesc &scan_desc, Index::Cursor *cursor,
                       bool *eof, char *err, uint32_t err_len) {
  *cursor = nullptr;
  *eof = true;
  ++g_scan_begin_count;

  if (!scan_desc.is_knn() || scan_desc.num_keys() != 1) {
    snprintf(err, err_len, "kvec_knn: only single-key KNN scans are supported");
    return true;
  }
  const IndexScanKey scan_key = scan_desc[0];
  if (!scan_key.is_knn() || scan_key.num_columns() != 1 ||
      !scan_key.is_bounded() || scan_key[0].data == nullptr ||
      scan_key[0].length != kVecBytes) {
    snprintf(err, err_len,
             "kvec_knn: KNN query key must be a single %u-byte vector",
             kVecBytes);
    return true;
  }
  const IndexScanKey::KeyPartData &query = scan_key[0];

  // Rank all entries by exact L2 distance to the query vector.
  struct Hit {
    size_t entry_pos;
    double distance;
  };
  std::vector<Hit> hits;
  const std::vector<KVecEntry> &entries = ctx->user()->entries;
  hits.reserve(entries.size());
  for (size_t i = 0; i < entries.size(); ++i) {
    if (entries[i].vec.size() != kVecBytes) continue;
    hits.push_back(Hit{i, l2_distance_raw(entries[i].vec.data(), query.data)});
  }
  std::sort(hits.begin(), hits.end(), [](const Hit &a, const Hit &b) {
    if (a.distance < b.distance) return true;
    if (a.distance > b.distance) return false;
    return a.entry_pos < b.entry_pos;
  });

  // The server passes LIMIT through scan_desc.limit(); honor it so we hand
  // back at most that many rows. 0 means unlimited.
  const uint32_t limit = scan_desc.limit();
  auto *c = new (std::nothrow) KVecCursor();
  if (c == nullptr) {
    snprintf(err, err_len, "kvec_knn: out of memory allocating cursor");
    return true;
  }
  for (const Hit &hit : hits) {
    if (limit != 0 && c->pkeys.size() >= limit) break;
    c->pkeys.push_back(entries[hit.entry_pos].pkey);
  }

  *cursor = c;
  *eof = c->pkeys.empty();
  return false;
}

static bool kvec_position(Index::Cursor cursor, Index::CursorOp op, bool *eof,
                          char * /*err*/, uint32_t /*err_len*/) {
  auto *c = static_cast<KVecCursor *>(cursor);
  // KNN scans are forward-only; NEXT advances, anything else is a no-op step.
  if (op == Index::CursorOp::Next && c->pos < c->pkeys.size()) {
    ++c->pos;
  }
  *eof = c->pos >= c->pkeys.size();
  return false;
}

static bool kvec_fetch(Index::Cursor cursor,
                       IndexScanKey::KeyPartRef * /*key_ref*/,
                       IndexScanKey::KeyPartData * /*key_columns*/,
                       IndexScanKey::KeyPartData *pkey_columns, char *err,
                       uint32_t err_len) {
  auto *c = static_cast<KVecCursor *>(cursor);
  if (c->pos >= c->pkeys.size()) {
    snprintf(err, err_len, "kvec_knn: fetch past end of cursor");
    return true;
  }
  ++g_scan_fetch_count;
  const std::vector<unsigned char> &pkey = c->pkeys[c->pos];
  pkey_columns[0].data = pkey.data();
  pkey_columns[0].length = static_cast<uint32_t>(pkey.size());
  return false;
}

static bool kvec_save(Index::Cursor /*cursor*/, char * /*err*/,
                      uint32_t /*err_len*/) {
  return false;
}

static bool kvec_restore(Index::Cursor /*cursor*/, MtrCtx::Ref /*mctx*/,
                         bool *eof, char * /*err*/, uint32_t /*err_len*/) {
  *eof = true;
  return false;
}

static void kvec_end(Index::Cursor *cursor) {
  delete static_cast<KVecCursor *>(*cursor);
  *cursor = nullptr;
}

// ============================================================================
// Names
// ============================================================================

static constexpr const char kKVectorType[] = "kvector";
static constexpr const char kKVecIndex[] = "kvec_knn";
static constexpr const char kKVecProfile[] = "kvec_profile_l2";
static constexpr const char kKVecL2Func[] = "kvec_l2_distance";

// A second index type + profile binding a different metric (cosine). Having
// two metrics on two distinct index types lets tests confirm the optimizer
// ties an ORDER BY distance function to the index type its profile actually
// binds: a cosine ORDER BY must only drive a kvec_knn_cos index, never a
// kvec_knn (L2) index. See knn_profile_mismatch.test.
static constexpr const char kKVecIndexCos[] = "kvec_knn_cos";
static constexpr const char kKVecProfileCos[] = "kvec_profile_cos";
static constexpr const char kKVecCosFunc[] = "kvec_cos_distance";

// ============================================================================
// KVECTOR type codec: text "[f0,f1,f2,f3]" <-> 16 little-endian bytes.
// ============================================================================

template <size_t N>
inline constexpr int64_t DECODE_BUFFER_SIZE = static_cast<int64_t>(N);

static void kvector_from_string(std::string_view from, vsql::CustomResult out) {
  vsql::Span<unsigned char> buf = out.buffer();
  if (buf.size() < kVecBytes) {
    out.error("kvector: decode buffer too small");
    return;
  }

  // Parse exactly kVecDim comma-separated floats, tolerating surrounding
  // brackets and spaces.
  const char *p = from.data();
  const char *end = p + from.size();
  auto skip_ws = [&]() {
    while (p < end &&
           (*p == ' ' || *p == '\t' || *p == '[' || *p == ']' || *p == ','))
      ++p;
  };

  uint32_t count = 0;
  while (count < kVecDim) {
    skip_ws();
    if (p >= end) break;
    char *num_end = nullptr;
    const double val = strtod(p, &num_end);
    if (num_end == p) {
      out.error("kvector: expected a number");
      return;
    }
    store_float_le(buf.data() + count * sizeof(float), static_cast<float>(val));
    p = num_end;
    ++count;
  }
  if (count != kVecDim) {
    out.error("kvector: expected exactly 4 elements");
    return;
  }
  out.set_length(kVecBytes);
}

static void kvector_to_string(vsql::CustomArg in, vsql::StringResult out) {
  auto data = in.value();
  char text[128];
  int pos = 0;
  pos += snprintf(text + pos, sizeof(text) - pos, "[");
  for (uint32_t i = 0; i < kVecDim; ++i) {
    const float f = load_float_le(data.data() + i * sizeof(float));
    pos += snprintf(text + pos, sizeof(text) - pos, "%s%g", i == 0 ? "" : ",",
                    static_cast<double>(f));
  }
  pos += snprintf(text + pos, sizeof(text) - pos, "]");
  out.set(std::string_view(text, static_cast<size_t>(pos)));
}

static int kvector_compare(vsql::CustomArg a, vsql::CustomArg b) {
  return memcmp(a.value().data(), b.value().data(), kVecBytes);
}

constexpr auto KVECTOR = vsql::make_type<kKVectorType>()
                             .persisted_length(kVecBytes)
                             .max_decode_buffer_length(DECODE_BUFFER_SIZE<256>)
                             .from_string<&kvector_from_string>()
                             .to_string<&kvector_to_string>()
                             .compare<&kvector_compare>()
                             .intrinsic_default_str("[0,0,0,0]")
                             .build();

// ============================================================================
// Index type registration
// ============================================================================

// clang-format off
static constexpr auto KVEC_KNN_INDEX =
    make_index_type<kKVecIndex, KVecCtx>()
        .lifecycle()
            .create<&kvec_create>()
            .load<&kvec_load>()
            .drop<&kvec_drop>()

        .dml()
            .insert<&kvec_insert>()
            .mark_delete<&kvec_mark_delete>()
            .purge<&kvec_purge>()

        .scan()
            .begin<&kvec_begin>()
            .position<&kvec_position>()
            .fetch<&kvec_fetch>()
            .save<&kvec_save>()
            .restore<&kvec_restore>()
            .end<&kvec_end>()

        .global()
            .capabilities(Index::Support::KNN)
            .storage_props(Index::Storage::HAS_ROW_REF | Index::Storage::REF_LOOKUP)

        .build();

// A second KNN index type, identical in mechanics to kvec_knn (it reuses the
// same callbacks — the scan still computes L2, since this test index is only
// here to be a distinct profile target, not a real cosine index). Its purpose
// is to give the cosine profile a valid index type to bind to, so a cosine
// ORDER BY is only recognized against a kvec_knn_cos index, never a kvec_knn.
static constexpr auto KVEC_KNN_COS_INDEX =
    make_index_type<kKVecIndexCos, KVecCtx>()
        .lifecycle()
            .create<&kvec_create>()
            .load<&kvec_load>()
            .drop<&kvec_drop>()

        .dml()
            .insert<&kvec_insert>()
            .mark_delete<&kvec_mark_delete>()
            .purge<&kvec_purge>()

        .scan()
            .begin<&kvec_begin>()
            .position<&kvec_position>()
            .fetch<&kvec_fetch>()
            .save<&kvec_save>()
            .restore<&kvec_restore>()
            .end<&kvec_end>()

        .global()
            .capabilities(Index::Support::KNN)
            .storage_props(Index::Storage::HAS_ROW_REF | Index::Storage::REF_LOOKUP)

        .build();
// clang-format on

// ============================================================================
// Distance functions + profiles
// ============================================================================

static void kvec_l2_distance_impl(vsql::CustomArg a, vsql::CustomArg b,
                                  vsql::RealResult out) {
  out.set(l2_distance_raw(a.value().data(), b.value().data()));
}

static void kvec_cos_distance_impl(vsql::CustomArg a, vsql::CustomArg b,
                                   vsql::RealResult out) {
  out.set(cosine_distance_raw(a.value().data(), b.value().data()));
}

// Introspection VDFs: expose the scan-callback counters so a test can prove
// the custom index scan actually executed (not a silent filesort fallback).
static void kvec_scan_begin_count(vsql::IntResult out) {
  out.set(g_scan_begin_count);
}

static void kvec_scan_fetch_count(vsql::IntResult out) {
  out.set(g_scan_fetch_count);
}

static const auto KVEC_L2_FN =
    make_index_function<&kvec_l2_distance_impl>(kKVecL2Func)
        .returns(vsql::REAL)
        .param(KVECTOR)
        .param(KVECTOR)
        .deterministic()
        .build();

static const auto KVEC_L2_PROFILE = make_index_profile(kKVecProfile)
                                        .for_type(kKVectorType)
                                        .using_index(kKVecIndex)
                                        .with_function(1, KVEC_L2_FN)
                                        .ordering(Index::Ordering::ASC)
                                        .default_for_type(true)
                                        .build();

static const auto KVEC_COS_FN =
    make_index_function<&kvec_cos_distance_impl>(kKVecCosFunc)
        .returns(vsql::REAL)
        .param(KVECTOR)
        .param(KVECTOR)
        .deterministic()
        .build();

// Binds cosine to kvec_knn_cos (NOT kvec_knn). Not default_for_type — a type
// has at most one default profile, which is the L2 one above.
static const auto KVEC_COS_PROFILE = make_index_profile(kKVecProfileCos)
                                         .for_type(kKVectorType)
                                         .using_index(kKVecIndexCos)
                                         .with_function(1, KVEC_COS_FN)
                                         .ordering(Index::Ordering::ASC)
                                         .build();

// ============================================================================
// Extension entry point
// ============================================================================

static auto INDEX_TYPE = vsql::preview_index_builder::IndexTypeCapability()
                             .index_type(KVEC_KNN_INDEX)
                             .index_type(KVEC_KNN_COS_INDEX);
static auto INDEX_PROFILE =
    vsql::preview_index_builder::IndexProfileCapability()
        .index_profile(KVEC_L2_PROFILE)
        .index_profile(KVEC_COS_PROFILE);

VEF_GENERATE_ENTRY_POINTS(
    vsql::make_extension()
        .with(INDEX_TYPE)
        .with(INDEX_PROFILE)
        .type(KVECTOR)
        // Register each distance function as a plain SQL VDF as well as an
        // index function. The make_index_function bindings above let the
        // optimizer recognize them as a profile's KNN distance; these make_func
        // registrations make them callable in SQL (e.g. in the ORDER BY). Same
        // function pointers.
        // TODO(villagesql-indexing): drop this double-registration once profile
        // bindings are auto-registered into victionary.funcs().
        .func(vsql::make_func<&kvec_l2_distance_impl>(kKVecL2Func)
                  .returns(vsql::REAL)
                  .param(KVECTOR)
                  .param(KVECTOR)
                  .deterministic()
                  .build())
        .func(vsql::make_func<&kvec_cos_distance_impl>(kKVecCosFunc)
                  .returns(vsql::REAL)
                  .param(KVECTOR)
                  .param(KVECTOR)
                  .deterministic()
                  .build())
        .func(vsql::make_func<&kvec_scan_begin_count>("kvec_scan_begin_count")
                  .returns(vsql::INT)
                  .no_params()
                  .build())
        .func(vsql::make_func<&kvec_scan_fetch_count>("kvec_scan_fetch_count")
                  .returns(vsql::INT)
                  .no_params()
                  .build()))
