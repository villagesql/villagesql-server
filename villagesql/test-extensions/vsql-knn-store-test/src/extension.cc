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

// vsql_knn_store_test extension: a FUNCTIONAL KNN custom index that exercises
// the SAME InnoDB-resident surface the real HNSW index (vsql-vector) uses, but
// with a trivially-correct BRUTE-FORCE exact scan as the oracle. It is the union
// of vsql-knn-mem-test (index/KNN/scan/profile) and vsql-storage-test (real
// external column storage), wired the way vsql-vector actually wires it.
//
// Unlike vsql-knn-mem-test -- which stores vectors + primary keys in a flat
// in-memory list and returns the PK at fetch -- this extension:
//
//   * Stores the KVECTOR value in a REAL external column store (make_column_store
//     + StorageCapability + ColumnStoreCapability), so the server hands out a
//     stable col_ref per stored value and persists a rowid_prefix alongside it.
//   * Declares the index with HAS_COLUMN_REF | REF_LOOKUP (NOT HAS_ROW_REF), so
//     the server drives the REF_LOOKUP row-fetch path: the extension returns the
//     col_ref at scan_fetch and the server resolves col_ref -> rowid_prefix ->
//     base-table row (handler::custom_index_ref_to_row).
//   * The index itself stores ONLY the col_ref per row (reference-only, like the
//     real HNSW node) in a real page-backed store -- NOT the vector bytes.
//   * At scan time it resolves each stored col_ref back to the vector via
//     get_key_data (the col_ref_to_data_fn path) and computes distance by
//     dispatching through the registered profile HELPER VDF (index.helper<>),
//     exercising the real protocol-3 dispatch -- not a private C++ loop.
//
// So a regression in any of: InnoDB insert routing, profile resolution/dispatch,
// col_ref_to_data_fn, the REF_LOOKUP col_ref->row read path, or rowid_prefix
// persistence, fails this extension's MTR test.
//
// NOTE: internal testing tool, not an example of how to write an extension. The
// scan is exact brute force (no graph); HNSW-graph correctness stays in
// vsql-vector's own tests.

#include <villagesql/preview/index_builder.h>
#include <villagesql/preview/storage_api.h>
#include <villagesql/preview/storage_builder.h>
#include <villagesql/vsql.h>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <string_view>
#include <vector>

using namespace vsql::preview_index_builder;

namespace storage = vsql::preview_storage;
using vsql::preview_storage::MtrCtx;
using vsql::preview_storage::Segment;
using vsql::preview_storage::Space;
using vsql::preview_storage_builder::ColumnStoreCapability;
using vsql::preview_storage_builder::make_column_store;
using vsql::preview_storage_builder::StorageCapability;

// ============================================================================
// KVECTOR shape and codec
// ============================================================================
//
// Persisted field layout mirrors a real external-column vector (e.g. SVECTOR):
//   [0..7]   Column::Ref placeholder (zero from from_string; the server fills
//            it in with the stable col_ref after the column-store insert).
//   [8..end] kVecDim little-endian float32 elements.
// So the persisted length is 8 + kVecBytes. The 8-byte prefix is what the index
// reads as the col_ref at insert (like vsql-vector create_node).

static constexpr uint32_t kVecDim = 4;
static constexpr uint32_t kVecBytes = kVecDim * sizeof(float);
static constexpr size_t kRefSize = 8;  // sizeof(Column::Ref)
static constexpr size_t kFieldSize = kRefSize + kVecBytes;

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

// Distance over the FLOAT payload (no prefix). Operates on kVecBytes-byte
// float arrays.
static double l2_distance_floats(const unsigned char *a,
                                 const unsigned char *b) {
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
// metric so the extension binds more than one distance function to different
// index types (see kKVecIndexCos) -- the profile-discrimination test relies on
// this. Operates on kVecBytes-byte float arrays (no prefix).
static double cosine_distance_floats(const unsigned char *a,
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

// ============================================================================
// KVECTOR external column store (models vsql-storage-test's STORED_INT store).
// One data page per stored value. Page DATA layout:
//   HEADER_SIZE + 0            [kVecBytes]  float payload (col_data)
//   HEADER_SIZE + kVecBytes    [1]          rowid length
//   HEADER_SIZE + kVecBytes+1  [kRowidMax]  rowid bytes (zero padded)
//   HEADER_SIZE + ...          [8]          last-writer trx_ref
//   HEADER_SIZE + ...          [1]          delete-mark flag
// The rowid_prefix IS persisted and returned at select -- the REF_LOOKUP row
// fetch depends on it (this is exactly the bug-10 surface).
// ============================================================================

static constexpr uint32_t kRowidMax = 32;

constexpr storage::Page::Offset kColOff = storage::Page::HEADER_SIZE;
constexpr storage::Page::Offset kRowidLenOff = kColOff + kVecBytes;
constexpr storage::Page::Offset kRowidOff = kRowidLenOff + 1;
constexpr storage::Page::Offset kTrxOff = kRowidOff + kRowidMax;
constexpr storage::Page::Offset kFlagOff = kTrxOff + 8;

struct KVecColCtx {
  storage::Space::Ref space = 0;
  storage::Segment::PageRef root_page = storage::Page::INVALID_REF;
};

using ColCtx = storage::Column::StorageCtx<KVecColCtx>;

static storage::Column::StorageRef encode_storage_ref(
    storage::Space::Ref space, storage::Segment::PageRef root) {
  return (static_cast<storage::Column::StorageRef>(space) << 32) |
         static_cast<storage::Column::StorageRef>(root);
}

static bool kvec_col_create(ColCtx *ctx, storage::Space::Ref space,
                            storage::Segment::TrxRef trx, uint32_t /*col_len*/,
                            char *err, uint32_t err_len) {
  storage::Segment::PageRef root_page;
  if (storage::Segment::create(space, 1, trx, root_page) !=
      storage::Error::SUCCESS) {
    snprintf(err, err_len, "kvector create: %s", storage::last_error().data());
    return true;
  }
  ctx->user()->space = space;
  ctx->user()->root_page = root_page;
  ctx->set_ref(encode_storage_ref(space, root_page));
  return false;
}

static bool kvec_col_drop(ColCtx *ctx, storage::Segment::TrxRef trx, char *err,
                          uint32_t err_len) {
  if (storage::Segment::drop(ctx->user()->space, trx, ctx->user()->root_page) !=
      storage::Error::SUCCESS) {
    snprintf(err, err_len, "kvector drop: %s", storage::last_error().data());
    return true;
  }
  return false;
}

static bool kvec_col_load(ColCtx *ctx, storage::Column::StorageRef storage_ref,
                          char * /*err*/, uint32_t /*err_len*/) {
  ctx->user()->space = static_cast<storage::Space::Ref>(storage_ref >> 32);
  ctx->user()->root_page =
      static_cast<storage::Segment::PageRef>(storage_ref & 0xFFFFFFFF);
  ctx->set_ref(storage_ref);
  return false;
}

static bool kvec_col_insert(ColCtx *ctx, storage::MtrCtx::Ref mctx,
                            storage::Segment::TrxRef trx,
                            storage::Column::Data col_data,
                            storage::Column::Data rowid_prefix,
                            storage::Column::Ref *col_ref, char *err,
                            uint32_t err_len) {
  if (col_data.length != kVecBytes) {
    snprintf(err, err_len, "kvector insert: expected %u-byte payload, got %u",
             kVecBytes, col_data.length);
    return true;
  }
  if (rowid_prefix.length > kRowidMax) {
    snprintf(err, err_len, "kvector insert: rowid too long (%u > %u)",
             rowid_prefix.length, kRowidMax);
    return true;
  }

  storage::Page root;
  if (root.load(ctx->user()->space, ctx->user()->root_page,
                storage::Page::Latch::EXCLUSIVE,
                mctx) != storage::Error::SUCCESS) {
    snprintf(err, err_len, "kvector insert: root load failed: %s",
             storage::last_error().data());
    return true;
  }
  storage::Segment::Ref seg = storage::Segment::get_header(root, 0);
  if (seg == nullptr) {
    snprintf(err, err_len, "kvector insert: segment header not found");
    return true;
  }
  storage::Page data_page;
  if (data_page.load_new(seg, mctx) != storage::Error::SUCCESS) {
    snprintf(err, err_len, "kvector insert: page alloc failed: %s",
             storage::last_error().data());
    return true;
  }

  data_page.write_string(kColOff, col_data.data, col_data.length, mctx);

  unsigned char rowid_buf[kRowidMax] = {};
  if (rowid_prefix.length > 0)
    memcpy(rowid_buf, rowid_prefix.data, rowid_prefix.length);
  data_page.write_integer_1(kRowidLenOff,
                            static_cast<uint8_t>(rowid_prefix.length), mctx);
  data_page.write_string(kRowidOff, rowid_buf, kRowidMax, mctx);

  data_page.write_integer_8(kTrxOff, static_cast<uint64_t>(trx), mctx);
  data_page.write_integer_1(kFlagOff, 0, mctx);

  *col_ref = static_cast<storage::Column::Ref>(data_page.get_ref());
  return false;
}

static bool kvec_col_select(ColCtx *ctx, storage::MtrCtx::Ref mctx,
                            storage::Column::Ref col_ref,
                            storage::Column::Data *col_data,
                            storage::Column::Data *rowid_prefix,
                            storage::Segment::TrxRef *trx_ref,
                            bool *delete_marked, char *err, uint32_t err_len) {
  auto page_num = static_cast<storage::Segment::PageRef>(col_ref);
  storage::Page page;
  if (page.load(ctx->user()->space, page_num, storage::Page::Latch::SHARED,
                mctx) != storage::Error::SUCCESS) {
    snprintf(err, err_len, "kvector select: page load failed: %s",
             storage::last_error().data());
    return true;
  }
  const unsigned char *base = page.get_data();
  col_data->data = base + kColOff;
  col_data->length = kVecBytes;
  // Return the persisted rowid_prefix (its real length) so the server's
  // REF_LOOKUP path can resolve the row.
  rowid_prefix->data = base + kRowidOff;
  rowid_prefix->length = page.read_integer_1(kRowidLenOff);
  *trx_ref =
      static_cast<storage::Segment::TrxRef>(page.read_integer_8(kTrxOff));
  *delete_marked = page.read_integer_1(kFlagOff) != 0;
  return false;
}

static bool kvec_col_mark_delete(ColCtx *ctx, storage::MtrCtx::Ref mctx,
                                 storage::Segment::TrxRef trx,
                                 storage::Column::Ref col_ref, bool delete_mark,
                                 char *err, uint32_t err_len) {
  auto page_num = static_cast<storage::Segment::PageRef>(col_ref);
  storage::Page page;
  if (page.load(ctx->user()->space, page_num, storage::Page::Latch::EXCLUSIVE,
                mctx) != storage::Error::SUCCESS) {
    snprintf(err, err_len, "kvector mark_delete: page load failed: %s",
             storage::last_error().data());
    return true;
  }
  page.write_integer_1(kFlagOff, delete_mark ? 1 : 0, mctx);
  page.write_integer_8(kTrxOff, static_cast<uint64_t>(trx), mctx);
  return false;
}

static bool kvec_col_purge(ColCtx * /*ctx*/, storage::MtrCtx::Ref /*mctx*/,
                           storage::Segment::TrxRef /*trx*/,
                           storage::Column::Ref /*col_ref*/, char * /*err*/,
                           uint32_t /*err_len*/) {
  return false;
}

// ============================================================================
// KVECTOR type codec: text "[f0,f1,f2,f3]" <-> [8-byte ref placeholder][floats]
// ============================================================================

template <size_t N>
inline constexpr int64_t DECODE_BUFFER_SIZE = static_cast<int64_t>(N);

static void kvector_from_string(std::string_view from, vsql::CustomResult out) {
  vsql::Span<unsigned char> buf = out.buffer();
  if (buf.size() < kFieldSize) {
    out.error("kvector: decode buffer too small");
    return;
  }
  // [0..7] zero Column::Ref placeholder (server fills after column insert).
  memset(buf.data(), 0, kRefSize);
  unsigned char *floats = buf.data() + kRefSize;

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
    store_float_le(floats + count * sizeof(float), static_cast<float>(val));
    p = num_end;
    ++count;
  }
  if (count != kVecDim) {
    out.error("kvector: expected exactly 4 elements");
    return;
  }
  out.set_length(kFieldSize);
}

static void kvector_to_string(vsql::CustomArg in, vsql::StringResult out) {
  auto data = in.value();
  if (data.size() < kFieldSize) return;
  const unsigned char *floats = data.data() + kRefSize;
  char text[128];
  int pos = 0;
  pos += snprintf(text + pos, sizeof(text) - pos, "[");
  for (uint32_t i = 0; i < kVecDim; ++i) {
    const float f = load_float_le(floats + i * sizeof(float));
    pos += snprintf(text + pos, sizeof(text) - pos, "%s%g", i == 0 ? "" : ",",
                    static_cast<double>(f));
  }
  pos += snprintf(text + pos, sizeof(text) - pos, "]");
  out.set(std::string_view(text, static_cast<size_t>(pos)));
}

static int kvector_compare(vsql::CustomArg a, vsql::CustomArg b) {
  // Compare the float payload (skip the ref prefix), like SVECTOR.
  auto va = a.value();
  auto vb = b.value();
  if (va.size() < kFieldSize || vb.size() < kFieldSize) return 0;
  return memcmp(va.data() + kRefSize, vb.data() + kRefSize, kVecBytes);
}

// ============================================================================
// Index: stores ONE col_ref per row (reference-only), brute-force KNN scan.
// ============================================================================

// Per-index state: the flat list of stored col_refs. Persisted on a real
// page-backed store would be ideal, but for a brute-force test index an
// arena-resident list (like vsql-knn-mem-test) is sufficient to exercise the
// col_ref/REF_LOOKUP surface. The col_ref values themselves come from the REAL
// column store, so the col_ref_to_data_fn / rowid_prefix / REF_LOOKUP paths are
// all genuinely driven.
struct KVecIndexCtx {
  std::vector<IndexScanKey::KeyPartRef> col_refs;
};

using IdxCtx = Index::StorageCtx<KVecIndexCtx>;

// Observability: prove the custom scan actually executed.
static long long g_scan_begin_count = 0;
static long long g_scan_fetch_count = 0;

struct KVecCursor {
  std::vector<IndexScanKey::KeyPartRef> refs;
  size_t pos = 0;
};

// Reads the 8-byte big-endian column reference from the extended vector field's
// prefix (mach_write_to_8, mirrored by mach_read_from_8) -- exactly how
// vsql-vector's create_node obtains its VID.
static IndexScanKey::KeyPartRef read_col_ref_be(const unsigned char *p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v = (v << 8) | static_cast<uint64_t>(p[i]);
  return static_cast<IndexScanKey::KeyPartRef>(v);
}

static bool kvec_create(IdxCtx * /*ctx*/, const Index & /*index*/,
                        Space::Ref /*space_ref*/, Segment::TrxRef /*trx_ref*/,
                        char * /*err*/, uint32_t /*err_len*/) {
  return false;
}

static bool kvec_drop(IdxCtx *ctx, const Index & /*index*/,
                      Segment::TrxRef /*trx_ref*/, char * /*err*/,
                      uint32_t /*err_len*/) {
  ctx->user()->col_refs.clear();
  return false;
}

static bool kvec_load(IdxCtx * /*ctx*/, const Index & /*index*/,
                      Index::StorageRef /*storage_ref*/, char * /*err*/,
                      uint32_t /*err_len*/) {
  return false;
}

static bool kvec_insert(IdxCtx *ctx, const Index & /*index*/,
                        Segment::TrxRef /*trx_ref*/,
                        IndexScanKey::KeyPartData *key_columns,
                        IndexScanKey::KeyPartData * /*pkey_columns*/,
                        IndexScanKey::KeyPartRef * /*key_ref*/, char *err,
                        uint32_t err_len) {
  // For an externally-stored column the index entry's key field carries the
  // 8-byte col_ref prefix (the server wrote it during the clustered insert).
  // Store that reference -- NOT the vector bytes.
  if (key_columns == nullptr || key_columns[0].data == nullptr ||
      key_columns[0].length < kRefSize) {
    snprintf(err, err_len, "kvec_store: key column too short for a col_ref");
    return true;
  }
  ctx->user()->col_refs.push_back(read_col_ref_be(key_columns[0].data));
  return false;
}

static bool kvec_mark_delete(IdxCtx * /*ctx*/, const Index & /*index*/,
                             Segment::TrxRef /*trx_ref*/,
                             IndexScanKey::KeyPartRef * /*key_ref*/,
                             IndexScanKey::KeyPartData * /*key_columns*/,
                             IndexScanKey::KeyPartData * /*pkey_columns*/,
                             bool /*delete_mark*/, char * /*err*/,
                             uint32_t /*err_len*/) {
  return false;
}

static bool kvec_purge(IdxCtx * /*ctx*/, const Index & /*index*/,
                       Segment::TrxRef /*trx_ref*/,
                       IndexScanKey::KeyPartRef * /*key_ref*/,
                       IndexScanKey::KeyPartData * /*key_columns*/,
                       IndexScanKey::KeyPartData * /*pkey_columns*/,
                       char * /*err*/, uint32_t /*err_len*/) {
  return false;
}

// fn_id the L2 distance helper is registered under (with_helper(1, ...)).
static constexpr uint32_t kDistanceHelperFnId = 1;
static constexpr uint32_t kKeyPos = 0;

static bool kvec_begin(IdxCtx *ctx, const Index &index, MtrCtx::Ref /*mctx*/,
                       const IndexScanDesc &scan_desc, Index::Cursor *cursor,
                       bool *eof, char *err, uint32_t err_len) {
  *cursor = nullptr;
  *eof = true;
  ++g_scan_begin_count;

  if (!scan_desc.is_knn() || scan_desc.num_keys() != 1) {
    snprintf(err, err_len, "kvec_store: only single-key KNN scans supported");
    return true;
  }
  const IndexScanKey scan_key = scan_desc[0];
  if (!scan_key.is_knn() || scan_key.num_columns() != 1 ||
      !scan_key.is_bounded() || scan_key[0].data == nullptr) {
    snprintf(err, err_len, "kvec_store: bad KNN query key");
    return true;
  }
  // The query key is a full persisted KVECTOR value [ref placeholder][floats].
  const IndexScanKey::KeyPartData &query = scan_key[0];

  // Rank every stored col_ref by exact L2 distance, computed by dispatching the
  // registered profile HELPER VDF (index.helper) -- exercising the real
  // protocol-3 profile dispatch on server-resolved column data.
  struct Hit {
    IndexScanKey::KeyPartRef ref;
    double distance;
  };
  std::vector<Hit> hits;
  const auto &col_refs = ctx->user()->col_refs;
  hits.reserve(col_refs.size());

  for (IndexScanKey::KeyPartRef ref : col_refs) {
    // Resolve the stored col_ref back to the column's float payload via the
    // server (col_ref_to_data_fn). The column store returns the kVecBytes float
    // payload (no ref prefix); reconstruct a full [prefix][floats] value so it
    // is a valid VDF operand matching the query key, exactly like vsql-vector's
    // fetch_vector.
    IndexScanKey::KeyPartData raw;
    if (index.get_key_data(ref, &raw)) {
      snprintf(err, err_len, "kvec_store: get_key_data failed: %s",
               index.get_error());
      return true;
    }
    unsigned char stored[kFieldSize] = {};
    const uint32_t floats_len = raw.length < kVecBytes ? raw.length : kVecBytes;
    memcpy(stored + kRefSize, raw.data, floats_len);
    const IndexScanKey::KeyPartData stored_val{stored, kFieldSize};

    double d = 0.0;
    index.helper<double>(kKeyPos, kDistanceHelperFnId, &d, query, stored_val);
    hits.push_back(Hit{ref, d});
  }

  std::sort(hits.begin(), hits.end(), [](const Hit &a, const Hit &b) {
    if (a.distance < b.distance) return true;
    if (a.distance > b.distance) return false;
    return a.ref < b.ref;
  });

  const uint32_t limit = scan_desc.limit();
  auto *c = new (std::nothrow) KVecCursor();
  if (c == nullptr) {
    snprintf(err, err_len, "kvec_store: out of memory allocating cursor");
    return true;
  }
  for (const Hit &hit : hits) {
    if (limit != 0 && c->refs.size() >= limit) break;
    c->refs.push_back(hit.ref);
  }

  *cursor = c;
  *eof = c->refs.empty();
  return false;
}

static bool kvec_position(Index::Cursor cursor, Index::CursorOp op, bool *eof,
                          char * /*err*/, uint32_t /*err_len*/) {
  auto *c = static_cast<KVecCursor *>(cursor);
  if (op == Index::CursorOp::Next && c->pos < c->refs.size()) ++c->pos;
  *eof = c->pos >= c->refs.size();
  return false;
}

static bool kvec_fetch(Index::Cursor cursor, IndexScanKey::KeyPartRef *key_ref,
                       IndexScanKey::KeyPartData * /*key_columns*/,
                       IndexScanKey::KeyPartData * /*pkey_columns*/, char *err,
                       uint32_t err_len) {
  auto *c = static_cast<KVecCursor *>(cursor);
  if (c->pos >= c->refs.size()) {
    snprintf(err, err_len, "kvec_store: fetch past end of cursor");
    return true;
  }
  ++g_scan_fetch_count;
  // REF_LOOKUP: return the stored col_ref; the server resolves it to the row
  // (col_ref -> rowid_prefix -> clustered row). pkey_columns is left unset.
  if (key_ref != nullptr) *key_ref = c->refs[c->pos];
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
static constexpr const char kKVecIndex[] = "kvec_store";
static constexpr const char kKVecProfile[] = "kvec_store_profile_l2";
static constexpr const char kKVecL2Func[] = "kvec_store_l2_distance";

// A second index type + profile binding a different metric (cosine). The
// optimizer must tie an ORDER BY distance function to the index type whose
// profile actually binds it: a cosine ORDER BY may only drive a kvec_store_cos
// index, never a kvec_store (L2) one. Exercised by knn_profile_mismatch.
static constexpr const char kKVecIndexCos[] = "kvec_store_cos";
static constexpr const char kKVecProfileCos[] = "kvec_store_profile_cos";
static constexpr const char kKVecCosFunc[] = "kvec_store_cos_distance";

// ============================================================================
// Type + column-store registration
// ============================================================================

constexpr auto KVECTOR = vsql::make_type<kKVectorType>()
                             .persisted_length(kFieldSize)
                             .max_decode_buffer_length(DECODE_BUFFER_SIZE<256>)
                             .from_string<&kvector_from_string>()
                             .to_string<&kvector_to_string>()
                             .compare<&kvector_compare>()
                             .intrinsic_default_str("[0,0,0,0]")
                             .build();

static constexpr auto kKVectorStorage =
    make_column_store<KVecColCtx>(KVECTOR)
        .create<&kvec_col_create>()
        .drop<&kvec_col_drop>()
        .load<&kvec_col_load>()
        .insert<&kvec_col_insert>()
        .select<&kvec_col_select>()
        .mark_delete<&kvec_col_mark_delete>()
        .purge<&kvec_col_purge>()
        .build();

// ============================================================================
// Index type registration (HAS_COLUMN_REF | REF_LOOKUP -- the real surface)
// ============================================================================

// clang-format off
static constexpr auto KVEC_STORE_INDEX =
    make_index_type<kKVecIndex, KVecIndexCtx>()
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
            .storage_props(Index::Storage::HAS_COLUMN_REF |
                           Index::Storage::REF_LOOKUP)

        .build();
// clang-format on

// A second index type so the cosine profile has a valid index to bind to. It
// reuses the same generic lifecycle/DML/scan callbacks as the L2 index -- its
// only purpose is to be a distinct profile target, so a cosine ORDER BY is
// recognized only against a kvec_store_cos index, never a kvec_store (L2) one.
// clang-format off
static constexpr auto KVEC_STORE_COS_INDEX =
    make_index_type<kKVecIndexCos, KVecIndexCtx>()
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
            .storage_props(Index::Storage::HAS_COLUMN_REF |
                           Index::Storage::REF_LOOKUP)

        .build();
// clang-format on

// ============================================================================
// Distance function + profile
// ============================================================================

// The distance VDF operates on two full persisted KVECTOR values; skip the
// 8-byte ref prefix and compute L2 over the floats, like SVECTOR's distance.
static void kvec_l2_distance_fn(vsql::CustomArg a, vsql::CustomArg b,
                                vsql::RealResult out) {
  auto va = a.value();
  auto vb = b.value();
  if (va.size() < kFieldSize || vb.size() < kFieldSize) {
    out.error("kvec_store_l2_distance: operand too short");
    return;
  }
  out.set(l2_distance_floats(va.data() + kRefSize, vb.data() + kRefSize));
}

static void kvec_scan_begin_count(vsql::IntResult out) {
  out.set(g_scan_begin_count);
}
static void kvec_scan_fetch_count(vsql::IntResult out) {
  out.set(g_scan_fetch_count);
}

static const auto KVEC_L2_FN =
    make_index_function<&kvec_l2_distance_fn>(kKVecL2Func)
        .returns(vsql::REAL)
        .param(KVECTOR)
        .param(KVECTOR)
        .deterministic()
        .build();

static const auto KVEC_L2_PROFILE =
    make_index_profile(kKVecProfile)
        .for_type(kKVectorType)
        .using_index(kKVecIndex)
        .with_function(1, KVEC_L2_FN)
        .with_helper(1, KVEC_L2_FN)
        .ordering(Index::Ordering::ASC)
        .default_for_type(true)
        .build();

// Cosine metric, bound to the kvec_store_cos index (NOT kvec_store). Not
// default_for_type -- a type may have only one default profile, and L2 owns it.
static void kvec_cos_distance_fn(vsql::CustomArg a, vsql::CustomArg b,
                                 vsql::RealResult out) {
  auto va = a.value();
  auto vb = b.value();
  if (va.size() < kFieldSize || vb.size() < kFieldSize) {
    out.error("kvec_store_cos_distance: operand too short");
    return;
  }
  out.set(cosine_distance_floats(va.data() + kRefSize, vb.data() + kRefSize));
}

static const auto KVEC_COS_FN =
    make_index_function<&kvec_cos_distance_fn>(kKVecCosFunc)
        .returns(vsql::REAL)
        .param(KVECTOR)
        .param(KVECTOR)
        .deterministic()
        .build();

static const auto KVEC_COS_PROFILE =
    make_index_profile(kKVecProfileCos)
        .for_type(kKVectorType)
        .using_index(kKVecIndexCos)
        .with_function(1, KVEC_COS_FN)
        .with_helper(1, KVEC_COS_FN)
        .ordering(Index::Ordering::ASC)
        .build();

// ============================================================================
// Extension entry point
// ============================================================================

static auto STORAGE = StorageCapability{};
static auto COLUMN_STORE = ColumnStoreCapability().column_store(kKVectorStorage);
static auto INDEX_TYPE = IndexTypeCapability()
                             .index_type(KVEC_STORE_INDEX)
                             .index_type(KVEC_STORE_COS_INDEX);
static auto INDEX_PROFILE = IndexProfileCapability()
                                .index_profile(KVEC_L2_PROFILE)
                                .index_profile(KVEC_COS_PROFILE);

using namespace vsql;

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .with(STORAGE)
        .with(COLUMN_STORE)
        .with(INDEX_TYPE)
        .with(INDEX_PROFILE)
        .type(KVECTOR)
        // Also register the distance function as a plain SQL VDF so it is
        // callable in the ORDER BY (same fn pointer as the index function).
        .func(make_func<&kvec_l2_distance_fn>(kKVecL2Func)
                  .returns(REAL)
                  .param(KVECTOR)
                  .param(KVECTOR)
                  .deterministic()
                  .build())
        .func(make_func<&kvec_cos_distance_fn>(kKVecCosFunc)
                  .returns(REAL)
                  .param(KVECTOR)
                  .param(KVECTOR)
                  .deterministic()
                  .build())
        .func(make_func<&kvec_scan_begin_count>("kvec_store_scan_begin_count")
                  .returns(INT)
                  .no_params()
                  .build())
        .func(make_func<&kvec_scan_fetch_count>("kvec_store_scan_fetch_count")
                  .returns(INT)
                  .no_params()
                  .build()))
