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

#include "index.h"
#include "type.h"

#include <villagesql/preview/index_builder.h>
#include <villagesql/preview/table_storage.h>
#include <villagesql/vsql.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <vector>

// Defined in extension.cc.
extern vsql::preview_table_storage::TableStorageCapability g_hidden_table;

// Process-wide counter of vec_chain_index_scan_begin invocations.
// Exposed via the vec_chain_scan_count VDF so tests can assert the
// optimizer actually routed a query through the custom index instead
// of falling back to a full base-table scan + sort.
std::atomic<uint64_t> g_vec_chain_scan_count{0};

using vsql::preview_index_builder::Index;
using vsql::preview_index_builder::IndexScanDesc;
using vsql::preview_index_builder::IndexScanKey;
using vsql::preview_storage::MtrCtx;
using vsql::preview_storage::Segment;
using vsql::preview_storage::Space;

namespace {

// Forward declaration so vec_chain_index_scan_begin (declared earlier
// to register with the SDK builder) can call into the helper that
// opens a fresh hidden-table handle.
bool attach_inspector_handle(const vef_preview_table_storage_t **abi_out,
                             vef_table_storage_t **table_out,
                             vef_table_storage_handle_t **handle_out,
                             char *error_msg, uint32_t error_msg_len);

// Per-loaded-index storage. Holds the monotonic insert counter that
// populates each row's `seq` column. TODO(villagesql-indexing): on
// vec_chain_index_load, seed this from MAX(seq) in the hidden table
// (scan by_seq DESC LIMIT 1) so restart preserves monotonicity. For
// milestone 3 the counter just needs to be unique within a single
// process lifetime.
struct VecChainIndexStorage {
  std::atomic<uint64_t> next_seq{1};
};

// Result of a chain traversal: one entry per node visited, with its
// computed distance to the query vector and a copy of the data we'll
// hand back to the server (pkey bytes + key bytes). Sorted by distance
// in scan_begin and truncated to the query's limit.
struct VecChainHit {
  double distance = 0.0;
  std::vector<unsigned char> pkey_bytes;
  std::vector<unsigned char> key_bytes;
};

struct VecChainCursor {
  std::vector<VecChainHit> hits;
  size_t pos = 0;
};

using VecChainIndexCtx = Index::StorageCtx<VecChainIndexStorage>;

constexpr const char kVecChainIndexTypeName[] = "vec_chain";

// Hidden-table schema. PK = pk_id (base row PK bytes), payload = vec
// (encoded vector) + prev_gref (chain link) + seq (monotonic, used by
// the by_seq secondary index for head lookup).
const vef_table_storage_col_def_t kVecChainColumns[] = {
    {.name = "pk_id",
     .type = VEF_TABLE_STORAGE_COL_BYTES,
     .max_length = 255,
     .nullable = false},
    {.name = "vec",
     .type = VEF_TABLE_STORAGE_COL_BYTES,
     .max_length = 255,
     .nullable = false},
    {.name = "prev_gref",
     .type = VEF_TABLE_STORAGE_COL_BYTES,
     // The gref is the engine's row ref for this hidden table. For an
     // InnoDB clustered index keyed on pk_id VARBINARY(255), that's
     // ~256 bytes (PK length + length prefix). Size up so it fits.
     // Once REF mode lands and we size pk_id to handler::ref_length,
     // this can shrink to match.
     .max_length = 4096,
     .nullable = true},
    {.name = "seq",
     .type = VEF_TABLE_STORAGE_COL_UINT64,
     .max_length = 8,
     .nullable = false},
    // Tombstone marker: 0 = live, 1 = deleted. The walker emits live
    // rows as results but follows prev_gref through tombstones, so
    // deleting a non-tail node doesn't break the chain. Compare with
    // MariaDB hlindex which encodes the same idea by making tref
    // nullable. update_row flips this on mark_delete.
    {.name = "deleted",
     .type = VEF_TABLE_STORAGE_COL_UINT64,
     .max_length = 8,
     .nullable = false},
};
const uint32_t kVecChainPrimaryKey[] = {0};
const uint32_t kVecChainBySeqColumns[] = {3};
const vef_table_storage_index_def_t kVecChainSecondaryIndexes[] = {
    {.name = "by_seq",
     .column_indices = kVecChainBySeqColumns,
     .column_count = 1,
     .unique = false},
};

bool vec_chain_index_table_storage_def(const vef_index_ctx_t * /*index_ctx*/,
                                       vef_table_storage_def_t *def_out,
                                       char * /*error_msg*/,
                                       uint32_t /*error_msg_len*/) {
  def_out->version = 1;
  // TODO(villagesql-indexing): logical name hardcoded — only one
  // vec_chain index allowed system-wide. Lift when per-index naming
  // lands.
  def_out->logical_name = "vec_chain";
  def_out->columns = kVecChainColumns;
  def_out->column_count =
      sizeof(kVecChainColumns) / sizeof(kVecChainColumns[0]);
  def_out->primary_key_columns = kVecChainPrimaryKey;
  def_out->primary_key_column_count =
      sizeof(kVecChainPrimaryKey) / sizeof(kVecChainPrimaryKey[0]);
  def_out->secondary_indexes = kVecChainSecondaryIndexes;
  def_out->secondary_index_count =
      sizeof(kVecChainSecondaryIndexes) / sizeof(kVecChainSecondaryIndexes[0]);
  return false;
}

// Milestone 2: all of these are no-ops. The hidden table is created by
// the server; nothing else is wired yet. Real implementations land in
// later milestones (insert/delete maintenance, scan via gref chain).

bool vec_chain_index_create(VecChainIndexCtx * /*ctx*/, const Index & /*index*/,
                            Space::Ref /*space_ref*/,
                            Segment::TrxRef /*trx_ref*/, char * /*error_msg*/,
                            uint32_t /*error_msg_len*/) {
  return false;
}

bool vec_chain_index_load(VecChainIndexCtx * /*ctx*/, const Index & /*index*/,
                          Index::StorageRef /*storage_ref*/,
                          char * /*error_msg*/, uint32_t /*error_msg_len*/) {
  return false;
}

bool vec_chain_index_drop(VecChainIndexCtx * /*ctx*/, const Index & /*index*/,
                          Segment::TrxRef /*trx_ref*/, char * /*error_msg*/,
                          uint32_t /*error_msg_len*/) {
  return false;
}

bool vec_chain_index_insert(VecChainIndexCtx *ctx, const Index &index,
                            Segment::TrxRef /*trx_ref*/,
                            IndexScanKey::KeyPartData *key_columns,
                            IndexScanKey::KeyPartData *pkey_columns,
                            IndexScanKey::KeyPartRef * /*key_ref*/,
                            char *error_msg, uint32_t error_msg_len) {
  if (ctx == nullptr || key_columns == nullptr || pkey_columns == nullptr ||
      index.get_num_key_cols() != 1 || index.get_primary_num_key_cols() < 1) {
    snprintf(error_msg, error_msg_len,
             "vec_chain insert requires one key column and a primary key");
    return true;
  }
  if (index.get_table_storage_handle() == nullptr) {
    snprintf(error_msg, error_msg_len,
             "vec_chain insert requires a server-provided hidden table handle");
    return true;
  }

  const vef_preview_table_storage_t *abi = g_hidden_table.abi();
  if (abi == nullptr) {
    snprintf(error_msg, error_msg_len,
             "table_storage capability is unavailable");
    return true;
  }

  // Capture the current head's gref so we can link the new node to it.
  // Scan the by_seq secondary index in DESC order, take the first row,
  // ask for its position(). Empty table → no head → prev_gref stays NULL.
  std::vector<unsigned char> prev_gref_bytes;
  bool has_prev = false;
  {
    vef_table_storage_scan_t scan{
        .version = 1,
        .scan_type = VEF_TABLE_STORAGE_SCAN_SECONDARY_INDEX,
        .key_values = nullptr,
        .key_value_count = 0,
        .limit = 1,
        .secondary_index_name = "by_seq",
        .direction = VEF_TABLE_STORAGE_SCAN_DIR_DESC,
    };
    vef_table_storage_cursor_t *cursor = nullptr;
    bool eof = true;
    if (abi->scan_begin(index.get_table_storage_handle(), &scan, &cursor, &eof,
                        error_msg, error_msg_len)) {
      return true;
    }
    if (!eof && cursor != nullptr) {
      const unsigned char *ref = nullptr;
      uint32_t ref_len = 0;
      if (abi->scan_position(cursor, &ref, &ref_len, error_msg,
                             error_msg_len)) {
        abi->scan_end(cursor);
        return true;
      }
      prev_gref_bytes.assign(ref, ref + ref_len);
      has_prev = true;
    }
    abi->scan_end(cursor);
  }

  // The hidden-table ABI stores integer column values as decimal ASCII
  // (see parse_integer in table_storage.cc), so render seq into a stack
  // buffer that way.
  const uint64_t seq = ctx->user()->next_seq.fetch_add(1);
  char seq_buf[32];
  const int seq_len = snprintf(seq_buf, sizeof(seq_buf), "%llu",
                               static_cast<unsigned long long>(seq));

  static const char kZero[] = "0";
  vef_table_storage_value_t values[5] = {
      // pk_id: copy from base PK columns. Milestone 3 supports only
      // single-column PKs; multi-column PKs would need encoding.
      {.data = pkey_columns[0].data,
       .length = pkey_columns[0].length,
       .is_null = false},
      // vec: the indexed column's raw bytes.
      {.data = key_columns[0].data,
       .length = key_columns[0].length,
       .is_null = false},
      // prev_gref: previous head's gref, or NULL if this is the first
      // node in the chain.
      {.data = has_prev ? prev_gref_bytes.data() : nullptr,
       .length = has_prev ? static_cast<uint32_t>(prev_gref_bytes.size()) : 0,
       .is_null = !has_prev},
      // seq.
      {.data = reinterpret_cast<const unsigned char *>(seq_buf),
       .length = static_cast<uint32_t>(seq_len),
       .is_null = false},
      // deleted: 0 (live).
      {.data = reinterpret_cast<const unsigned char *>(kZero),
       .length = 1,
       .is_null = false},
  };

  return abi->insert(index.get_table_storage_handle(), values, 5, error_msg,
                     error_msg_len);
}

bool vec_chain_index_mark_delete(VecChainIndexCtx * /*ctx*/, const Index &index,
                                 Segment::TrxRef /*trx_ref*/,
                                 IndexScanKey::KeyPartRef * /*key_ref*/,
                                 IndexScanKey::KeyPartData * /*key_columns*/,
                                 IndexScanKey::KeyPartData *pkey_columns,
                                 bool delete_mark, char *error_msg,
                                 uint32_t error_msg_len) {
  if (!delete_mark) return false;
  if (pkey_columns == nullptr || index.get_primary_num_key_cols() < 1) {
    snprintf(error_msg, error_msg_len,
             "vec_chain delete requires a primary key");
    return true;
  }
  if (index.get_table_storage_handle() == nullptr) {
    snprintf(error_msg, error_msg_len,
             "vec_chain delete requires a server-provided hidden table handle");
    return true;
  }

  const vef_preview_table_storage_t *abi = g_hidden_table.abi();
  if (abi == nullptr) {
    snprintf(error_msg, error_msg_len,
             "table_storage capability is unavailable");
    return true;
  }

  // Tombstone: read the existing row's columns, then write back with
  // deleted=1. The neighbors (prev_gref of newer nodes) still point at
  // this row's gref, so the walker can pass through.
  vef_table_storage_value_t key_values[1] = {
      {.data = pkey_columns[0].data,
       .length = pkey_columns[0].length,
       .is_null = false},
  };
  vef_table_storage_scan_t scan{
      .version = 1,
      .scan_type = VEF_TABLE_STORAGE_SCAN_PRIMARY_KEY,
      .key_values = key_values,
      .key_value_count = 1,
      .limit = 1,
      .secondary_index_name = nullptr,
      .direction = VEF_TABLE_STORAGE_SCAN_DIR_ASC,
  };
  vef_table_storage_cursor_t *cursor = nullptr;
  bool eof = true;
  if (abi->scan_begin(index.get_table_storage_handle(), &scan, &cursor, &eof,
                      error_msg, error_msg_len)) {
    return true;
  }
  if (eof || cursor == nullptr) {
    // Row not found — nothing to tombstone. Not an error: ON DUPLICATE
    // and similar flows can issue a mark_delete for a row that was
    // never inserted (or was already purged).
    abi->scan_end(cursor);
    return false;
  }

  vef_table_storage_value_t existing[5]{};
  if (abi->scan_fetch(cursor, existing, 5, error_msg, error_msg_len)) {
    abi->scan_end(cursor);
    return true;
  }
  // Copy column bytes out of the cursor — scan_end may invalidate them.
  std::vector<unsigned char> pk_bytes(existing[0].data,
                                      existing[0].data + existing[0].length);
  std::vector<unsigned char> vec_bytes(existing[1].data,
                                       existing[1].data + existing[1].length);
  const bool prev_gref_null = existing[2].is_null;
  std::vector<unsigned char> prev_gref_bytes(
      existing[2].data, existing[2].data + existing[2].length);
  std::vector<unsigned char> seq_bytes(existing[3].data,
                                       existing[3].data + existing[3].length);
  abi->scan_end(cursor);

  static const char kOne[] = "1";
  vef_table_storage_value_t updated[5] = {
      {.data = pk_bytes.data(),
       .length = static_cast<uint32_t>(pk_bytes.size()),
       .is_null = false},
      {.data = vec_bytes.data(),
       .length = static_cast<uint32_t>(vec_bytes.size()),
       .is_null = false},
      {.data = prev_gref_null ? nullptr : prev_gref_bytes.data(),
       .length =
           prev_gref_null ? 0 : static_cast<uint32_t>(prev_gref_bytes.size()),
       .is_null = prev_gref_null},
      {.data = seq_bytes.data(),
       .length = static_cast<uint32_t>(seq_bytes.size()),
       .is_null = false},
      {.data = reinterpret_cast<const unsigned char *>(kOne),
       .length = 1,
       .is_null = false},
  };
  return abi->update_row(index.get_table_storage_handle(), key_values, 1,
                         updated, 5, error_msg, error_msg_len);
}

bool vec_chain_index_purge(VecChainIndexCtx * /*ctx*/, const Index & /*index*/,
                           Segment::TrxRef /*trx_ref*/,
                           IndexScanKey::KeyPartRef * /*key_ref*/,
                           IndexScanKey::KeyPartData * /*key_columns*/,
                           IndexScanKey::KeyPartData * /*pkey_columns*/,
                           char * /*error_msg*/, uint32_t /*error_msg_len*/) {
  return false;
}

// Walk the chain from the head (newest insert) backwards via prev_gref,
// computing distance to `query` at each node. Stores all results in
// cursor->hits; the caller is responsible for sorting and trimming.
// Returns true on error.
bool walk_chain(const vef_preview_table_storage_t *abi,
                vef_table_storage_handle_t *handle,
                const vef_storage_col_data_t &query, VecChainCursor *cursor,
                char *error_msg, uint32_t error_msg_len) {
  // Find the head row (highest seq).
  std::vector<unsigned char> next_gref;
  bool has_next = false;
  {
    vef_table_storage_scan_t scan{
        .version = 1,
        .scan_type = VEF_TABLE_STORAGE_SCAN_SECONDARY_INDEX,
        .key_values = nullptr,
        .key_value_count = 0,
        .limit = 1,
        .secondary_index_name = "by_seq",
        .direction = VEF_TABLE_STORAGE_SCAN_DIR_DESC,
    };
    vef_table_storage_cursor_t *head_cursor = nullptr;
    bool eof = true;
    if (abi->scan_begin(handle, &scan, &head_cursor, &eof, error_msg,
                        error_msg_len)) {
      return true;
    }
    if (!eof && head_cursor != nullptr) {
      const unsigned char *ref = nullptr;
      uint32_t ref_len = 0;
      if (abi->scan_position(head_cursor, &ref, &ref_len, error_msg,
                             error_msg_len)) {
        abi->scan_end(head_cursor);
        return true;
      }
      next_gref.assign(ref, ref + ref_len);
      has_next = true;
    }
    abi->scan_end(head_cursor);
  }
  if (!has_next) return false;

  // The query vector tells us the bytes-per-element (float vs double).
  // For milestone 5 we only support float vectors (matches the
  // optimizer's MVECTOR-shaped recognition).
  const VecChainParams params{
      .dimension = static_cast<int64_t>(query.length / sizeof(float)),
      .bytes_per_elem = sizeof(float)};

  while (has_next) {
    vef_table_storage_cursor_t *node_cursor = nullptr;
    bool eof = true;
    if (abi->scan_seek(handle, next_gref.data(),
                       static_cast<uint32_t>(next_gref.size()), &node_cursor,
                       &eof, error_msg, error_msg_len)) {
      return true;
    }
    if (eof || node_cursor == nullptr) {
      // Dangling gref — the row was deleted underneath us. Stop the
      // walk; the chain is implicitly truncated for this scan.
      abi->scan_end(node_cursor);
      break;
    }

    // Fetch the row's columns: pk_id, vec, prev_gref, seq, deleted.
    vef_table_storage_value_t values[5]{};
    if (abi->scan_fetch(node_cursor, values, 5, error_msg, error_msg_len)) {
      abi->scan_end(node_cursor);
      return true;
    }

    // Tombstone check: a deleted row stays in the chain so that newer
    // nodes' prev_gref links keep working, but it's not emitted as a
    // search result.
    const bool deleted = values[4].length > 0 && values[4].data != nullptr &&
                         values[4].data[0] != '0';

    // Compute distance. The stored vec length must match the query's;
    // if not, skip (defensive — shouldn't happen for a healthy index).
    if (!deleted && values[1].length == query.length) {
      VecChainHit hit;
      hit.distance =
          vec_chain_l2_distance_raw(values[1].data, query.data, params);
      hit.pkey_bytes.assign(values[0].data, values[0].data + values[0].length);
      hit.key_bytes.assign(values[1].data, values[1].data + values[1].length);
      cursor->hits.push_back(std::move(hit));
    }

    // Follow prev_gref to the next node in the chain.
    if (values[2].is_null || values[2].length == 0) {
      has_next = false;
    } else {
      next_gref.assign(values[2].data, values[2].data + values[2].length);
    }
    abi->scan_end(node_cursor);
  }
  return false;
}

bool vec_chain_index_scan_begin(VecChainIndexCtx * /*ctx*/, const Index &index,
                                MtrCtx::Ref /*mctx*/,
                                const IndexScanDesc &scan_desc,
                                Index::Cursor *cursor, bool *eof,
                                char *error_msg, uint32_t error_msg_len) {
  *cursor = nullptr;
  *eof = true;

  g_vec_chain_scan_count.fetch_add(1);

  if (!scan_desc.is_knn() || scan_desc.num_keys() != 1) {
    snprintf(error_msg, error_msg_len,
             "vec_chain index only supports one-column KNN scans");
    return true;
  }
  const IndexScanKey scan_key = scan_desc[0];
  if (!scan_key.is_knn() || scan_key.num_columns() != 1 ||
      !scan_key.is_bounded() || scan_key[0].data == nullptr) {
    snprintf(error_msg, error_msg_len,
             "vec_chain index only supports one-column KNN scans");
    return true;
  }

  const vef_preview_table_storage_t *abi = g_hidden_table.abi();
  if (abi == nullptr) {
    snprintf(error_msg, error_msg_len,
             "table_storage capability is unavailable");
    return true;
  }

  // Choose a hidden-table handle: prefer the one the runtime supplies
  // (set by the hidden-table backend's before_callback on the write
  // path), otherwise open a temporary read-only handle for this scan.
  // TODO(villagesql-indexing): the runtime should also supply a handle
  // on the read path — adding a backend->prepare_table_reads hook
  // would amortize handle setup across statements instead of forcing
  // each scan_begin to open one.
  vef_table_storage_handle_t *handle = index.get_table_storage_handle();
  vef_table_storage_handle_t *local_handle = nullptr;
  vef_table_storage_t *local_table = nullptr;
  if (handle == nullptr) {
    const vef_preview_table_storage_t *attached_abi = nullptr;
    if (attach_inspector_handle(&attached_abi, &local_table, &local_handle,
                                error_msg, error_msg_len)) {
      return true;
    }
    handle = local_handle;
  }

  auto *c = new (std::nothrow) VecChainCursor();
  if (c == nullptr) {
    if (local_handle != nullptr) {
      abi->close(local_handle);
      abi->drop(local_table, error_msg, error_msg_len);
    }
    snprintf(error_msg, error_msg_len, "out of memory allocating cursor");
    return true;
  }
  if (walk_chain(abi, handle, scan_key[0], c, error_msg, error_msg_len)) {
    delete c;
    if (local_handle != nullptr) {
      abi->close(local_handle);
      abi->drop(local_table, error_msg, error_msg_len);
    }
    return true;
  }

  if (local_handle != nullptr) {
    abi->close(local_handle);
    abi->drop(local_table, error_msg, error_msg_len);
  }

  std::stable_sort(c->hits.begin(), c->hits.end(),
                   [](const VecChainHit &a, const VecChainHit &b) {
                     return a.distance < b.distance;
                   });
  if (scan_desc.limit() > 0 && c->hits.size() > scan_desc.limit()) {
    c->hits.resize(scan_desc.limit());
  }

  *eof = c->hits.empty();
  *cursor = reinterpret_cast<Index::Cursor>(c);
  return false;
}

bool vec_chain_index_scan_position(Index::Cursor cursor, Index::CursorOp op,
                                   bool *eof, char * /*error_msg*/,
                                   uint32_t /*error_msg_len*/) {
  auto *c = reinterpret_cast<VecChainCursor *>(cursor);
  if (op == Index::CursorOp::Prev) {
    *eof = true;
    return false;
  }
  if (c->pos < c->hits.size()) c->pos++;
  *eof = c->pos >= c->hits.size();
  return false;
}

bool vec_chain_index_scan_fetch(Index::Cursor cursor,
                                IndexScanKey::KeyPartRef * /*key_ref*/,
                                IndexScanKey::KeyPartData *key_columns,
                                IndexScanKey::KeyPartData *pkey_columns,
                                char *error_msg, uint32_t error_msg_len) {
  auto *c = reinterpret_cast<VecChainCursor *>(cursor);
  if (c->pos >= c->hits.size()) {
    snprintf(error_msg, error_msg_len, "vec_chain cursor is at EOF");
    return true;
  }
  const VecChainHit &hit = c->hits[c->pos];
  key_columns[0] = IndexScanKey::KeyPartData{
      .data = hit.key_bytes.data(),
      .length = static_cast<uint32_t>(hit.key_bytes.size())};
  pkey_columns[0] = IndexScanKey::KeyPartData{
      .data = hit.pkey_bytes.data(),
      .length = static_cast<uint32_t>(hit.pkey_bytes.size())};
  return false;
}

bool vec_chain_index_scan_save(Index::Cursor /*cursor*/, char * /*error_msg*/,
                               uint32_t /*error_msg_len*/) {
  return false;
}

bool vec_chain_index_scan_restore(Index::Cursor cursor, MtrCtx::Ref /*mctx*/,
                                  bool *eof, char * /*error_msg*/,
                                  uint32_t /*error_msg_len*/) {
  auto *c = reinterpret_cast<VecChainCursor *>(cursor);
  *eof = c == nullptr || c->pos >= c->hits.size();
  return false;
}

void vec_chain_index_scan_end(Index::Cursor *cursor) {
  if (cursor == nullptr || *cursor == nullptr) return;
  delete reinterpret_cast<VecChainCursor *>(*cursor);
  *cursor = nullptr;
}

// Attach a fresh inspector handle to the hidden table for vec_chain.
// Mirrors the bloom_hidden inspector pattern: build a vef_table_storage_def_t
// that describes the same schema as the index registration, call the
// ABI's create() (which now just allocates the descriptor), then open
// a read handle. Caller owns both and must release via abi->close +
// abi->drop.
bool attach_inspector_handle(const vef_preview_table_storage_t **abi_out,
                             vef_table_storage_t **table_out,
                             vef_table_storage_handle_t **handle_out,
                             char *error_msg, uint32_t error_msg_len) {
  *abi_out = nullptr;
  *table_out = nullptr;
  *handle_out = nullptr;
  const vef_preview_table_storage_t *abi = g_hidden_table.abi();
  if (abi == nullptr) {
    snprintf(error_msg, error_msg_len,
             "table_storage capability is unavailable");
    return true;
  }
  vef_table_storage_def_t def{};
  if (vec_chain_index_table_storage_def(/*index_ctx=*/nullptr, &def, error_msg,
                                        error_msg_len)) {
    return true;
  }
  vef_table_storage_t *table = nullptr;
  if (abi->create(&def, &table, error_msg, error_msg_len)) {
    return true;
  }
  vef_table_storage_handle_t *handle = nullptr;
  if (abi->open(table, VEF_TABLE_STORAGE_LOCK_READ, &handle, error_msg,
                error_msg_len)) {
    abi->drop(table, error_msg, error_msg_len);
    return true;
  }
  *abi_out = abi;
  *table_out = table;
  *handle_out = handle;
  return false;
}

// Decode "[x,y,z]" into a float vector of the given dimension. Returns
// true on parse error.
bool encode_query_vector(int64_t dimension, std::string_view text,
                         std::vector<unsigned char> *out, char *error_msg,
                         uint32_t error_msg_len) {
  std::string input(text);
  const char *s = input.c_str();
  while (*s == ' ') s++;
  if (*s != '[') {
    snprintf(error_msg, error_msg_len, "expected '[' in query vector");
    return true;
  }
  s++;

  out->assign(static_cast<size_t>(dimension) * sizeof(float), 0);
  int64_t count = 0;
  while (*s != '\0') {
    while (*s == ' ') s++;
    if (*s == ']') break;
    if (count >= dimension) {
      snprintf(error_msg, error_msg_len,
               "query vector has more than %" PRId64 " elements", dimension);
      return true;
    }
    char *endptr = nullptr;
    float val = strtof(s, &endptr);
    if (endptr == s) {
      snprintf(error_msg, error_msg_len, "parse error in query vector");
      return true;
    }
    unsigned char buf[4];
    store_float(buf, val);
    memcpy(out->data() + count * sizeof(float), buf, sizeof(buf));
    count++;
    s = endptr;
    while (*s == ' ') s++;
    if (*s == ',') s++;
  }
  if (*s != ']') {
    snprintf(error_msg, error_msg_len, "expected ']' in query vector");
    return true;
  }
  if (count != dimension) {
    snprintf(error_msg, error_msg_len,
             "query vector dimension mismatch (got %" PRId64
             ", expected %" PRId64 ")",
             count, dimension);
    return true;
  }
  return false;
}

}  // namespace

uint64_t vec_chain_scan_count() { return g_vec_chain_scan_count.load(); }

bool vec_chain_inspect_knn(int64_t dimension, std::string_view query_text,
                           uint32_t limit, std::string *result, char *error_msg,
                           uint32_t error_msg_len) {
  std::vector<unsigned char> query_bytes;
  if (encode_query_vector(dimension, query_text, &query_bytes, error_msg,
                          error_msg_len)) {
    return true;
  }

  const vef_preview_table_storage_t *abi = nullptr;
  vef_table_storage_t *table = nullptr;
  vef_table_storage_handle_t *handle = nullptr;
  if (attach_inspector_handle(&abi, &table, &handle, error_msg,
                              error_msg_len)) {
    return true;
  }

  vef_storage_col_data_t query{
      .data = query_bytes.data(),
      .length = static_cast<uint32_t>(query_bytes.size()),
  };
  VecChainCursor cursor;
  bool walk_failed =
      walk_chain(abi, handle, query, &cursor, error_msg, error_msg_len);

  abi->close(handle);
  abi->drop(table, error_msg, error_msg_len);

  if (walk_failed) return true;

  // Stable sort so ties resolve in chain-traversal order (newest insert
  // first), which is what the tests rely on for deterministic output.
  std::stable_sort(cursor.hits.begin(), cursor.hits.end(),
                   [](const VecChainHit &a, const VecChainHit &b) {
                     return a.distance < b.distance;
                   });
  if (limit > 0 && cursor.hits.size() > limit) {
    cursor.hits.resize(limit);
  }

  // Decode each hit's PK bytes as int32 (the test uses INT PRIMARY KEY).
  std::string out;
  for (size_t i = 0; i < cursor.hits.size(); i++) {
    if (i > 0) out += ",";
    const auto &pk = cursor.hits[i].pkey_bytes;
    int32_t id = 0;
    if (pk.size() == sizeof(int32_t)) {
      memcpy(&id, pk.data(), sizeof(int32_t));
    }
    char buf[16];
    snprintf(buf, sizeof(buf), "%d", id);
    out += buf;
  }
  *result = std::move(out);
  return false;
}

namespace {

[[maybe_unused]] constexpr auto VEC_CHAIN_INDEX =
    vsql::preview_index_builder::make_index_type<kVecChainIndexTypeName,
                                                 VecChainIndexStorage>()
        .lifecycle()
        .create<&vec_chain_index_create>()
        .load<&vec_chain_index_load>()
        .drop<&vec_chain_index_drop>()
        .dml()
        .insert<&vec_chain_index_insert>()
        .mark_delete<&vec_chain_index_mark_delete>()
        .purge<&vec_chain_index_purge>()
        .scan()
        .begin<&vec_chain_index_scan_begin>()
        .position<&vec_chain_index_scan_position>()
        .fetch<&vec_chain_index_scan_fetch>()
        .save<&vec_chain_index_scan_save>()
        .restore<&vec_chain_index_scan_restore>()
        .end<&vec_chain_index_scan_end>()
        .global()
        .capabilities(Index::Support::KNN)
        .storage_props(Index::Storage::HAS_ROW_REF | Index::Storage::REF_LOOKUP)
        .table_storage<&vec_chain_index_table_storage_def>()
        .build();

}  // namespace

vsql::preview_index_builder::IndexTypeCapability<1> VEC_CHAIN_INDEXES =
    vsql::preview_index_builder::IndexTypeCapability<>().index_type(
        VEC_CHAIN_INDEX);

namespace {

constexpr const char kVecChainProfileL2[] = "vec_chain_l2";

const auto VEC_CHAIN_L2_FN =
    vsql::preview_index_builder::make_index_function<&vec_chain_l2_distance>(
        "vec_chain_l2_distance")
        .returns(vsql::REAL)
        .param(kVecChainTypeName)
        .param(kVecChainTypeName)
        .deterministic()
        .build();

const auto VEC_CHAIN_L2_PROFILE =
    vsql::preview_index_builder::make_index_profile(kVecChainProfileL2)
        .for_type(kVecChainTypeName)
        .using_index(kVecChainIndexTypeName)
        .with_function(1, VEC_CHAIN_L2_FN)
        .ordering(Index::Ordering::ASC)
        .default_for_type(true)
        .build();

}  // namespace

vsql::preview_index_builder::IndexProfileCapability<1> VEC_CHAIN_PROFILES =
    vsql::preview_index_builder::IndexProfileCapability<>().index_profile(
        VEC_CHAIN_L2_PROFILE);
