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

#include <villagesql/preview/index_builder.h>
#include <villagesql/preview/table_storage.h>
#include <villagesql/vsql.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

struct CacheEntry {
  unsigned long long row_id = 0;
  std::string payload;
};

struct BloomIndexStorage {};

struct EmptyCursor {};

using vsql::preview_index_builder::Index;
using vsql::preview_index_builder::IndexScanDesc;
using vsql::preview_index_builder::IndexScanKey;
using vsql::preview_storage::MtrCtx;
using vsql::preview_storage::Segment;
using vsql::preview_storage::Space;

using BloomIndexCtx = Index::StorageCtx<BloomIndexStorage>;

static vsql::preview_table_storage::TableStorageCapability g_hidden_table;
static vef_table_storage_t *g_table = nullptr;
static std::mutex g_cache_mu;
static std::unordered_map<unsigned long long, std::vector<CacheEntry>> g_cache;

static constexpr const char kBloomIndexTypeName[] = "bloom";

static void set_uint64_value(unsigned long long value, char *buf,
                             size_t buf_len, vef_table_storage_value_t *out) {
  snprintf(buf, buf_len, "%llu", value);
  out->data = reinterpret_cast<const unsigned char *>(buf);
  out->length = static_cast<uint32_t>(strlen(buf));
  out->is_null = false;
}

static bool parse_uint64(const vef_table_storage_value_t &value,
                         unsigned long long *out) {
  if (value.is_null || value.data == nullptr || value.length == 0) return true;

  // The table_storage ABI emits integer column values as decimal ASCII
  // (see copy_field_value in services/preview/table_storage.cc). Try
  // that first; fall back to fixed-width little-endian only if the
  // bytes don't parse as a valid decimal string, for back-compat with
  // any extension that supplies raw integer bytes via insert.
  std::string text(reinterpret_cast<const char *>(value.data), value.length);
  char *end = nullptr;
  const unsigned long long parsed = strtoull(text.c_str(), &end, 10);
  if (end != text.c_str() && *end == '\0') {
    *out = parsed;
    return false;
  }

  if (value.length == 8) {
    uint64_t bin = 0;
    memcpy(&bin, value.data, sizeof(bin));
    *out = static_cast<unsigned long long>(bin);
    return false;
  }
  if (value.length == 4) {
    uint32_t bin = 0;
    memcpy(&bin, value.data, sizeof(bin));
    *out = static_cast<unsigned long long>(bin);
    return false;
  }
  if (value.length == 2) {
    uint16_t bin = 0;
    memcpy(&bin, value.data, sizeof(bin));
    *out = static_cast<unsigned long long>(bin);
    return false;
  }
  if (value.length == 1) {
    *out = static_cast<unsigned long long>(value.data[0]);
    return false;
  }
  return true;
}

static bool parse_storage_uint64(const vef_storage_col_data_t &value,
                                 unsigned long long *out) {
  if (value.data == nullptr || value.length == 0) return true;
  vef_table_storage_value_t hidden_value{
      .data = value.data,
      .length = value.length,
      .is_null = false,
  };
  return parse_uint64(hidden_value, out);
}

static bool attach_hidden_table(vef_table_storage_t **table, char *error_msg,
                                uint32_t error_msg_len) {
  if (*table != nullptr) return false;
  const vef_preview_table_storage_t *abi = g_hidden_table.abi();
  if (abi == nullptr || abi->version != VEF_PREVIEW_TABLE_STORAGE_ABI_VERSION) {
    snprintf(error_msg, error_msg_len,
             "table_storage capability is unavailable");
    return true;
  }

  vef_table_storage_col_def_t columns[] = {
      {.name = "bucket",
       .type = VEF_TABLE_STORAGE_COL_UINT64,
       .max_length = 8,
       .nullable = false},
      {.name = "row_id",
       .type = VEF_TABLE_STORAGE_COL_UINT64,
       .max_length = 8,
       .nullable = false},
      {.name = "payload",
       .type = VEF_TABLE_STORAGE_COL_BYTES,
       .max_length = 255,
       .nullable = false},
  };
  uint32_t primary_key[] = {0, 1};
  vef_table_storage_def_t def{
      .version = 1,
      .logical_name = "bloom_test",
      .columns = columns,
      .column_count = 3,
      .primary_key_columns = primary_key,
      .primary_key_column_count = 2,
  };
  return abi->create(&def, table, error_msg, error_msg_len);
}

static bool insert_hidden_row(vef_table_storage_t *table,
                              unsigned long long bucket,
                              unsigned long long row_id,
                              std::string_view payload, char *error_msg,
                              uint32_t error_msg_len) {
  const vef_preview_table_storage_t *abi = g_hidden_table.abi();
  if (abi == nullptr) {
    snprintf(error_msg, error_msg_len,
             "table_storage capability is unavailable");
    return true;
  }
  char bucket_buf[32]{};
  char row_id_buf[32]{};
  vef_table_storage_value_t bucket_value{};
  vef_table_storage_value_t row_id_value{};
  set_uint64_value(bucket, bucket_buf, sizeof(bucket_buf), &bucket_value);
  set_uint64_value(row_id, row_id_buf, sizeof(row_id_buf), &row_id_value);
  vef_table_storage_value_t values[] = {
      bucket_value,
      row_id_value,
      {.data = reinterpret_cast<const unsigned char *>(payload.data()),
       .length = static_cast<uint32_t>(payload.size()),
       .is_null = false},
  };
  vef_table_storage_handle_t *handle = nullptr;
  if (abi->open(table, VEF_TABLE_STORAGE_LOCK_WRITE, &handle, error_msg,
                error_msg_len) ||
      abi->insert(handle, values, 3, error_msg, error_msg_len)) {
    if (handle != nullptr) abi->close(handle);
    return true;
  }
  abi->close(handle);
  return false;
}

static bool delete_hidden_row(vef_table_storage_t *table,
                              unsigned long long bucket,
                              unsigned long long row_id, char *error_msg,
                              uint32_t error_msg_len) {
  const vef_preview_table_storage_t *abi = g_hidden_table.abi();
  if (abi == nullptr) {
    snprintf(error_msg, error_msg_len,
             "table_storage capability is unavailable");
    return true;
  }
  char bucket_buf[32]{};
  char row_id_buf[32]{};
  vef_table_storage_value_t key_values[2]{};
  set_uint64_value(bucket, bucket_buf, sizeof(bucket_buf), &key_values[0]);
  set_uint64_value(row_id, row_id_buf, sizeof(row_id_buf), &key_values[1]);
  vef_table_storage_handle_t *handle = nullptr;
  if (abi->open(table, VEF_TABLE_STORAGE_LOCK_WRITE, &handle, error_msg,
                error_msg_len) ||
      abi->delete_row(handle, key_values, 2, error_msg, error_msg_len)) {
    if (handle != nullptr) abi->close(handle);
    return true;
  }
  abi->close(handle);
  return false;
}

static const vef_preview_table_storage_t *checked_abi(vsql::IntResult &out) {
  const vef_preview_table_storage_t *abi = g_hidden_table.abi();
  if (abi == nullptr || abi->version != VEF_PREVIEW_TABLE_STORAGE_ABI_VERSION) {
    out.error("table_storage capability is unavailable");
    return nullptr;
  }
  return abi;
}

static const vef_preview_table_storage_t *checked_abi(vsql::StringResult &out) {
  const vef_preview_table_storage_t *abi = g_hidden_table.abi();
  if (abi == nullptr || abi->version != VEF_PREVIEW_TABLE_STORAGE_ABI_VERSION) {
    out.error("table_storage capability is unavailable");
    return nullptr;
  }
  return abi;
}

static bool ensure_table(vsql::IntResult &out) {
  if (g_table != nullptr) return true;
  out.error("bloom hidden table has not been created");
  return false;
}

static bool ensure_table(vsql::StringResult &out) {
  if (g_table != nullptr) return true;
  out.error("bloom hidden table has not been created");
  return false;
}

void bloom_create(vsql::IntResult out) {
  if (g_table != nullptr) {
    out.set(1);
    return;
  }

  char err[512]{};
  if (attach_hidden_table(&g_table, err, sizeof(err))) {
    out.error(err[0] == '\0' ? "bloom.create failed" : err);
    return;
  }
  out.set(1);
}

void bloom_insert(vsql::IntArg bucket, vsql::IntArg row_id,
                  vsql::StringArg payload, vsql::IntResult out) {
  if (bucket.is_null() || row_id.is_null() || payload.is_null()) {
    out.set_null();
    return;
  }
  if (!ensure_table(out)) return;

  auto payload_value = payload.value();

  char err[512]{};
  if (insert_hidden_row(g_table,
                        static_cast<unsigned long long>(bucket.value()),
                        static_cast<unsigned long long>(row_id.value()),
                        payload_value, err, sizeof(err))) {
    out.error(err[0] == '\0' ? "bloom.insert failed" : err);
    return;
  }
  out.set(1);
}

void bloom_delete(vsql::IntArg bucket, vsql::IntArg row_id,
                  vsql::IntResult out) {
  if (bucket.is_null() || row_id.is_null()) {
    out.set_null();
    return;
  }
  if (!ensure_table(out)) return;

  char err[512]{};
  if (delete_hidden_row(
          g_table, static_cast<unsigned long long>(bucket.value()),
          static_cast<unsigned long long>(row_id.value()), err, sizeof(err))) {
    out.error(err[0] == '\0' ? "bloom.delete failed" : err);
    return;
  }
  out.set(1);
}

void bloom_load_cache(vsql::IntResult out) {
  if (!ensure_table(out)) return;

  char err[512]{};
  const vef_preview_table_storage_t *abi = checked_abi(out);
  if (abi == nullptr) return;
  vef_table_storage_handle_t *handle = nullptr;
  vef_table_storage_cursor_t *cursor = nullptr;
  bool eof = true;
  vef_table_storage_scan_t scan{
      .version = 1,
      .scan_type = VEF_TABLE_STORAGE_SCAN_FULL,
      .key_values = nullptr,
      .key_value_count = 0,
      .limit = 0,
  };
  if (abi->open(g_table, VEF_TABLE_STORAGE_LOCK_READ, &handle, err,
                sizeof(err)) ||
      abi->scan_begin(handle, &scan, &cursor, &eof, err, sizeof(err))) {
    if (cursor != nullptr) abi->scan_end(cursor);
    if (handle != nullptr) abi->close(handle);
    out.error(err[0] == '\0' ? "bloom.load_cache failed" : err);
    return;
  }

  std::unordered_map<unsigned long long, std::vector<CacheEntry>> next_cache;
  long long count = 0;
  while (!eof) {
    vef_table_storage_value_t values[3]{};
    unsigned long long bucket = 0;
    unsigned long long row_id = 0;
    if (abi->scan_fetch(cursor, values, 3, err, sizeof(err)) ||
        parse_uint64(values[0], &bucket) || parse_uint64(values[1], &row_id)) {
      abi->scan_end(cursor);
      abi->close(handle);
      out.error(err[0] == '\0' ? "bloom.fetch failed" : err);
      return;
    }
    CacheEntry entry;
    entry.row_id = row_id;
    if (!values[2].is_null && values[2].data != nullptr) {
      entry.payload.assign(reinterpret_cast<const char *>(values[2].data),
                           values[2].length);
    }
    next_cache[bucket].push_back(std::move(entry));
    count++;
    if (abi->scan_next(cursor, &eof, err, sizeof(err))) {
      abi->scan_end(cursor);
      abi->close(handle);
      out.error(err[0] == '\0' ? "bloom.scan_next failed" : err);
      return;
    }
  }
  abi->scan_end(cursor);
  abi->close(handle);

  for (auto &kv : next_cache) {
    std::sort(kv.second.begin(), kv.second.end(),
              [](const CacheEntry &a, const CacheEntry &b) {
                return a.row_id < b.row_id;
              });
  }
  {
    std::lock_guard<std::mutex> guard(g_cache_mu);
    g_cache = std::move(next_cache);
  }
  out.set(count);
}

void bloom_cache_count(vsql::IntResult out) {
  long long count = 0;
  std::lock_guard<std::mutex> guard(g_cache_mu);
  for (const auto &kv : g_cache) count += kv.second.size();
  out.set(count);
}

void bloom_might_contain(vsql::IntArg bucket,
                         vsql::StringArg payload [[maybe_unused]],
                         vsql::IntResult out) {
  if (bucket.is_null() || payload.is_null()) {
    out.set_null();
    return;
  }
  std::lock_guard<std::mutex> guard(g_cache_mu);
  out.set(g_cache.find(static_cast<unsigned long long>(bucket.value())) !=
                  g_cache.end()
              ? 1
              : 0);
}

void bloom_search(vsql::IntArg bucket, vsql::StringResult out) {
  if (bucket.is_null()) {
    out.set_null();
    return;
  }
  std::vector<unsigned long long> ids;
  {
    std::lock_guard<std::mutex> guard(g_cache_mu);
    auto it = g_cache.find(static_cast<unsigned long long>(bucket.value()));
    if (it != g_cache.end()) {
      for (const auto &entry : it->second) ids.push_back(entry.row_id);
    }
  }

  std::sort(ids.begin(), ids.end());
  std::string result;
  for (size_t i = 0; i < ids.size(); i++) {
    if (i > 0) result.push_back(',');
    result += std::to_string(ids[i]);
  }
  out.set(result);
}

void bloom_drop(vsql::IntResult out) {
  {
    std::lock_guard<std::mutex> guard(g_cache_mu);
    g_cache.clear();
  }
  if (g_table == nullptr) {
    out.set(0);
    return;
  }
  char err[512]{};
  const vef_preview_table_storage_t *abi = checked_abi(out);
  if (abi == nullptr) return;
  vef_table_storage_t *table = g_table;
  g_table = nullptr;
  if (abi->drop(table, err, sizeof(err))) {
    out.error(err[0] == '\0' ? "bloom.drop failed" : err);
    return;
  }
  out.set(1);
}

// Per-index columns and PK indices used by both the index DML callbacks and
// the table_storage_def callback. Pointed at by static storage to keep the
// def's lifetime trivial.
static const vef_table_storage_col_def_t kBloomIndexColumns[] = {
    {.name = "bucket",
     .type = VEF_TABLE_STORAGE_COL_UINT64,
     .max_length = 8,
     .nullable = false},
    {.name = "row_id",
     .type = VEF_TABLE_STORAGE_COL_UINT64,
     .max_length = 8,
     .nullable = false},
    {.name = "payload",
     .type = VEF_TABLE_STORAGE_COL_BYTES,
     .max_length = 255,
     .nullable = false},
};
static const uint32_t kBloomIndexPrimaryKey[] = {0, 1};

bool bloom_index_table_storage_def(const vef_index_ctx_t * /*index_ctx*/,
                                   vef_table_storage_def_t *def_out,
                                   char * /*error_msg*/,
                                   uint32_t /*error_msg_len*/) {
  def_out->version = 1;
  def_out->logical_name = "bloom_test";
  def_out->columns = kBloomIndexColumns;
  def_out->column_count =
      sizeof(kBloomIndexColumns) / sizeof(kBloomIndexColumns[0]);
  def_out->primary_key_columns = kBloomIndexPrimaryKey;
  def_out->primary_key_column_count =
      sizeof(kBloomIndexPrimaryKey) / sizeof(kBloomIndexPrimaryKey[0]);
  return false;
}

bool bloom_index_create(BloomIndexCtx * /*ctx*/, const Index & /*index*/,
                        Space::Ref /*space_ref*/, Segment::TrxRef /*trx_ref*/,
                        char * /*error_msg*/, uint32_t /*error_msg_len*/) {
  // Server owns the backing hidden table — nothing to do here.
  return false;
}

bool bloom_index_load(BloomIndexCtx * /*ctx*/, const Index & /*index*/,
                      Index::StorageRef /*storage_ref*/, char * /*error_msg*/,
                      uint32_t /*error_msg_len*/) {
  return false;
}

bool bloom_index_drop(BloomIndexCtx * /*ctx*/, const Index & /*index*/,
                      Segment::TrxRef /*trx_ref*/, char * /*error_msg*/,
                      uint32_t /*error_msg_len*/) {
  return false;
}

bool bloom_index_insert(BloomIndexCtx * /*ctx*/, const Index &index,
                        Segment::TrxRef /*trx_ref*/,
                        IndexScanKey::KeyPartData *key_columns,
                        IndexScanKey::KeyPartData *pkey_columns,
                        IndexScanKey::KeyPartRef *key_ref, char *error_msg,
                        uint32_t error_msg_len) {
  if (index.get_num_key_cols() != 1 || index.get_primary_num_key_cols() != 1 ||
      key_columns == nullptr || pkey_columns == nullptr) {
    snprintf(error_msg, error_msg_len,
             "bloom maintenance requires one key and one primary key");
    return true;
  }
  if (index.get_table_storage_handle() == nullptr) {
    snprintf(
        error_msg, error_msg_len,
        "bloom maintenance requires a server-provided hidden table handle");
    return true;
  }

  unsigned long long bucket = 0;
  unsigned long long row_id = 0;
  if (parse_storage_uint64(key_columns[0], &bucket) ||
      parse_storage_uint64(pkey_columns[0], &row_id)) {
    snprintf(error_msg, error_msg_len,
             "bloom maintenance only supports integer columns");
    return true;
  }
  if (key_ref != nullptr) {
    *key_ref = static_cast<IndexScanKey::KeyPartRef>(row_id);
  }

  const vef_preview_table_storage_t *abi = g_hidden_table.abi();
  if (abi == nullptr) {
    snprintf(error_msg, error_msg_len,
             "table_storage capability is unavailable");
    return true;
  }
  char bucket_buf[32]{};
  char row_id_buf[32]{};
  vef_table_storage_value_t bucket_value{};
  vef_table_storage_value_t row_id_value{};
  set_uint64_value(bucket, bucket_buf, sizeof(bucket_buf), &bucket_value);
  set_uint64_value(row_id, row_id_buf, sizeof(row_id_buf), &row_id_value);
  const std::string_view payload("maintained");
  vef_table_storage_value_t values[] = {
      bucket_value,
      row_id_value,
      {.data = reinterpret_cast<const unsigned char *>(payload.data()),
       .length = static_cast<uint32_t>(payload.size()),
       .is_null = false},
  };
  return abi->insert(index.get_table_storage_handle(), values, 3, error_msg,
                     error_msg_len);
}

bool bloom_index_mark_delete(BloomIndexCtx * /*ctx*/, const Index &index,
                             Segment::TrxRef /*trx_ref*/,
                             IndexScanKey::KeyPartRef * /*key_ref*/,
                             IndexScanKey::KeyPartData *key_columns,
                             IndexScanKey::KeyPartData *pkey_columns,
                             bool delete_mark, char *error_msg,
                             uint32_t error_msg_len) {
  if (!delete_mark) return false;
  if (index.get_num_key_cols() != 1 || index.get_primary_num_key_cols() != 1 ||
      key_columns == nullptr || pkey_columns == nullptr) {
    snprintf(error_msg, error_msg_len,
             "bloom maintenance requires one key and one primary key");
    return true;
  }
  if (index.get_table_storage_handle() == nullptr) {
    snprintf(
        error_msg, error_msg_len,
        "bloom maintenance requires a server-provided hidden table handle");
    return true;
  }

  unsigned long long bucket = 0;
  unsigned long long row_id = 0;
  if (parse_storage_uint64(key_columns[0], &bucket) ||
      parse_storage_uint64(pkey_columns[0], &row_id)) {
    snprintf(error_msg, error_msg_len,
             "bloom maintenance only supports integer columns");
    return true;
  }

  const vef_preview_table_storage_t *abi = g_hidden_table.abi();
  if (abi == nullptr) {
    snprintf(error_msg, error_msg_len,
             "table_storage capability is unavailable");
    return true;
  }
  char bucket_buf[32]{};
  char row_id_buf[32]{};
  vef_table_storage_value_t key_values[2]{};
  set_uint64_value(bucket, bucket_buf, sizeof(bucket_buf), &key_values[0]);
  set_uint64_value(row_id, row_id_buf, sizeof(row_id_buf), &key_values[1]);
  return abi->delete_row(index.get_table_storage_handle(), key_values, 2,
                         error_msg, error_msg_len);
}

bool bloom_index_purge(BloomIndexCtx * /*ctx*/, const Index & /*index*/,
                       Segment::TrxRef /*trx_ref*/,
                       IndexScanKey::KeyPartRef * /*key_ref*/,
                       IndexScanKey::KeyPartData * /*key_columns*/,
                       IndexScanKey::KeyPartData * /*pkey_columns*/,
                       char * /*error_msg*/, uint32_t /*error_msg_len*/) {
  return false;
}

bool bloom_index_scan_begin(BloomIndexCtx * /*ctx*/, const Index & /*index*/,
                            MtrCtx::Ref /*mctx*/,
                            const IndexScanDesc & /*scan_desc*/,
                            Index::Cursor *cursor, bool *eof,
                            char * /*error_msg*/, uint32_t /*error_msg_len*/) {
  *cursor = reinterpret_cast<Index::Cursor>(new EmptyCursor());
  *eof = true;
  return false;
}

bool bloom_index_scan_position(Index::Cursor /*cursor*/, Index::CursorOp /*op*/,
                               bool *eof, char * /*error_msg*/,
                               uint32_t /*error_msg_len*/) {
  *eof = true;
  return false;
}

bool bloom_index_scan_fetch(Index::Cursor /*cursor*/,
                            IndexScanKey::KeyPartRef * /*key_ref*/,
                            IndexScanKey::KeyPartData * /*key_columns*/,
                            IndexScanKey::KeyPartData * /*pkey_columns*/,
                            char *error_msg, uint32_t error_msg_len) {
  snprintf(error_msg, error_msg_len,
           "bloom scan is not implemented in the maintenance POC");
  return true;
}

bool bloom_index_scan_save(Index::Cursor /*cursor*/, char * /*error_msg*/,
                           uint32_t /*error_msg_len*/) {
  return false;
}

bool bloom_index_scan_restore(Index::Cursor /*cursor*/, MtrCtx::Ref /*mctx*/,
                              bool *eof, char * /*error_msg*/,
                              uint32_t /*error_msg_len*/) {
  *eof = true;
  return false;
}

void bloom_index_scan_end(Index::Cursor *cursor) {
  if (cursor == nullptr || *cursor == nullptr) return;
  delete reinterpret_cast<EmptyCursor *>(*cursor);
  *cursor = nullptr;
}

[[maybe_unused]] constexpr auto BLOOM_INDEX =
    vsql::preview_index_builder::make_index_type<kBloomIndexTypeName,
                                                 BloomIndexStorage>()
        .lifecycle()
        .create<&bloom_index_create>()
        .load<&bloom_index_load>()
        .drop<&bloom_index_drop>()
        .dml()
        .insert<&bloom_index_insert>()
        .mark_delete<&bloom_index_mark_delete>()
        .purge<&bloom_index_purge>()
        .scan()
        .begin<&bloom_index_scan_begin>()
        .position<&bloom_index_scan_position>()
        .fetch<&bloom_index_scan_fetch>()
        .save<&bloom_index_scan_save>()
        .restore<&bloom_index_scan_restore>()
        .end<&bloom_index_scan_end>()
        .global()
        .capabilities(Index::Support::POINT_LOOKUP)
        .storage_props(Index::Storage::HAS_ROW_REF | Index::Storage::REF_LOOKUP)
        .table_storage<&bloom_index_table_storage_def>()
        .build();

static auto BLOOM_INDEXES =
    vsql::preview_index_builder::IndexTypeCapability().index_type(BLOOM_INDEX);

using namespace vsql;

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .with(g_hidden_table)
        .with(BLOOM_INDEXES)
        .func(make_func<&bloom_create>("bloom_create")
                  .returns(INT)
                  .no_params()
                  .build())
        .func(make_func<&bloom_insert>("bloom_insert")
                  .returns(INT)
                  .param(INT)
                  .param(INT)
                  .param(STRING)
                  .build())
        .func(make_func<&bloom_delete>("bloom_delete")
                  .returns(INT)
                  .param(INT)
                  .param(INT)
                  .build())
        .func(make_func<&bloom_load_cache>("bloom_load_cache")
                  .returns(INT)
                  .no_params()
                  .build())
        .func(make_func<&bloom_cache_count>("bloom_cache_count")
                  .returns(INT)
                  .no_params()
                  .build())
        .func(make_func<&bloom_might_contain>("bloom_might_contain")
                  .returns(INT)
                  .param(INT)
                  .param(STRING)
                  .build())
        .func(make_func<&bloom_search>("bloom_search")
                  .returns(STRING)
                  .param(INT)
                  .build())
        .func(make_func<&bloom_drop>("bloom_drop")
                  .returns(INT)
                  .no_params()
                  .build()))
