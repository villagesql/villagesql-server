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

// BLOOM is a simple in-memory Bloom signature index test extension. It mirrors
// the mvector test-extension shape: a preview custom index type plus UDFs that
// exercise the same data structure manually before CREATE INDEX integration is
// wired through SQL.

#include <villagesql/preview/index_builder.h>
#include <villagesql/vsql.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <new>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

constexpr uint32_t kBloomDefaultBits = 1024;
constexpr uint32_t kBloomDefaultHashes = 2;
constexpr uint32_t kBloomMaxColumns = 32;

struct BloomOptions {
  uint32_t signature_bits = kBloomDefaultBits;
  uint32_t default_hashes = kBloomDefaultHashes;
  uint32_t column_hashes[kBloomMaxColumns] = {};
};

struct BloomSignature {
  std::vector<uint64_t> words;
};

struct BloomManualEntry {
  long long pk = 0;
  std::string key;
  BloomSignature signature;
};

struct BloomManualIndex {
  BloomOptions options;
  std::unordered_map<long long, BloomManualEntry> entries;
};

struct BloomPKeyPart {
  std::vector<unsigned char> data;
};

struct BloomStorageEntry {
  uint64_t ref = 0;
  std::vector<std::vector<unsigned char>> key_columns;
  std::vector<BloomPKeyPart> pkey;
  BloomSignature signature;
  bool delete_marked = false;
};

struct BloomStorage {
  uint64_t next_ref = 1;
  BloomOptions options;
  std::vector<BloomStorageEntry> entries;
};

struct BloomCursor {
  BloomStorage *storage = nullptr;
  std::vector<size_t> hits;
  size_t pos = 0;
};

using vsql::preview_index_builder::Index;
using vsql::preview_index_builder::IndexScanDesc;
using vsql::preview_index_builder::IndexScanKey;
using vsql::preview_storage::MtrCtx;
using vsql::preview_storage::Segment;
using vsql::preview_storage::Space;

using BloomCtx = Index::StorageCtx<BloomStorage>;

std::mutex g_bloom_indexes_mu;
std::unordered_map<std::string, BloomManualIndex> g_bloom_indexes;

void bloom_assign_bytes(std::vector<unsigned char> &dst,
                        const IndexScanKey::KeyPartData &src) {
  if (src.length == 0) {
    dst.clear();
    return;
  }
  dst.assign(src.data, src.data + src.length);
}

uint64_t bloom_hash_bytes(const unsigned char *data, size_t length,
                          uint64_t seed) {
  uint64_t hash = 1469598103934665603ULL ^ seed;
  for (size_t i = 0; i < length; i++) {
    hash ^= data[i];
    hash *= 1099511628211ULL;
  }
  hash ^= length + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
  return hash;
}

uint32_t bloom_word_count(uint32_t signature_bits) {
  return (signature_bits + 63) / 64;
}

uint32_t bloom_hash_count_for_column(const BloomOptions &options,
                                     uint32_t col) {
  if (col < kBloomMaxColumns && options.column_hashes[col] != 0) {
    return options.column_hashes[col];
  }
  return options.default_hashes;
}

void bloom_set_bit(BloomSignature &sig, uint32_t bit) {
  sig.words[bit / 64] |= (uint64_t{1} << (bit % 64));
}

bool bloom_contains_signature(const BloomSignature &candidate,
                              const BloomSignature &query) {
  if (candidate.words.size() != query.words.size()) return false;
  for (size_t i = 0; i < query.words.size(); i++) {
    if ((candidate.words[i] & query.words[i]) != query.words[i]) return false;
  }
  return true;
}

bool bloom_key_matches_text(const std::string &stored, std::string_view query) {
  if (stored == query) return true;
  if (query.empty()) return false;
  return stored.find(query) != std::string::npos;
}

BloomSignature bloom_signature_for_columns(
    const BloomOptions &options, const std::vector<std::string_view> &columns) {
  BloomSignature sig;
  sig.words.assign(bloom_word_count(options.signature_bits), 0);
  if (options.signature_bits == 0) return sig;

  for (size_t col = 0; col < columns.size(); col++) {
    const auto &value = columns[col];
    const auto *data = reinterpret_cast<const unsigned char *>(value.data());
    const uint32_t hashes =
        bloom_hash_count_for_column(options, static_cast<uint32_t>(col));
    const uint64_t h1 = bloom_hash_bytes(data, value.size(), col + 1);
    uint64_t h2 =
        bloom_hash_bytes(data, value.size(), 0x9e3779b97f4a7c15ULL + col);
    if (h2 == 0) h2 = 0x27d4eb2d;
    for (uint32_t i = 0; i < hashes; i++) {
      const uint64_t combined = h1 + i * h2;
      bloom_set_bit(sig,
                    static_cast<uint32_t>(combined % options.signature_bits));
    }
  }
  return sig;
}

BloomSignature bloom_signature_for_storage(
    const BloomOptions &options, const Index &index,
    IndexScanKey::KeyPartData *key_columns) {
  std::vector<std::string_view> columns;
  columns.reserve(index.get_num_key_cols());
  for (uint32_t i = 0; i < index.get_num_key_cols(); i++) {
    const IndexScanKey::KeyPartData &col = key_columns[i];
    columns.emplace_back(reinterpret_cast<const char *>(col.data), col.length);
  }
  return bloom_signature_for_columns(options, columns);
}

bool bloom_parse_uint32(const char *value, uint32_t min_value,
                        uint32_t max_value, uint32_t *out) {
  char *end = nullptr;
  const unsigned long parsed = strtoul(value, &end, 10);
  if (end == value || *end != '\0' || parsed < min_value ||
      parsed > max_value) {
    return true;
  }
  *out = static_cast<uint32_t>(parsed);
  return false;
}

bool bloom_parse_options(const vef_index_param_t *params, uint32_t count,
                         BloomOptions *out, char *error_msg,
                         uint32_t error_msg_len) {
  *out = BloomOptions{};
  for (uint32_t i = 0; i < count; i++) {
    const char *key = params[i].key;
    const char *value = params[i].value;
    if (strcmp(key, "length") == 0) {
      if (bloom_parse_uint32(value, 64, 8192, &out->signature_bits)) {
        snprintf(error_msg, error_msg_len,
                 "length must be an integer in 64..8192, got '%s'", value);
        return true;
      }
    } else if (strcmp(key, "hashes") == 0) {
      if (bloom_parse_uint32(value, 1, 32, &out->default_hashes)) {
        snprintf(error_msg, error_msg_len,
                 "hashes must be an integer in 1..32, got '%s'", value);
        return true;
      }
    } else if (strncmp(key, "col", 3) == 0) {
      uint32_t col = 0;
      if (bloom_parse_uint32(key + 3, 1, kBloomMaxColumns, &col) ||
          bloom_parse_uint32(value, 1, 32, &out->column_hashes[col - 1])) {
        snprintf(error_msg, error_msg_len,
                 "column options use colN=1..32 for N in 1..%u",
                 kBloomMaxColumns);
        return true;
      }
    } else {
      snprintf(error_msg, error_msg_len, "unknown bloom option '%s'", key);
      return true;
    }
  }
  return false;
}

bool bloom_create(BloomCtx *ctx, const Index &index, Space::Ref /*space_ref*/,
                  Segment::TrxRef /*trx_ref*/, char * /*error_msg*/,
                  uint32_t /*error_msg_len*/) {
  ctx->user()->entries.clear();
  ctx->user()->next_ref = 1;
  if (const BloomOptions *opts = index.options<BloomOptions>()) {
    ctx->user()->options = *opts;
  }
  return false;
}

bool bloom_load(BloomCtx *ctx, const Index & /*index*/,
                Index::StorageRef storage_ref, char * /*error_msg*/,
                uint32_t /*error_msg_len*/) {
  ctx->set_ref(storage_ref);
  ctx->user()->entries.clear();
  ctx->user()->next_ref = 1;
  ctx->user()->options = BloomOptions{};
  return false;
}

bool bloom_drop(BloomCtx *ctx, const Index &index, Segment::TrxRef /*trx_ref*/,
                char * /*error_msg*/, uint32_t /*error_msg_len*/) {
  ctx->user()->entries.clear();
  const std::string index_name =
      index.get_index_ref() == nullptr
          ? ""
          : static_cast<const char *>(index.get_index_ref());
  if (!index_name.empty()) {
    std::lock_guard<std::mutex> guard(g_bloom_indexes_mu);
    g_bloom_indexes.erase(index_name);
  }
  return false;
}

bool bloom_pkey_equal(const BloomStorageEntry &entry, const Index &index,
                      IndexScanKey::KeyPartData *pkey_columns) {
  if (entry.pkey.size() != index.get_primary_num_key_cols()) return false;
  for (uint32_t i = 0; i < index.get_primary_num_key_cols(); i++) {
    const BloomPKeyPart &lhs = entry.pkey[i];
    const IndexScanKey::KeyPartData &rhs = pkey_columns[i];
    if (lhs.data.size() != rhs.length) return false;
    if (rhs.length > 0 && memcmp(lhs.data.data(), rhs.data, rhs.length) != 0) {
      return false;
    }
  }
  return true;
}

std::string bloom_storage_key_as_search_text(
    const IndexScanKey::KeyPartData &key) {
  if (key.data == nullptr || key.length == 0) return "";

  if (key.length >= 2 && key.length <= 256) {
    const uint32_t varchar_len = key.data[0];
    if (varchar_len <= key.length - 1) {
      return std::string(reinterpret_cast<const char *>(key.data + 1),
                         varchar_len);
    }
  }
  if (key.length >= 3) {
    const uint32_t varchar_len = static_cast<uint32_t>(key.data[0]) |
                                 (static_cast<uint32_t>(key.data[1]) << 8);
    if (varchar_len <= key.length - 2) {
      return std::string(reinterpret_cast<const char *>(key.data + 2),
                         varchar_len);
    }
  }
  size_t nul_pos = 0;
  while (nul_pos < key.length && key.data[nul_pos] != '\0') nul_pos++;
  if (nul_pos > 0 && nul_pos < key.length) {
    return std::string(reinterpret_cast<const char *>(key.data), nul_pos);
  }
  return std::string(reinterpret_cast<const char *>(key.data), key.length);
}

std::string bloom_runtime_index_name(const Index &index) {
  if (index.get_index_ref() == nullptr) return "";
  return static_cast<const char *>(index.get_index_ref());
}

long long bloom_pkey_as_int(const Index &index,
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

bool bloom_insert(BloomCtx *ctx, const Index &index,
                  Segment::TrxRef /*trx_ref*/,
                  IndexScanKey::KeyPartData *key_columns,
                  IndexScanKey::KeyPartData *pkey_columns,
                  IndexScanKey::KeyPartRef *key_ref, char *error_msg,
                  uint32_t error_msg_len) {
  if (index.get_num_key_cols() == 0 || key_columns == nullptr) {
    snprintf(error_msg, error_msg_len,
             "BLOOM index requires at least one key column");
    return true;
  }
  for (uint32_t i = 0; i < index.get_num_key_cols(); i++) {
    if (key_columns[i].data == nullptr && key_columns[i].length > 0) {
      snprintf(error_msg, error_msg_len, "BLOOM key column is invalid");
      return true;
    }
  }

  BloomStorageEntry entry;
  entry.ref = ctx->user()->next_ref++;
  entry.key_columns.resize(index.get_num_key_cols());
  for (uint32_t i = 0; i < index.get_num_key_cols(); i++) {
    bloom_assign_bytes(entry.key_columns[i], key_columns[i]);
  }
  entry.pkey.resize(index.get_primary_num_key_cols());
  for (uint32_t i = 0; i < index.get_primary_num_key_cols(); i++) {
    bloom_assign_bytes(entry.pkey[i].data, pkey_columns[i]);
  }
  entry.signature =
      bloom_signature_for_storage(ctx->user()->options, index, key_columns);
  if (key_ref != nullptr) *key_ref = entry.ref;
  ctx->user()->entries.push_back(std::move(entry));

  const std::string index_name = bloom_runtime_index_name(index);
  const long long pk = bloom_pkey_as_int(index, pkey_columns);
  if (!index_name.empty() && pk != 0 && index.get_num_key_cols() == 1) {
    const IndexScanKey::KeyPartData &key = key_columns[0];
    std::lock_guard<std::mutex> guard(g_bloom_indexes_mu);
    BloomManualIndex &mirror_index = g_bloom_indexes[index_name];
    mirror_index.options = ctx->user()->options;
    BloomManualEntry mirror;
    mirror.pk = pk;
    mirror.key.assign(reinterpret_cast<const char *>(key.data), key.length);
    const std::string search_key = bloom_storage_key_as_search_text(key);
    std::vector<std::string_view> columns{search_key};
    mirror.signature =
        bloom_signature_for_columns(mirror_index.options, columns);
    mirror_index.entries[pk] = std::move(mirror);
  }
  return false;
}

bool bloom_mark_delete(BloomCtx *ctx, const Index &index,
                       Segment::TrxRef /*trx_ref*/,
                       IndexScanKey::KeyPartRef *key_ref,
                       IndexScanKey::KeyPartData * /*key_columns*/,
                       IndexScanKey::KeyPartData *pkey_columns,
                       bool delete_mark, char * /*error_msg*/,
                       uint32_t /*error_msg_len*/) {
  for (BloomStorageEntry &entry : ctx->user()->entries) {
    if (key_ref != nullptr && *key_ref != IndexScanKey::EMPTY_REF &&
        entry.ref != *key_ref) {
      continue;
    }
    if ((key_ref == nullptr || *key_ref == IndexScanKey::EMPTY_REF) &&
        !bloom_pkey_equal(entry, index, pkey_columns)) {
      continue;
    }
    entry.delete_marked = delete_mark;
    const std::string index_name = bloom_runtime_index_name(index);
    const long long pk = bloom_pkey_as_int(index, pkey_columns);
    if (delete_mark && !index_name.empty() && pk != 0) {
      std::lock_guard<std::mutex> guard(g_bloom_indexes_mu);
      auto index_it = g_bloom_indexes.find(index_name);
      if (index_it != g_bloom_indexes.end()) {
        index_it->second.entries.erase(pk);
      }
    }
    return false;
  }
  return false;
}

bool bloom_purge(BloomCtx *ctx, const Index &index, Segment::TrxRef /*trx_ref*/,
                 IndexScanKey::KeyPartRef *key_ref,
                 IndexScanKey::KeyPartData * /*key_columns*/,
                 IndexScanKey::KeyPartData *pkey_columns, char * /*error_msg*/,
                 uint32_t /*error_msg_len*/) {
  auto &entries = ctx->user()->entries;
  entries.erase(std::remove_if(entries.begin(), entries.end(),
                               [&](const BloomStorageEntry &entry) {
                                 if (key_ref != nullptr &&
                                     *key_ref != IndexScanKey::EMPTY_REF) {
                                   return entry.ref == *key_ref;
                                 }
                                 return bloom_pkey_equal(entry, index,
                                                         pkey_columns);
                               }),
                entries.end());
  return false;
}

bool bloom_scan_begin(BloomCtx *ctx, const Index &index, MtrCtx::Ref /*mctx*/,
                      const IndexScanDesc &scan_desc, Index::Cursor *cursor,
                      bool *eof, char *error_msg, uint32_t error_msg_len) {
  *cursor = nullptr;
  *eof = true;
  if (!scan_desc.is_point() || scan_desc.num_keys() != 1) {
    snprintf(error_msg, error_msg_len,
             "BLOOM index only supports full-key point lookups");
    return true;
  }
  const IndexScanKey scan_key = scan_desc[0];
  if (!scan_key.is_begin() || !scan_key.include_key() ||
      scan_key.num_columns() != index.get_num_key_cols() ||
      !scan_key.is_bounded()) {
    snprintf(error_msg, error_msg_len,
             "BLOOM index only supports full-key point lookups");
    return true;
  }

  auto *c = new (std::nothrow) BloomCursor();
  if (c == nullptr) {
    snprintf(error_msg, error_msg_len, "out of memory allocating cursor");
    return true;
  }
  c->storage = ctx->user();
  // Build the query signature from the scan key's columns. We need a
  // contiguous KeyPartData[] to pass to bloom_signature_for_storage, so
  // copy them into a local vector.
  std::vector<IndexScanKey::KeyPartData> query_cols;
  query_cols.reserve(scan_key.num_columns());
  for (uint32_t i = 0; i < scan_key.num_columns(); i++) {
    query_cols.push_back(scan_key[i]);
  }
  const BloomSignature query = bloom_signature_for_storage(
      ctx->user()->options, index, query_cols.data());
  for (size_t i = 0; i < c->storage->entries.size(); i++) {
    const BloomStorageEntry &entry = c->storage->entries[i];
    if (!entry.delete_marked &&
        bloom_contains_signature(entry.signature, query)) {
      c->hits.push_back(i);
      if (scan_desc.limit() > 0 && c->hits.size() >= scan_desc.limit()) break;
    }
  }
  *eof = c->hits.empty();
  *cursor = c;
  return false;
}

bool bloom_scan_position(Index::Cursor cursor, Index::CursorOp op, bool *eof,
                         char * /*error_msg*/, uint32_t /*error_msg_len*/) {
  auto *c = static_cast<BloomCursor *>(cursor);
  if (op == Index::CursorOp::Prev) {
    *eof = true;
    return false;
  }
  if (c->pos < c->hits.size()) c->pos++;
  *eof = c->pos >= c->hits.size();
  return false;
}

bool bloom_scan_fetch(Index::Cursor cursor, IndexScanKey::KeyPartRef *key_ref,
                      IndexScanKey::KeyPartData *key_columns,
                      IndexScanKey::KeyPartData *pkey_columns, char *error_msg,
                      uint32_t error_msg_len) {
  auto *c = static_cast<BloomCursor *>(cursor);
  if (c->pos >= c->hits.size()) {
    snprintf(error_msg, error_msg_len, "BLOOM index cursor is at EOF");
    return true;
  }

  const BloomStorageEntry &entry = c->storage->entries[c->hits[c->pos]];
  if (key_ref != nullptr) *key_ref = entry.ref;
  for (size_t i = 0; i < entry.key_columns.size(); i++) {
    key_columns[i] = IndexScanKey::KeyPartData{
        .data = entry.key_columns[i].data(),
        .length = static_cast<uint32_t>(entry.key_columns[i].size())};
  }
  for (size_t i = 0; i < entry.pkey.size(); i++) {
    pkey_columns[i] = IndexScanKey::KeyPartData{
        .data = entry.pkey[i].data.data(),
        .length = static_cast<uint32_t>(entry.pkey[i].data.size())};
  }
  return false;
}

bool bloom_scan_save(Index::Cursor /*cursor*/, char * /*error_msg*/,
                     uint32_t /*error_msg_len*/) {
  return false;
}

bool bloom_scan_restore(Index::Cursor cursor, MtrCtx::Ref /*mctx*/, bool *eof,
                        char * /*error_msg*/, uint32_t /*error_msg_len*/) {
  auto *c = static_cast<BloomCursor *>(cursor);
  *eof = c->pos >= c->hits.size();
  return false;
}

void bloom_scan_end(Index::Cursor *cursor) {
  delete static_cast<BloomCursor *>(*cursor);
  *cursor = nullptr;
}

bool bloom_index_name(vsql::StringArg index_name, std::string &out,
                      vsql::IntResult &result) {
  if (index_name.is_null()) {
    result.set_null();
    return false;
  }
  auto value = index_name.value();
  out.assign(value.data(), value.size());
  if (out.empty()) {
    result.error("bloom index name must not be empty");
    return false;
  }
  return true;
}

bool bloom_index_name(vsql::StringArg index_name, std::string &out,
                      vsql::StringResult &result) {
  if (index_name.is_null()) {
    result.set_null();
    return false;
  }
  auto value = index_name.value();
  out.assign(value.data(), value.size());
  if (out.empty()) {
    result.error("bloom index name must not be empty");
    return false;
  }
  return true;
}

void bloom_index_clear(vsql::StringArg index_name, vsql::IntResult out) {
  std::string name;
  if (!bloom_index_name(index_name, name, out)) return;

  std::lock_guard<std::mutex> guard(g_bloom_indexes_mu);
  g_bloom_indexes.erase(name);
  out.set(1);
}

void bloom_index_upsert(vsql::StringArg index_name, vsql::IntArg pk,
                        vsql::StringArg key, vsql::IntResult out) {
  std::string name;
  if (!bloom_index_name(index_name, name, out)) return;
  if (pk.is_null() || key.is_null()) {
    out.set_null();
    return;
  }

  auto key_value = key.value();
  std::vector<std::string_view> columns{key_value};
  BloomManualEntry entry;
  entry.pk = pk.value();
  entry.key.assign(key_value.data(), key_value.size());

  std::lock_guard<std::mutex> guard(g_bloom_indexes_mu);
  BloomManualIndex &index = g_bloom_indexes[name];
  entry.signature = bloom_signature_for_columns(index.options, columns);
  index.entries[entry.pk] = std::move(entry);
  out.set(1);
}

void bloom_index_delete(vsql::StringArg index_name, vsql::IntArg pk,
                        vsql::IntResult out) {
  std::string name;
  if (!bloom_index_name(index_name, name, out)) return;
  if (pk.is_null()) {
    out.set_null();
    return;
  }

  std::lock_guard<std::mutex> guard(g_bloom_indexes_mu);
  auto index_it = g_bloom_indexes.find(name);
  if (index_it == g_bloom_indexes.end()) {
    out.set(0);
    return;
  }
  out.set(index_it->second.entries.erase(pk.value()) ? 1 : 0);
}

void bloom_index_count(vsql::StringArg index_name, vsql::IntResult out) {
  std::string name;
  if (!bloom_index_name(index_name, name, out)) return;

  std::lock_guard<std::mutex> guard(g_bloom_indexes_mu);
  auto index_it = g_bloom_indexes.find(name);
  if (index_it == g_bloom_indexes.end()) {
    out.set(0);
    return;
  }
  out.set(static_cast<long long>(index_it->second.entries.size()));
}

void bloom_might_contain(vsql::StringArg index_name, vsql::StringArg key,
                         vsql::IntResult out) {
  std::string name;
  if (!bloom_index_name(index_name, name, out)) return;
  if (key.is_null()) {
    out.set_null();
    return;
  }

  auto key_value = key.value();
  std::vector<std::string_view> columns{key_value};
  std::lock_guard<std::mutex> guard(g_bloom_indexes_mu);
  auto index_it = g_bloom_indexes.find(name);
  if (index_it == g_bloom_indexes.end()) {
    out.set(0);
    return;
  }
  const BloomSignature query =
      bloom_signature_for_columns(index_it->second.options, columns);
  for (const auto &kv : index_it->second.entries) {
    if (bloom_key_matches_text(kv.second.key, key_value) ||
        bloom_contains_signature(kv.second.signature, query)) {
      out.set(1);
      return;
    }
  }
  out.set(0);
}

void bloom_search(vsql::StringArg index_name, vsql::StringArg key,
                  vsql::StringResult out) {
  std::string name;
  if (!bloom_index_name(index_name, name, out)) return;
  if (key.is_null()) {
    out.set_null();
    return;
  }

  auto key_value = key.value();
  std::vector<std::string_view> columns{key_value};
  std::vector<long long> hits;
  {
    std::lock_guard<std::mutex> guard(g_bloom_indexes_mu);
    auto index_it = g_bloom_indexes.find(name);
    if (index_it == g_bloom_indexes.end()) {
      out.set("");
      return;
    }
    const BloomSignature query =
        bloom_signature_for_columns(index_it->second.options, columns);
    for (const auto &kv : index_it->second.entries) {
      if (bloom_key_matches_text(kv.second.key, key_value) ||
          bloom_contains_signature(kv.second.signature, query)) {
        hits.push_back(kv.second.pk);
      }
    }
  }

  std::sort(hits.begin(), hits.end());
  auto buf = out.buffer();
  size_t pos = 0;
  for (size_t i = 0; i < hits.size(); i++) {
    int written = snprintf(buf.data() + pos, buf.size() - pos, "%s%lld",
                           i == 0 ? "" : ",", hits[i]);
    if (written < 0 || pos + static_cast<size_t>(written) >= buf.size()) {
      out.error("bloom_search: output buffer too small");
      return;
    }
    pos += static_cast<size_t>(written);
  }
  out.set_length(pos);
}

static constexpr const char kBloomIndexTypeName[] = "bloom";

[[maybe_unused]] constexpr auto BLOOM_MEMORY_INDEX =
    vsql::preview_index_builder::make_index_type<kBloomIndexTypeName,
                                                 BloomStorage>()
        .lifecycle()
        .create<&bloom_create>()
        .load<&bloom_load>()
        .drop<&bloom_drop>()
        .dml()
        .insert<&bloom_insert>()
        .mark_delete<&bloom_mark_delete>()
        .purge<&bloom_purge>()
        .scan()
        .begin<&bloom_scan_begin>()
        .position<&bloom_scan_position>()
        .fetch<&bloom_scan_fetch>()
        .save<&bloom_scan_save>()
        .restore<&bloom_scan_restore>()
        .end<&bloom_scan_end>()
        .global()
        .capabilities(Index::Support::POINT_LOOKUP)
        .storage_props(Index::Storage::HAS_ROW_REF | Index::Storage::REF_LOOKUP)
        .options<BloomOptions, &bloom_parse_options>()
        .build();

static auto BLOOM_INDEXES =
    vsql::preview_index_builder::IndexTypeCapability().index_type(
        BLOOM_MEMORY_INDEX);

using namespace vsql;

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .with(BLOOM_INDEXES)
        .func(make_func<&bloom_index_clear>("bloom_index_clear")
                  .returns(INT)
                  .param(STRING)
                  .build())
        .func(make_func<&bloom_index_upsert>("bloom_index_insert")
                  .returns(INT)
                  .param(STRING)
                  .param(INT)
                  .param(STRING)
                  .build())
        .func(make_func<&bloom_index_upsert>("bloom_index_update")
                  .returns(INT)
                  .param(STRING)
                  .param(INT)
                  .param(STRING)
                  .build())
        .func(make_func<&bloom_index_upsert>("bloom_index_upsert")
                  .returns(INT)
                  .param(STRING)
                  .param(INT)
                  .param(STRING)
                  .build())
        .func(make_func<&bloom_index_delete>("bloom_index_delete")
                  .returns(INT)
                  .param(STRING)
                  .param(INT)
                  .build())
        .func(make_func<&bloom_index_count>("bloom_index_count")
                  .returns(INT)
                  .param(STRING)
                  .build())
        .func(make_func<&bloom_might_contain>("bloom_might_contain")
                  .returns(INT)
                  .param(STRING)
                  .param(STRING)
                  .build())
        .func(make_func<&bloom_search>("bloom_search")
                  .returns(STRING)
                  .param(STRING)
                  .param(STRING)
                  .buffer_size(4096)
                  .build()))
