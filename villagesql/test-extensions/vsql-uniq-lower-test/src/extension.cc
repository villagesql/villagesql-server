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

#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>

// Per-index storage is unused — the hidden table is the entire state.
struct UniqLowerStorage {};

struct EmptyCursor {};

using vsql::preview_index_builder::Index;
using vsql::preview_index_builder::IndexScanDesc;
using vsql::preview_index_builder::IndexScanKey;
using vsql::preview_storage::MtrCtx;
using vsql::preview_storage::Segment;
using vsql::preview_storage::Space;

using UniqLowerCtx = Index::StorageCtx<UniqLowerStorage>;

static vsql::preview_table_storage::TableStorageCapability g_hidden_table;

static constexpr const char kUniqLowerIndexTypeName[] = "uniq_lower";

// Hidden table schema:
//   lower_value VARBINARY(255) NOT NULL  -- lowercased indexed column
//   pk_id       VARBINARY(255) NOT NULL  -- base row's primary key bytes
//   PRIMARY KEY (lower_value)
//
// The PK on lower_value makes the uniqueness check free — InnoDB rejects
// duplicate inserts and we surface that as the index's error. pk_id is
// stored for completeness (and as a future identity for lookup-style
// scans).
//
// TODO(villagesql-indexing): the runtime passes raw Field::data_ptr() +
// pack_length() to extensions for non-custom-type fields. For utf8mb4
// VARCHAR/CHAR fields this includes MySQL's length prefix and is also
// 4x the character count. Real implementation should extract logical
// string bytes via val_str() so extensions don't have to know MySQL's
// on-row packing, and so charset/collation can be respected. Until
// then this extension targets ASCII columns only (CHAR(N)
// CHARACTER SET ascii), where pack_length ≈ N + small prefix and the
// VARBINARY(255) PK is sufficient.
//
// TODO(villagesql-indexing): the hidden-table backend rounds the
// requested max_length to 255 or 4096 (see table_storage.cc), and 4096
// exceeds InnoDB's 3072-byte single-column PK limit. Backend should
// pass the requested length through directly.
static const vef_table_storage_col_def_t kUniqLowerColumns[] = {
    {.name = "lower_value",
     .type = VEF_TABLE_STORAGE_COL_BYTES,
     .max_length = 255,
     .nullable = false},
    {.name = "pk_id",
     .type = VEF_TABLE_STORAGE_COL_BYTES,
     .max_length = 255,
     .nullable = false},
};
static const uint32_t kUniqLowerPrimaryKey[] = {0};

bool uniq_lower_index_table_storage_def(const vef_index_ctx_t * /*index_ctx*/,
                                        vef_table_storage_def_t *def_out,
                                        char * /*error_msg*/,
                                        uint32_t /*error_msg_len*/) {
  def_out->version = 1;
  // TODO(villagesql-indexing): logical name is hardcoded, so only one
  // uniq_lower index is supported system-wide. Same limitation as
  // bloom_hidden — fix when per-index naming lands.
  def_out->logical_name = "uniq_lower";
  def_out->columns = kUniqLowerColumns;
  def_out->column_count =
      sizeof(kUniqLowerColumns) / sizeof(kUniqLowerColumns[0]);
  def_out->primary_key_columns = kUniqLowerPrimaryKey;
  def_out->primary_key_column_count =
      sizeof(kUniqLowerPrimaryKey) / sizeof(kUniqLowerPrimaryKey[0]);
  return false;
}

bool uniq_lower_index_create(UniqLowerCtx * /*ctx*/, const Index & /*index*/,
                             Space::Ref /*space_ref*/,
                             Segment::TrxRef /*trx_ref*/, char * /*error_msg*/,
                             uint32_t /*error_msg_len*/) {
  return false;
}

bool uniq_lower_index_load(UniqLowerCtx * /*ctx*/, const Index & /*index*/,
                           Index::StorageRef /*storage_ref*/,
                           char * /*error_msg*/, uint32_t /*error_msg_len*/) {
  return false;
}

bool uniq_lower_index_drop(UniqLowerCtx * /*ctx*/, const Index & /*index*/,
                           Segment::TrxRef /*trx_ref*/, char * /*error_msg*/,
                           uint32_t /*error_msg_len*/) {
  return false;
}

// ASCII-only lowercase. Sufficient for a POC — locale-aware lowering is
// out of scope.
static std::string ascii_lower(const IndexScanKey::KeyPartData &col) {
  std::string out;
  if (col.data == nullptr) return out;
  out.assign(reinterpret_cast<const char *>(col.data), col.length);
  for (char &c : out) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return out;
}

bool uniq_lower_index_insert(UniqLowerCtx * /*ctx*/, const Index &index,
                             Segment::TrxRef /*trx_ref*/,
                             IndexScanKey::KeyPartData *key_columns,
                             IndexScanKey::KeyPartData *pkey_columns,
                             IndexScanKey::KeyPartRef * /*key_ref*/,
                             char *error_msg, uint32_t error_msg_len) {
  if (index.get_num_key_cols() != 1 || index.get_primary_num_key_cols() < 1 ||
      key_columns == nullptr || pkey_columns == nullptr) {
    snprintf(error_msg, error_msg_len,
             "uniq_lower requires one key column and a primary key");
    return true;
  }
  if (index.get_table_storage_handle() == nullptr) {
    snprintf(error_msg, error_msg_len,
             "uniq_lower requires a server-provided hidden table handle");
    return true;
  }

  const std::string lower = ascii_lower(key_columns[0]);
  vef_table_storage_value_t values[2] = {
      {.data = reinterpret_cast<const unsigned char *>(lower.data()),
       .length = static_cast<uint32_t>(lower.size()),
       .is_null = false},
      {.data = pkey_columns[0].data,
       .length = pkey_columns[0].length,
       .is_null = false},
  };

  const vef_preview_table_storage_t *abi = g_hidden_table.abi();
  if (abi == nullptr) {
    snprintf(error_msg, error_msg_len,
             "table_storage capability is unavailable");
    return true;
  }
  // The hidden table's PRIMARY KEY (lower_value) does the uniqueness
  // check. ha_write_row returns HA_ERR_FOUND_DUPP_KEY (121) when the
  // lowered value already exists; the ABI surfaces that as a generic
  // insert failure, which we report back to the runtime. TODO: thread
  // a nicer "Duplicate entry 'X'" message through.
  return abi->insert(index.get_table_storage_handle(), values, 2, error_msg,
                     error_msg_len);
}

bool uniq_lower_index_mark_delete(UniqLowerCtx * /*ctx*/, const Index &index,
                                  Segment::TrxRef /*trx_ref*/,
                                  IndexScanKey::KeyPartRef * /*key_ref*/,
                                  IndexScanKey::KeyPartData *key_columns,
                                  IndexScanKey::KeyPartData * /*pkey_columns*/,
                                  bool delete_mark, char *error_msg,
                                  uint32_t error_msg_len) {
  if (!delete_mark) return false;
  if (index.get_num_key_cols() != 1 || key_columns == nullptr) {
    snprintf(error_msg, error_msg_len,
             "uniq_lower delete requires one key column");
    return true;
  }
  if (index.get_table_storage_handle() == nullptr) {
    snprintf(error_msg, error_msg_len,
             "uniq_lower requires a server-provided hidden table handle");
    return true;
  }

  const std::string lower = ascii_lower(key_columns[0]);
  vef_table_storage_value_t key_values[1] = {
      {.data = reinterpret_cast<const unsigned char *>(lower.data()),
       .length = static_cast<uint32_t>(lower.size()),
       .is_null = false},
  };

  const vef_preview_table_storage_t *abi = g_hidden_table.abi();
  if (abi == nullptr) {
    snprintf(error_msg, error_msg_len,
             "table_storage capability is unavailable");
    return true;
  }
  return abi->delete_row(index.get_table_storage_handle(), key_values, 1,
                         error_msg, error_msg_len);
}

bool uniq_lower_index_purge(UniqLowerCtx * /*ctx*/, const Index & /*index*/,
                            Segment::TrxRef /*trx_ref*/,
                            IndexScanKey::KeyPartRef * /*key_ref*/,
                            IndexScanKey::KeyPartData * /*key_columns*/,
                            IndexScanKey::KeyPartData * /*pkey_columns*/,
                            char * /*error_msg*/, uint32_t /*error_msg_len*/) {
  return false;
}

bool uniq_lower_index_scan_begin(UniqLowerCtx * /*ctx*/,
                                 const Index & /*index*/, MtrCtx::Ref /*mctx*/,
                                 const IndexScanDesc & /*scan_desc*/,
                                 Index::Cursor *cursor, bool *eof,
                                 char * /*error_msg*/,
                                 uint32_t /*error_msg_len*/) {
  *cursor = reinterpret_cast<Index::Cursor>(new EmptyCursor());
  *eof = true;
  return false;
}

bool uniq_lower_index_scan_position(Index::Cursor /*cursor*/,
                                    Index::CursorOp /*op*/, bool *eof,
                                    char * /*error_msg*/,
                                    uint32_t /*error_msg_len*/) {
  *eof = true;
  return false;
}

bool uniq_lower_index_scan_fetch(Index::Cursor /*cursor*/,
                                 IndexScanKey::KeyPartRef * /*key_ref*/,
                                 IndexScanKey::KeyPartData * /*key_columns*/,
                                 IndexScanKey::KeyPartData * /*pkey_columns*/,
                                 char *error_msg, uint32_t error_msg_len) {
  snprintf(error_msg, error_msg_len,
           "uniq_lower scan is not implemented in the maintenance POC");
  return true;
}

bool uniq_lower_index_scan_save(Index::Cursor /*cursor*/, char * /*error_msg*/,
                                uint32_t /*error_msg_len*/) {
  return false;
}

bool uniq_lower_index_scan_restore(Index::Cursor /*cursor*/,
                                   MtrCtx::Ref /*mctx*/, bool *eof,
                                   char * /*error_msg*/,
                                   uint32_t /*error_msg_len*/) {
  *eof = true;
  return false;
}

void uniq_lower_index_scan_end(Index::Cursor *cursor) {
  if (cursor == nullptr || *cursor == nullptr) return;
  delete reinterpret_cast<EmptyCursor *>(*cursor);
  *cursor = nullptr;
}

[[maybe_unused]] constexpr auto UNIQ_LOWER_INDEX =
    vsql::preview_index_builder::make_index_type<kUniqLowerIndexTypeName,
                                                 UniqLowerStorage>()
        .lifecycle()
        .create<&uniq_lower_index_create>()
        .load<&uniq_lower_index_load>()
        .drop<&uniq_lower_index_drop>()
        .dml()
        .insert<&uniq_lower_index_insert>()
        .mark_delete<&uniq_lower_index_mark_delete>()
        .purge<&uniq_lower_index_purge>()
        .scan()
        .begin<&uniq_lower_index_scan_begin>()
        .position<&uniq_lower_index_scan_position>()
        .fetch<&uniq_lower_index_scan_fetch>()
        .save<&uniq_lower_index_scan_save>()
        .restore<&uniq_lower_index_scan_restore>()
        .end<&uniq_lower_index_scan_end>()
        .global()
        .capabilities(Index::Support::POINT_LOOKUP)
        .storage_props(Index::Storage::HAS_ROW_REF | Index::Storage::REF_LOOKUP)
        .table_storage<&uniq_lower_index_table_storage_def>()
        .build();

static auto UNIQ_LOWER_INDEXES =
    vsql::preview_index_builder::IndexTypeCapability().index_type(
        UNIQ_LOWER_INDEX);

using namespace vsql;

VEF_GENERATE_ENTRY_POINTS(
    make_extension().with(g_hidden_table).with(UNIQ_LOWER_INDEXES))
