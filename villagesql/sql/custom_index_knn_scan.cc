/* Copyright (c) 2026 VillageSQL Contributors
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see <https://www.gnu.org/licenses/>.
 */

#include "villagesql/sql/custom_index_knn_scan.h"

#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "my_base.h"
#include "mysql/strings/m_ctype.h"
#include "sql/handler.h"
#include "sql/iterators/row_iterator.h"
#include "sql/iterators/timing_iterator.h"
#include "sql/key.h"
#include "sql/sql_executor.h"
#include "sql/table.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/descriptor/index_type_descriptor.h"
#include "villagesql/schema/systable/custom_indexes.h"
#include "villagesql/schema/victionary_client.h"
#include "villagesql/sql/custom_index_runtime_internal.h"

namespace villagesql {

struct CustomIndexKnnScan {
  // The loaded index handle the storage engine owns for this open index
  // (fetched via handler::get_custom_index_handle). We do not own these; the
  // engine keeps them alive for the lifetime of the open index.
  const vef_type_index_intf_t *intf{nullptr};
  vef_index_ctx_t *ctx{nullptr};
  vef_storage_ctx_t *storage{nullptr};
  vef_index_cursor_ref_t cursor{0};
  bool eof{true};
  // scan_fetch fills these; the REF_LOOKUP read path consumes only key_ref
  // (the extension's stable column reference), resolving it to the row inside
  // the engine. key_columns/pkey_columns arrays are still passed to scan_fetch
  // per the ABI, but their contents are not consumed here.
  std::vector<vef_storage_col_data_t> key_columns;
  std::vector<vef_storage_col_data_t> pkey_columns;
};

bool custom_index_knn_scan_begin(TABLE *table, uint key_idx,
                                 const char *index_name,
                                 const unsigned char *query_key,
                                 uint32_t query_key_len, uint32_t limit,
                                 CustomIndexKnnScan **scan, char *error_msg,
                                 uint32_t error_msg_len) {
  if (scan == nullptr) return true;
  *scan = nullptr;
  if (table == nullptr || table->s == nullptr || index_name == nullptr ||
      query_key == nullptr || query_key_len == 0) {
    snprintf(error_msg, error_msg_len, "invalid custom index KNN scan");
    return true;
  }

  VictionaryClient &vclient = VictionaryClient::instance();
  if (!vclient.is_initialized()) {
    snprintf(error_msg, error_msg_len, "Victionary is not initialized");
    return true;
  }

  const IndexEntry *matched_entry = nullptr;
  const IndexTypeDescriptor *matched_descriptor = nullptr;
  std::vector<std::string> matched_columns;
  {
    auto read_lock = vclient.get_read_lock();
    std::vector<const IndexEntry *> indexes = vclient.GetCustomIndexesForTable(
        table->s->db.str, table->s->table_name.str);
    for (const IndexEntry *entry : indexes) {
      if (my_strcasecmp(system_charset_info, entry->index_name().c_str(),
                        index_name) != 0) {
        continue;
      }
      const auto *descriptor = vclient.index_type_descriptors().get_committed(
          IndexTypeDescriptorKey(entry->index_type_name, entry->extension_name,
                                 entry->extension_version));
      if (descriptor == nullptr ||
          !(descriptor->intf().capabilities & VEF_INDEX_CAP_KNN)) {
        snprintf(error_msg, error_msg_len,
                 "custom index '%s' does not support KNN", index_name);
        return true;
      }
      get_custom_index_columns(/*thd=*/nullptr, entry->index_id,
                               &matched_columns);
      matched_entry = entry;
      matched_descriptor = descriptor;
      break;
    }
  }

  if (matched_entry == nullptr || matched_descriptor == nullptr) {
    snprintf(error_msg, error_msg_len, "custom index '%s' not found",
             index_name);
    return true;
  }
  if (matched_columns.size() != 1) {
    snprintf(error_msg, error_msg_len,
             "custom KNN scans require a one-column index");
    return true;
  }
  // No primary-key requirement: the REF_LOOKUP read path resolves the
  // extension's column reference to the row inside the engine (works for
  // PK-less tables via the hidden DB_ROW_ID too).

  // Use the loaded index instance the storage engine already holds for this
  // open index (it loaded the extension with the correct persisted
  // storage_ref at table-open). We drive the extension's scan callbacks
  // against that live storage rather than re-loading our own copy. The
  // InnoDB-resident insert path (Custom_index::insert) uses the same handle,
  // so insert and scan operate on the same engine-owned storage context.
  const vef_type_index_intf_t *intf = nullptr;
  vef_index_ctx_t *ctx = nullptr;
  vef_storage_ctx_t *storage = nullptr;
  if (get_loaded_custom_index(table->file, key_idx, &intf, &ctx, &storage)) {
    snprintf(error_msg, error_msg_len,
             "custom index '%s' is not loaded by the storage engine",
             index_name);
    return true;
  }

  auto result = std::make_unique<CustomIndexKnnScan>();
  result->intf = intf;
  result->ctx = ctx;
  result->storage = storage;
  result->key_columns.resize(ctx->num_key_columns);
  result->pkey_columns.resize(ctx->num_primary_key_columns);

  vef_storage_col_data_t query_column{
      .data = query_key,
      .length = query_key_len,
  };
  vef_index_scan_key_t scan_key{
      .version = 1,
      .type = VEF_INDEX_SCAN_KEY_TYPE_KNN_QUERY,
      .num_key_columns = 1,
      .key_columns = &query_column,
      .include_key = true,
  };
  vef_index_scan_desc_t scan_desc{
      .version = 1,
      .scan_type = VEF_INDEX_SCAN_TYPE_KNN,
      .reverse = false,
      .limit = limit,
      .num_keys = 1,
      .keys = &scan_key,
  };

  if (intf->scan_begin(ctx, storage, /*mctx=*/0, &scan_desc, &result->cursor,
                       &result->eof, error_msg, error_msg_len)) {
    return true;
  }

  *scan = result.release();
  return false;
}

bool custom_index_knn_scan_next(CustomIndexKnnScan *scan, uint64_t *out_key_ref,
                                bool *eof, char *error_msg,
                                uint32_t error_msg_len) {
  if (scan == nullptr || out_key_ref == nullptr || eof == nullptr) {
    snprintf(error_msg, error_msg_len, "invalid custom index KNN cursor");
    return true;
  }
  if (scan->eof) {
    *eof = true;
    return false;
  }

  vef_storage_col_ref_t key_ref = VEF_STORAGE_EMPTY_COLUMN_REF;
  if (scan->intf->scan_fetch(scan->cursor, &key_ref, scan->key_columns.data(),
                             scan->pkey_columns.data(), error_msg,
                             error_msg_len)) {
    return true;
  }

  // REF_LOOKUP: the extension's stable column reference identifies the row; the
  // server resolves it to the base row inside the engine (see
  // handler::custom_index_ref_to_row). No primary key is consumed here.
  if (key_ref == VEF_STORAGE_EMPTY_COLUMN_REF) {
    snprintf(error_msg, error_msg_len,
             "custom index KNN cursor returned an empty column reference");
    return true;
  }
  *out_key_ref = static_cast<uint64_t>(key_ref);
  *eof = false;

  bool next_eof = false;
  if (scan->intf->scan_position(scan->cursor, VEF_INDEX_CURSOR_OP_NEXT,
                                &next_eof, error_msg, error_msg_len)) {
    return true;
  }
  scan->eof = next_eof;
  return false;
}

void custom_index_knn_scan_end(CustomIndexKnnScan **scan) {
  if (scan == nullptr || *scan == nullptr) return;
  if ((*scan)->cursor != 0) {
    (*scan)->intf->scan_end(&(*scan)->cursor);
  }
  delete *scan;
  *scan = nullptr;
}

namespace {

class CustomHypergraphDistanceIterator final : public TableRowIterator {
 public:
  CustomHypergraphDistanceIterator(THD *thd, TABLE *table, int key_idx,
                                   const CustomHypergraphDistanceScanSpec *spec,
                                   double expected_rows, ha_rows *examined_rows)
      : TableRowIterator(thd, table),
        m_record(table->record[0]),
        m_key_idx(key_idx),
        m_spec(spec),
        m_expected_rows(expected_rows),
        m_examined_rows(examined_rows) {}

  ~CustomHypergraphDistanceIterator() override {
    custom_index_knn_scan_end(&m_scan);
  }

  bool Init() override {
    custom_index_knn_scan_end(&m_scan);
    int error = 0;
    // Random-read init: the REF_LOOKUP fetch (custom_index_ref_to_row) does its
    // own clustered lookup through the prebuilt read state, like rnd_pos.
    if (!table()->file->inited) {
      error = table()->file->ha_rnd_init(false);
      if (error) {
        PrintError(error);
        return true;
      }
    }

    if (set_record_buffer(table(), m_expected_rows)) {
      return true;
    }

    char error_msg[512]{};
    if (custom_index_knn_scan_begin(
            table(), m_key_idx, table()->key_info[m_key_idx].name,
            m_spec->query_key, m_spec->query_key_len, m_spec->limit, &m_scan,
            error_msg, sizeof(error_msg))) {
      LogVSQL(ERROR_LEVEL, "Failed to begin custom KNN scan: %s", error_msg);
      PrintError(HA_ERR_INTERNAL_ERROR);
      return true;
    }
    return false;
  }

  int Read() override {
    for (;;) {
      uint64_t key_ref = 0;
      bool eof = false;
      char error_msg[512]{};
      if (custom_index_knn_scan_next(m_scan, &key_ref, &eof, error_msg,
                                     sizeof(error_msg))) {
        LogVSQL(ERROR_LEVEL, "Failed to read custom KNN scan: %s", error_msg);
        return HandleError(HA_ERR_INTERNAL_ERROR);
      }
      if (eof) return -1;

      // REF_LOOKUP: resolve the extension's column reference to the full row
      // inside the engine. No primary key involved.
      bool row_not_found = false;
      if (table()->file->custom_index_ref_to_row(m_key_idx, key_ref, m_record,
                                                 &row_not_found, error_msg,
                                                 sizeof(error_msg))) {
        LogVSQL(ERROR_LEVEL, "Failed to fetch row for KNN hit: %s", error_msg);
        return HandleError(HA_ERR_INTERNAL_ERROR);
      }
      if (row_not_found) {
        // The KNN hit's row is not visible to this transaction (MVCC) -- e.g. a
        // concurrently-inserted, uncommitted row the graph surfaced. Skip it and
        // pull the next candidate; the scan returned the full ef_search pool
        // (not just k), so there is backfill to satisfy the LIMIT. Not counted
        // as an examined row.
        continue;
      }
      if (m_examined_rows != nullptr) {
        ++*m_examined_rows;
      }
      return 0;
    }
  }

 private:
  uchar *const m_record;
  const int m_key_idx;
  const CustomHypergraphDistanceScanSpec *const m_spec;
  const double m_expected_rows;
  ha_rows *const m_examined_rows;
  CustomIndexKnnScan *m_scan{nullptr};
};

}  // namespace

unique_ptr_destroy_only<RowIterator> CreateCustomHypergraphDistanceIterator(
    THD *thd, MEM_ROOT *mem_root, TABLE *table, int key_idx,
    void *custom_scan_spec, double expected_rows, ha_rows *examined_rows) {
  const auto *spec =
      static_cast<const CustomHypergraphDistanceScanSpec *>(custom_scan_spec);
  if (spec == nullptr || spec->table != table) {
    return unique_ptr_destroy_only<RowIterator>(nullptr);
  }
  return NewIterator<CustomHypergraphDistanceIterator>(
      thd, mem_root, table, key_idx, spec, expected_rows, examined_rows);
}

}  // namespace villagesql
