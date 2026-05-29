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

#include "villagesql/sql/custom_index_hypergraph_optimizer.h"

#include <algorithm>
#include <cstdio>
#include <limits>
#include <string>
#include <vector>

#include "my_base.h"
#include "my_sys.h"
#include "mysql/strings/m_ctype.h"
#include "sql/field.h"
#include "sql/handler.h"
#include "sql/item.h"
#include "sql/item_func.h"
#include "sql/iterators/row_iterator.h"
#include "sql/iterators/timing_iterator.h"
#include "sql/join_optimizer/build_interesting_orders.h"
#include "sql/join_optimizer/interesting_orders.h"
#include "sql/key.h"
#include "sql/sql_class.h"
#include "sql/sql_executor.h"
#include "sql/sql_lex.h"
#include "sql/sql_optimizer.h"
#include "sql/table.h"
#include "template_utils.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/descriptor/index_context.h"
#include "villagesql/schema/descriptor/index_profile_descriptor.h"
#include "villagesql/schema/descriptor/index_type_descriptor.h"
#include "villagesql/schema/systable/helpers.h"
#include "villagesql/schema/victionary_client.h"
#include "villagesql/sql/custom_index_runtime.h"

namespace villagesql {
namespace {

// Per-query KNN scan spec. Allocated on thd->mem_root by
// CollectCustomKnnOrderingsForHypergraph and threaded through
// SpatialDistanceScanInfo::custom_scan_spec →
// AccessPath::index_distance_scan().custom_scan_spec →
// CreateCustomHypergraphDistanceIterator. No process-wide registry.
struct CustomHypergraphDistanceScanSpec {
  TABLE *table = nullptr;
  const unsigned char *query_key = nullptr;
  uint32_t query_key_len = 0;
  uint32_t limit = 0;
};

// Returns the udf if it names an index-profile-bound VDF that could drive
// a KNN scan; nullptr otherwise. Recognition consults
// index_profile_descriptors in the victionary: any two-argument VDF that
// appears in some profile's functions() bindings is a candidate. Downstream
// checks (FindCustomKnnIndexOnField) confirm that a matching KNN-capable
// custom index exists on the field.
// TODO(villagesql-indexing): tighten by validating the profile's type_ref
// matches the field's column type and the profile's index_type_ref matches
// the discovered index. Currently split across GetMVectorDistanceFunction
// (VDF recognition) and FindCustomKnnIndexOnField (index/column matching).
Item_udf_func *GetMVectorDistanceFunction(Item *item) {
  if (item->type() != Item::FUNC_ITEM) return nullptr;

  Item_func *const item_func = down_cast<Item_func *>(item);
  if (item_func->functype() != Item_func::UDF_FUNC || item_func->arg_count != 2)
    return nullptr;

  auto *udf = down_cast<Item_udf_func *>(item_func);
  if (!udf->is_vdf()) return nullptr;

  const char *qualified_name = udf->qualified_name();
  if (qualified_name == nullptr) return nullptr;

  VictionaryClient &vclient = VictionaryClient::instance();
  if (!vclient.is_initialized()) return nullptr;

  auto read_lock = vclient.get_read_lock();
  for (const IndexProfileDescriptor *profile :
       vclient.index_profile_descriptors().get_all_committed()) {
    const std::string &ext_name = profile->extension_name();
    for (const vef_index_profile_fn_binding_t &fn : profile->functions()) {
      if (fn.name == nullptr) continue;
      const std::string bound = make_qualified_base_name(ext_name, fn.name);
      if (my_strcasecmp(system_charset_info, qualified_name, bound.c_str()) ==
          0) {
        return udf;
      }
    }
  }
  return nullptr;
}

bool GetFieldAndQueryFromMVectorDistance(Item_udf_func *distance_func,
                                         TABLE *table, Item_field **field_item,
                                         Item **query_item) {
  Item *arg0 = distance_func->arguments()[0]->real_item();
  Item *arg1 = distance_func->arguments()[1]->real_item();

  if (arg0->type() == Item::FIELD_ITEM) {
    auto *candidate = down_cast<Item_field *>(arg0);
    if (candidate->field != nullptr && candidate->field->table == table &&
        arg1->const_item()) {
      *field_item = candidate;
      *query_item = arg1;
      return false;
    }
  }

  if (arg1->type() == Item::FIELD_ITEM) {
    auto *candidate = down_cast<Item_field *>(arg1);
    if (candidate->field != nullptr && candidate->field->table == table &&
        arg0->const_item()) {
      *field_item = candidate;
      *query_item = arg0;
      return false;
    }
  }

  return true;
}

// Walks TABLE::key_info[] looking for a single-column KNN-capable custom
// index on `field`. Uses KEY::custom_index_context (populated by
// MaybeInjectCustomIndex at table-open time) as the marker and source of
// the IndexTypeDescriptor. Returns the key_info[] slot as *key_idx; true on
// no match.
bool FindCustomKnnIndexOnField(TABLE *table, Field *field, uint *key_idx) {
  for (uint i = 0; i < table->s->keys; ++i) {
    const KEY &keyinfo = table->key_info[i];
    if (keyinfo.custom_index_context == nullptr) continue;

    const IndexTypeDescriptor *descriptor =
        keyinfo.custom_index_context->descriptor();
    if (descriptor == nullptr ||
        !(descriptor->intf().capabilities & VEF_INDEX_CAP_KNN)) {
      continue;
    }

    if (keyinfo.user_defined_key_parts != 1) continue;
    if (keyinfo.key_part[0].field != field) continue;

    *key_idx = i;
    return false;
  }

  return true;
}

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
    if (!table()->file->inited) {
      error = table()->file->ha_index_init(table()->s->primary_key, false);
      if (error) {
        PrintError(error);
        return true;
      }
    }

    if (set_record_buffer(table(), m_expected_rows)) {
      return true;
    }

    char error_msg[512]{};
    if (custom_index_knn_scan_begin(table(), table()->key_info[m_key_idx].name,
                                    m_spec->query_key, m_spec->query_key_len,
                                    m_spec->limit, &m_scan, error_msg,
                                    sizeof(error_msg))) {
      LogVSQL(ERROR_LEVEL, "Failed to begin custom KNN scan: %s", error_msg);
      PrintError(HA_ERR_INTERNAL_ERROR);
      return true;
    }
    return false;
  }

  int Read() override {
    for (;;) {
      const unsigned char *pkey_data = nullptr;
      uint32_t pkey_len = 0;
      bool eof = false;
      char error_msg[512]{};
      if (custom_index_knn_scan_next(m_scan, &pkey_data, &pkey_len, &eof,
                                     error_msg, sizeof(error_msg))) {
        LogVSQL(ERROR_LEVEL, "Failed to read custom KNN scan: %s", error_msg);
        return HandleError(HA_ERR_INTERNAL_ERROR);
      }
      if (eof) return -1;

      const KEY &primary_key = table()->s->key_info[table()->s->primary_key];
      if (pkey_len != primary_key.key_length) {
        return HandleError(HA_ERR_INTERNAL_ERROR);
      }

      const int error = table()->file->ha_index_read_map(
          m_record, pkey_data, HA_WHOLE_KEY, HA_READ_KEY_EXACT);
      if (error == HA_ERR_KEY_NOT_FOUND || error == HA_ERR_END_OF_FILE) {
        return HandleError(HA_ERR_INTERNAL_ERROR);
      }
      if (error) return HandleError(error);
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

void CollectCustomKnnOrderingsForHypergraph(
    THD *thd, Query_block *query_block, TABLE *table,
    LogicalOrderings *orderings,
    Mem_root_array<SpatialDistanceScanInfo> *spatial_indexes) {
  if (query_block->join == nullptr ||
      query_block->join->m_select_limit == HA_POS_ERROR) {
    return;
  }

  for (int i = 1; i < orderings->num_items(); ++i) {
    Item_udf_func *distance_func =
        GetMVectorDistanceFunction(orderings->item(i));
    if (distance_func == nullptr) continue;

    Item_field *field_item = nullptr;
    Item *query_item = nullptr;
    if (GetFieldAndQueryFromMVectorDistance(distance_func, table, &field_item,
                                            &query_item)) {
      continue;
    }

    uint key_idx = 0;
    if (FindCustomKnnIndexOnField(table, field_item->field, &key_idx)) {
      continue;
    }

    String query_buffer;
    String *query_value = query_item->val_str(&query_buffer);
    if (query_value == nullptr || query_item->null_value ||
        query_value->length() == 0) {
      continue;
    }

    auto *query_key = pointer_cast<unsigned char *>(
        memdup_root(thd->mem_root, query_value->ptr(), query_value->length()));
    if (query_key == nullptr) continue;

    auto *spec = new (thd->mem_root) CustomHypergraphDistanceScanSpec;
    if (spec == nullptr) continue;
    spec->table = table;
    spec->query_key = query_key;
    spec->query_key_len = static_cast<uint32_t>(query_value->length());
    spec->limit = static_cast<uint32_t>(
        std::min<ha_rows>(query_block->join->m_select_limit,
                          std::numeric_limits<uint32_t>::max()));

    SpatialDistanceScanInfo index_info;
    index_info.table = table;
    index_info.key_idx = static_cast<int>(key_idx);
    index_info.is_custom_index = true;
    index_info.custom_scan_spec = spec;

    OrderElement order_element{i, ORDER_ASC};
    Ordering::Elements elements{&order_element, 1};
    index_info.forward_order = orderings->AddOrdering(
        thd, Ordering(elements, Ordering::Kind::kOrder),
        /*interesting=*/false, /*used_at_end=*/true, /*homogenize_tables=*/0);
    spatial_indexes->push_back(index_info);
  }
}

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
