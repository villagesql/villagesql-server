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

#include "villagesql/sql/custom_index_knn_optimizer.h"

#include <algorithm>
#include <limits>
#include <string>
#include <vector>

#include "my_base.h"
#include "my_sys.h"
#include "mysql/strings/m_ctype.h"
#include "sql/field.h"
#include "sql/item.h"
#include "sql/item_func.h"
#include "sql/join_optimizer/build_interesting_orders.h"
#include "sql/join_optimizer/interesting_orders.h"
#include "sql/key.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_optimizer.h"
#include "sql/table.h"
#include "template_utils.h"
#include "villagesql/schema/descriptor/index_context.h"
#include "villagesql/schema/descriptor/index_profile_descriptor.h"
#include "villagesql/schema/descriptor/index_type_descriptor.h"
#include "villagesql/schema/systable/helpers.h"
#include "villagesql/schema/victionary_client.h"
#include "villagesql/sql/custom_index_knn_scan.h"

namespace villagesql {
namespace {

// Returns the udf if it names an index-profile-bound VDF that could drive
// a KNN scan; nullptr otherwise. Recognition consults
// index_profile_descriptors in the victionary: any two-argument VDF that
// appears in some profile's functions() bindings is a candidate. Downstream
// checks (FindCustomKnnIndexOnField) confirm that a matching KNN-capable
// custom index exists on the field.
// The profile binding for the specific index — the {index type, distance
// function} pairing chosen at CREATE INDEX time — is verified downstream in
// FindCustomKnnIndexOnField against KEY_PART_INFO::custom_index_profile.
// TODO(villagesql-indexing): also validate the profile's type_ref matches the
// field's column type.
Item_udf_func *GetCustomKnnDistanceFunction(Item *item) {
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

bool GetFieldAndQueryFromKnnDistance(Item_udf_func *distance_func, TABLE *table,
                                     Item_field **field_item,
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

// True if `profile` binds `qualified_fn_name` (the ORDER BY distance function)
// in one of its function bindings — i.e. this specific index answers this
// metric. The profile chosen at CREATE INDEX time (e.g. hnsw_cosine vs the
// default hnsw_l2) is what ties {data type, index type, function(s)} together:
// several profiles can share one index type (an extension may register hnsw_l2
// and hnsw_cosine both on index type "hnsw"), so checking the index type is
// not enough. Without matching the profile itself, a query ordering by one
// metric (cosine) could be answered by an index built for a different metric
// (L2), silently returning the wrong order.
bool ProfileBindsFunction(const char *qualified_fn_name,
                          const IndexProfileDescriptor &profile) {
  const std::string &ext_name = profile.extension_name();
  for (const vef_index_profile_fn_binding_t &fn : profile.functions()) {
    if (fn.name == nullptr) continue;
    const std::string bound = make_qualified_base_name(ext_name, fn.name);
    if (my_strcasecmp(system_charset_info, qualified_fn_name, bound.c_str()) ==
        0) {
      return true;
    }
  }
  return false;
}

// Walks TABLE::key_info[] looking for a single-column KNN-capable custom
// index on `field` whose selected profile actually binds `qualified_fn_name`
// (the ORDER BY distance function). Uses KEY::custom_index_context (populated
// by MaybeInjectCustomIndex at table-open time) as the marker and source of
// the IndexTypeDescriptor, and KEY_PART_INFO::custom_index_profile (resolved
// from the profile named at CREATE INDEX time) as the metric discriminator.
// Returns the key_info[] slot as *key_idx; true on no match.
bool FindCustomKnnIndexOnField(TABLE *table, Field *field,
                               const char *qualified_fn_name, uint *key_idx) {
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

    // The index must be KNN-capable (checked above) AND the profile chosen for
    // this key part must bind the specific ORDER BY function. Both are
    // required: capability says "this index can do KNN scans at all", the
    // profile binding says "and it answers this metric". The profile — not the
    // index type — is the discriminator, since one index type can back several
    // metric profiles.
    const IndexProfileDescriptor *profile =
        keyinfo.key_part[0].custom_index_profile;
    if (profile == nullptr) continue;
    if (!ProfileBindsFunction(qualified_fn_name, *profile)) continue;

    *key_idx = i;
    return false;
  }

  return true;
}

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
        GetCustomKnnDistanceFunction(orderings->item(i));
    if (distance_func == nullptr) continue;

    Item_field *field_item = nullptr;
    Item *query_item = nullptr;
    if (GetFieldAndQueryFromKnnDistance(distance_func, table, &field_item,
                                        &query_item)) {
      continue;
    }

    const char *qualified_fn_name = distance_func->qualified_name();
    if (qualified_fn_name == nullptr) continue;

    uint key_idx = 0;
    if (FindCustomKnnIndexOnField(table, field_item->field, qualified_fn_name,
                                  &key_idx)) {
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
    // A non-null spec marks this as the custom-index variant of the scan.
    index_info.custom_scan_spec = spec;

    OrderElement order_element{i, ORDER_ASC};
    Ordering::Elements elements{&order_element, 1};
    index_info.forward_order = orderings->AddOrdering(
        thd, Ordering(elements, Ordering::Kind::kOrder),
        /*interesting=*/false, /*used_at_end=*/true, /*homogenize_tables=*/0);
    spatial_indexes->push_back(index_info);
  }
}

}  // namespace villagesql
