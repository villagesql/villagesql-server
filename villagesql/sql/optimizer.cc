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

#include "villagesql/sql/optimizer.h"

#include "sql/field.h"
#include "sql/handler.h"
#include "sql/item.h"
#include "sql/item_func.h"
#include "sql/key.h"
#include "sql/sql_optimizer.h"
#include "sql/sql_select.h"
#include "sql/table.h"
#include "template_utils.h"
#include "villagesql/schema/descriptor/index_context.h"
#include "villagesql/schema/descriptor/index_profile_descriptor.h"
#include "villagesql/schema/descriptor/index_type_descriptor.h"
#include "villagesql/sdk/include/villagesql/abi/preview/index.h"

namespace villagesql {
namespace optimizer {

namespace {

// Checks that `item` is a candidate KNN ordering expression -- a
// deterministic VDF, returning REAL, called with exactly one field reference
// and one constant argument, like any other predicate. On success, returns
// the VDF call and sets *field_arg to the field-referencing argument.
const Item_udf_func *knn_order_call(Item *item, Item_field **field_arg) {
  if (!is_deterministic_vdf(item)) return nullptr;
  const auto *func = down_cast<const Item_udf_func *>(item->real_item());
  if (func->result_type() != REAL_RESULT) return nullptr;
  if (func->argument_count() != 2) return nullptr;

  Item_field *field = nullptr;
  for (uint i = 0; i < func->argument_count(); ++i) {
    Item *arg = func->arguments()[i]->real_item();
    if (arg->type() == Item::FIELD_ITEM) {
      if (field != nullptr) return nullptr;  // Only one field ref allowed.
      field = down_cast<Item_field *>(arg);
    } else if (!arg->const_item()) {
      return nullptr;  // The other argument must be constant.
    }
  }
  if (field == nullptr) return nullptr;
  *field_arg = field;
  return func;
}

// Returns the profile function binding that `order_call` was
// registered from, or nullptr if `profile` does not declare it.
const vef_index_profile_fn_binding_t *find_profile_function(
    const IndexProfileDescriptor *profile, const Item_udf_func *order_call) {
  const vef_func_desc_t *desc = order_call->vdf_func_desc();
  if (desc == nullptr) return nullptr;
  for (const auto &binding : profile->functions()) {
    if (binding.vdf == desc->vdf) return &binding;
  }
  return nullptr;
}

// Finds a candidate key (from `map`) on `field`'s table that is
// backed by a KNN-capable custom index type whose profile for this key
// declares `order_call` as one of its functions. Returns the key number, or
// -1 if no such key exists.
int find_knn_capable_key(const Field *field, const Key_map *map,
                         const Item_udf_func *order_call) {
  const TABLE *table = field->table;
  for (uint key_nr = 0; key_nr < table->s->keys; ++key_nr) {
    if (!map->is_set(key_nr)) continue;
    const KEY &key = table->key_info[key_nr];
    if (key.key_part[0].field != field) continue;

    // The custom index type must advertise KNN scan support, and the
    // storage engine backing it must be able to actually execute one.
    if (!(table->file->index_flags(key_nr, 0, true) & HA_SUPPORT_KNN)) continue;

    if (key.custom_index_context == nullptr) continue;

    const IndexProfileDescriptor *profile =
        key.key_part[0].custom_index_profile;
    if (profile == nullptr || !profile->ordering_asc()) continue;
    if (find_profile_function(profile, order_call) == nullptr) continue;

    return key_nr;
  }
  return -1;
}

}  // namespace

bool is_deterministic_vdf(const Item *item) {
  item = item->real_item();
  if (item->type() != Item::FUNC_ITEM) return false;

  const auto *func = down_cast<const Item_func *>(item);
  if (func->functype() != Item_func::UDF_FUNC) return false;

  return down_cast<const Item_udf_func *>(func)->is_vdf_deterministic();
}

bool try_knn_order(JOIN_TAB *tab, ORDER_with_src &order, int ref_key,
                   const Key_map *map, ha_rows select_limit, bool no_changes,
                   int *order_idx) {
  // KNN requires a limit.
  if (select_limit == HA_POS_ERROR) return false;

  // TODO(villagesql-indexing): Like test_if_skip_sort_order(), call
  // test_quick_select() to choose the right plan when an index scan is
  // already set, instead of giving up on KNN ordering entirely.
  if (ref_key >= 0) return false;

  // ORDER direction must be ASC, and there must be a single ORDER BY
  // expression: a KNN scan produces one ranked order, with no secondary
  // sort key.
  if (order.order == nullptr || order.order->next != nullptr) return false;
  if (order.order->direction == ORDER_DESC) return false;

  // The ORDER BY expression must be a deterministic VDF call with
  // one field reference and one constant argument, returning REAL.
  Item_field *field_arg = nullptr;
  const auto *order_call = knn_order_call(*order.order->item, &field_arg);
  if (order_call == nullptr) return false;

  // The ordered field must have a KNN-capable index whose profile
  // declares `order_call` as one of its functions.
  const int key_nr = find_knn_capable_key(field_arg->field, map, order_call);
  if (key_nr < 0) return false;

  if (no_changes) return true;

  // 3. Use an index scan on the KNN-capable index instead of a filesort.
  // TODO(villagesql-indexing): Actual KNN-ordered retrieval (honoring the
  // constant argument of `order_call` as the query point, and pushing
  // `select_limit` down as K) is not wired into the storage engine read
  // path yet -- see storage/innobase/villagesql/custom_index.cc and
  // vef_index_scan_desc_t/VEF_INDEX_SCAN_TYPE_KNN in abi/preview/index.h.
  // This only records the plan decision at the SQL layer.
  tab->set_index(key_nr);
  tab->set_type(JT_INDEX_SCAN);
  *order_idx = key_nr;
  return true;
}

}  // namespace optimizer
}  // namespace villagesql
