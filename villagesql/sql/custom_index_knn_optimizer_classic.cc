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

// Classic-optimizer integration for the KNN custom-index distance scan. The
// recognition hook (TrySkipSortWithCustomKnnIndex) marks the driving table
// JT_INDEX_DISTANCE during test_if_skip_sort_order(); the access-path builder
// (BuildCustomKnnDistanceAccessPath) is invoked from QEP_TAB::access_path().
// The optimizer-agnostic recognition lives in custom_index_knn_recognition.cc.

#include "villagesql/sql/custom_index_knn_optimizer_classic.h"

#include "my_base.h"
#include "sql/item.h"
#include "sql/join_optimizer/access_path.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_opt_exec_shared.h"
#include "sql/sql_optimizer.h"
#include "sql/table.h"
#include "villagesql/sql/custom_index_knn_recognition.h"
#include "villagesql/sql/custom_index_knn_scan.h"

namespace villagesql {

namespace {

// The single ORDER BY term a KNN scan can serve, or nullptr. Mirrors the
// classic optimizer's own gate for function orderings: a single, ascending
// ORDER BY item (the distance function). Multi-term or DESC orderings can't be
// served by the index's nearest-first traversal.
Item *SingleAscOrderItem(ORDER *order) {
  if (order == nullptr || order->next != nullptr) return nullptr;
  if (order->direction != ORDER_ASC) return nullptr;
  return *order->item;
}

}  // namespace

bool IsCustomKnnDistanceOrderItem(TABLE *table, Item *order_item) {
  if (table == nullptr || order_item == nullptr) return false;
  uint key_idx = 0;
  Item *query_item = nullptr;
  // RecognizeKnnOrderItem returns false (i.e. "recognized") on a match.
  return !RecognizeKnnOrderItem(table, order_item, &key_idx, &query_item);
}

bool TrySkipSortWithCustomKnnIndex(JOIN_TAB *tab, ORDER *order,
                                   bool no_changes) {
  // A KNN index order is only useful with a LIMIT; without one we'd walk the
  // whole graph outward. Read the limit from the query block rather than
  // JOIN::m_select_limit: for a simple (non-set-operation) SELECT, set_limit()
  // has not run at this optimize phase, so query_expression()->select_limit_cnt
  // (and thus m_select_limit) is still HA_POS_ERROR. query_block->get_limit()
  // is the authoritative parsed LIMIT here -- it's exactly what set_limit()
  // would read later. (The hypergraph path sees a populated m_select_limit
  // because it runs in a separate optimizer phase.)
  JOIN *const join = tab->join();
  if (join == nullptr || join->query_block == nullptr) return false;
  if (!join->query_block->has_limit()) return false;

  Item *order_item = SingleAscOrderItem(order);
  if (order_item == nullptr) return false;

  TABLE *table = tab->table();
  if (table == nullptr) return false;

  uint key_idx = 0;
  Item *query_item = nullptr;
  if (RecognizeKnnOrderItem(table, order_item, &key_idx, &query_item)) {
    return false;
  }

  // Mark the plan only when allowed to change it (test_if_skip_sort_order is
  // also called in probe mode). The distance scan uses no ref access, so ref is
  // left as-is (already -1 on this no-preceding-index path, mirroring FT's own
  // `assert(tab->ref().key == -1)` just above the FT branch). The query vector
  // is re-derived from this ORDER BY in BuildCustomKnnDistanceAccessPath, so no
  // spec is stashed on the plan here.
  if (!no_changes) {
    tab->set_type(JT_INDEX_DISTANCE);
    tab->set_index(key_idx);
  }
  return true;
}

AccessPath *BuildCustomKnnDistanceAccessPath(THD *thd, TABLE *table,
                                             uint key_idx, ORDER *order,
                                             ha_rows select_limit) {
  if (table == nullptr) return nullptr;

  // Re-derive the query vector from the ORDER BY the recognition step matched.
  // TrySkipSortWithCustomKnnIndex already validated single/ASC and set the
  // table's index to `key_idx`; re-running recognition here keeps the spec
  // build stateless (no plan-time stash) and confirms the item still resolves.
  Item *order_item = SingleAscOrderItem(order);
  if (order_item == nullptr) return nullptr;

  uint recognized_key = 0;
  Item *query_item = nullptr;
  if (RecognizeKnnOrderItem(table, order_item, &recognized_key, &query_item)) {
    return nullptr;
  }
  // The recognition and the chosen index must agree.
  if (recognized_key != key_idx) return nullptr;

  CustomKnnDistanceScanSpec *spec =
      BuildKnnScanSpec(thd, table, query_item, select_limit);
  if (spec == nullptr) return nullptr;

  AccessPath *path = new (thd->mem_root) AccessPath;
  if (path == nullptr) return nullptr;
  path->type = AccessPath::INDEX_DISTANCE_SCAN;
  path->count_examined_rows = true;
  path->index_distance_scan().table = table;
  path->index_distance_scan().idx = static_cast<int>(key_idx);
  path->index_distance_scan().custom_scan_spec = spec;
  // Custom-index variant carries no range (nearest-neighbor QUICK_RANGE is only
  // for the spatial variant).
  path->index_distance_scan().range = nullptr;
  return path;
}

}  // namespace villagesql
