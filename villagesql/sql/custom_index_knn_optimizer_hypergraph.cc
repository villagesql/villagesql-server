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

// Hypergraph-optimizer integration for the KNN custom-index distance scan.
// Registers ORDER BY <distance> LIMIT k orderings as distance-scan candidates
// so the hypergraph cost model can pick them. The optimizer-agnostic
// recognition lives in custom_index_knn_recognition.cc.

#include "villagesql/sql/custom_index_knn_optimizer_hypergraph.h"

#include "my_base.h"
#include "sql/item.h"
#include "sql/join_optimizer/build_interesting_orders.h"
#include "sql/join_optimizer/interesting_orders.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_optimizer.h"
#include "sql/table.h"
#include "villagesql/sql/custom_index_knn_recognition.h"
#include "villagesql/sql/custom_index_knn_scan.h"

namespace villagesql {

void CollectCustomKnnOrderingsForHypergraph(
    THD *thd, Query_block *query_block, TABLE *table,
    LogicalOrderings *orderings,
    Mem_root_array<SpatialDistanceScanInfo> *spatial_indexes) {
  if (query_block->join == nullptr ||
      query_block->join->m_select_limit == HA_POS_ERROR) {
    return;
  }

  for (int i = 1; i < orderings->num_items(); ++i) {
    uint key_idx = 0;
    Item *query_item = nullptr;
    if (RecognizeKnnOrderItem(table, orderings->item(i), &key_idx,
                              &query_item)) {
      continue;
    }

    CustomKnnDistanceScanSpec *spec = BuildKnnScanSpec(
        thd, table, query_item, query_block->join->m_select_limit);
    if (spec == nullptr) continue;

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
