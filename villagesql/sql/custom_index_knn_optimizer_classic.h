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

#ifndef VILLAGESQL_SQL_CUSTOM_INDEX_KNN_OPTIMIZER_CLASSIC_H_
#define VILLAGESQL_SQL_CUSTOM_INDEX_KNN_OPTIMIZER_CLASSIC_H_

// Classic-optimizer integration for the KNN custom-index distance scan. This is
// the single header the classic optimizer (sql/) includes to reach the
// VillageSQL KNN addon: recognition, the sort-skip hook, and the access-path
// builder. Independent of the hypergraph header
// (custom_index_knn_optimizer_hypergraph.h).

#include "my_base.h"  // ha_rows

class Item;
class JOIN_TAB;
class THD;
struct AccessPath;
struct ORDER;
struct TABLE;

namespace villagesql {

// True if `order_item` is a KNN distance function over a KNN-capable custom
// index on `table` (a single ORDER-BY term this scan could serve). Used by the
// classic optimizer to keep such an ordering eligible for index-order
// optimization: a distance UDF is otherwise disqualified as "expensive" and
// forced into a filesort before test_if_skip_sort_order (and the KNN
// recognition hook) ever runs. Pure predicate, no plan changes.
bool IsCustomKnnDistanceOrderItem(TABLE *table, Item *order_item);

// Classic optimizer hook. Returns true if a KNN-capable custom index on
// `tab`'s table satisfies ORDER BY <distance>(col, const) LIMIT k — meaning the
// filesort can be skipped. `order` is the query block's ORDER BY list (must be
// a single ASC distance term). The LIMIT is read from tab->join()'s query
// block (JOIN::m_select_limit is not yet populated at this phase for a simple
// SELECT). When it applies and `no_changes` is false, marks the table
// JT_INDEX_DISTANCE + sets the chosen index so QEP_TAB::access_path() builds
// the distance scan; with `no_changes` true it only reports applicability
// (probe mode). Called from test_if_skip_sort_order().
bool TrySkipSortWithCustomKnnIndex(JOIN_TAB *tab, ORDER *order,
                                   bool no_changes);

// Classic optimizer access-path builder. Rebuilds the KNN scan spec for the
// index `key_idx` chosen by TrySkipSortWithCustomKnnIndex from the query
// block's ORDER BY (`order`, single ASC distance term) and returns an
// AccessPath::INDEX_DISTANCE_SCAN driving CreateCustomKnnDistanceIterator, or
// nullptr on failure. Called from QEP_TAB::access_path() for JT_INDEX_DISTANCE.
AccessPath *BuildCustomKnnDistanceAccessPath(THD *thd, TABLE *table,
                                             uint key_idx, ORDER *order,
                                             ha_rows select_limit);

}  // namespace villagesql

#endif  // VILLAGESQL_SQL_CUSTOM_INDEX_KNN_OPTIMIZER_CLASSIC_H_
