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

#ifndef VILLAGESQL_SQL_OPTIMIZER_H_
#define VILLAGESQL_SQL_OPTIMIZER_H_

#include "my_base.h"
#include "sql/sql_bitmap.h"

class Item;
class JOIN_TAB;
class ORDER_with_src;

namespace villagesql {
namespace optimizer {

// Check whether a KNN-capable index can satisfy the requested `ORDER BY`
// without requiring filesort. `ref_key` identifies the index currently
// selected for ref/range/index access on `tab`, or -1 if no such index is
// selected. `map` contains the indexes that are candidates for satisfying
// the requested ordering. `select_limit` is the number of rows required by
// the query's LIMIT, or HA_POS_ERROR if there is no LIMIT. If `no_changes`
// is true, do not modify the current access plan, consistent with
// test_if_skip_sort_order(). If a suitable KNN index is found, set
// `*order_idx` to its index number and return true; otherwise return false.
bool try_knn_order(JOIN_TAB *tab, ORDER_with_src &order, int ref_key,
                   const Key_map *map, ha_rows select_limit, bool no_changes,
                   int *order_idx);

// Returns true if `item` is, after unwrapping refs, a call to a VDF declared
// deterministic by the extension author. Such VDFs are potential candidates
// for KNN index optimization.
bool is_deterministic_vdf(const Item *item);

}  // namespace optimizer
}  // namespace villagesql

#endif  // VILLAGESQL_SQL_OPTIMIZER_H_
