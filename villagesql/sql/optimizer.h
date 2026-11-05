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

#include <cstdint>
#include <limits>

#include "my_base.h"
#include "sql/sql_bitmap.h"

struct AccessPath;
class Item;
class JOIN_TAB;
class ORDER_with_src;
class THD;
struct KnnScanInfo;
struct ORDER;
struct TABLE;

namespace villagesql {
namespace optimizer {

// The largest limit of a KNN scan. The scan descriptor holds the limit in 32
// bits, so the optimizer does not choose a KNN scan for a larger limit.
constexpr ha_rows MAX_KNN_LIMIT = std::numeric_limits<uint32_t>::max();

// Check whether `order` can be satisfied by a KNN scan on a KNN-capable index
// in `map` of `table`, subject to the LIMIT `select_limit` (HA_POS_ERROR if
// none), the isolation level and the binlog format. Does not depend on a JOIN,
// so it is also used by single-table UPDATE and DELETE. On success, sets
// `*key_nr` to the index number and `*query_point` to the constant argument
// of the ordering function, and returns true.
bool find_knn_order_key(THD *thd, TABLE *table, const ORDER *order,
                        const Key_map *map, ha_rows select_limit, int *key_nr,
                        Item **query_point);

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

// Single-table UPDATE/DELETE variant of try_knn_order(), called from
// get_index_for_order(). Does nothing if `knn_scan_info` is nullptr or a range
// scan was chosen. On success, fills `*knn_scan_info` so that the caller can
// build a KNN scan, sets `*key` to the index number and returns true.
bool try_knn_order(TABLE *table, ORDER_with_src *order, ha_rows limit,
                   const AccessPath *range_scan, KnnScanInfo *knn_scan_info,
                   uint *key);

// Creates the KNN scan access path for index `key` from the `knn_scan_info`
// filled by try_knn_order(), for both the JOIN executor and single-table
// UPDATE/DELETE.
AccessPath *new_knn_scan_path(THD *thd, TABLE *table, uint key,
                              const KnnScanInfo *knn_scan_info,
                              bool count_examined_rows);

// Returns true if `item` is, after unwrapping refs, a call to a VDF declared
// deterministic by the extension author. Such VDFs are potential candidates
// for KNN index optimization.
bool is_deterministic_vdf(const Item *item);

// Removes custom indexes from `table->keys_in_use_for_query` so that ref,
// range and index merge access do not consider them for WHERE predicates.
// If `all` is true (hypergraph optimizer, which does not support custom
// indexes), every custom index is removed. Otherwise only those that do not
// advertise HA_READ_RANGE are removed. Under SERIALIZABLE, which a custom index
// cannot support, every custom index is removed from all three key maps.
// Otherwise the order-by and group-by key maps are left untouched: ordering is
// governed by HA_READ_ORDER (via Field::part_of_sortkey) and KNN ordering.
// Must be called after index hints have been applied.
void disable_custom_keys_for_query(TABLE *table, bool all);

}  // namespace optimizer
}  // namespace villagesql

#endif  // VILLAGESQL_SQL_OPTIMIZER_H_
