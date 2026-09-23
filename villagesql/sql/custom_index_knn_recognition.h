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

#ifndef VILLAGESQL_SQL_CUSTOM_INDEX_KNN_RECOGNITION_H_
#define VILLAGESQL_SQL_CUSTOM_INDEX_KNN_RECOGNITION_H_

// Common, optimizer-agnostic KNN-recognition core: given an ORDER BY item,
// decide whether a KNN-capable custom index can serve it, and materialize the
// per-query scan spec. An optimizer integration (see the per-optimizer
// custom_index_knn_optimizer_*.cc units) builds on top of this; nothing here is
// tied to a particular optimizer.

#include "my_base.h"  // ha_rows

class Item;
class THD;
struct TABLE;

namespace villagesql {

struct CustomKnnDistanceScanSpec;  // custom_index_knn_scan.h

// True (i.e. "recognized") if `order_item` is a KNN distance function over a
// KNN-capable custom index on `table` whose selected profile matches the
// distance metric. On a match returns false and fills *key_idx (the key_info[]
// slot) and *query_item_out (the constant query-vector argument). Pure: no plan
// changes, no allocation.
bool RecognizeKnnOrderItem(TABLE *table, Item *order_item, uint *key_idx,
                           Item **query_item_out);

// Materializes a CustomKnnDistanceScanSpec on thd->mem_root from a recognized
// query vector (`query_item`, the const arg returned by RecognizeKnnOrderItem).
// Returns nullptr on failure (null/empty query, OOM).
CustomKnnDistanceScanSpec *BuildKnnScanSpec(THD *thd, TABLE *table,
                                            Item *query_item,
                                            ha_rows select_limit);

}  // namespace villagesql

#endif  // VILLAGESQL_SQL_CUSTOM_INDEX_KNN_RECOGNITION_H_
