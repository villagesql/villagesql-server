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

#ifndef VILLAGESQL_SQL_CUSTOM_INDEX_HYPERGRAPH_OPTIMIZER_H_
#define VILLAGESQL_SQL_CUSTOM_INDEX_HYPERGRAPH_OPTIMIZER_H_

#include <sys/types.h>

#include "my_alloc.h"
#include "my_base.h"

class LogicalOrderings;
class Query_block;
class RowIterator;
class THD;
template <class T>
class Mem_root_array;
struct MEM_ROOT;
struct SpatialDistanceScanInfo;
struct TABLE;

namespace villagesql {

void CollectCustomKnnOrderingsForHypergraph(
    THD *thd, Query_block *query_block, TABLE *table,
    LogicalOrderings *orderings,
    Mem_root_array<SpatialDistanceScanInfo> *spatial_indexes);

// `custom_scan_spec` is the opaque pointer parked on
// AccessPath::index_distance_scan().custom_scan_spec.
unique_ptr_destroy_only<RowIterator> CreateCustomHypergraphDistanceIterator(
    THD *thd, MEM_ROOT *mem_root, TABLE *table, int key_idx,
    void *custom_scan_spec, double expected_rows, ha_rows *examined_rows);

}  // namespace villagesql

#endif  // VILLAGESQL_SQL_CUSTOM_INDEX_HYPERGRAPH_OPTIMIZER_H_
