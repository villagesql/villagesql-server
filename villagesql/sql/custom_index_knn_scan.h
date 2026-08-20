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

#ifndef VILLAGESQL_SQL_CUSTOM_INDEX_KNN_SCAN_H_
#define VILLAGESQL_SQL_CUSTOM_INDEX_KNN_SCAN_H_

// KNN scan execution for custom indexes: the row iterator that drives a custom
// distance-scan index through the standard handler index-scan verbs
// (ha_index_init / ha_index_read_map with HA_READ_NEAREST_NEIGHBOR /
// ha_index_next), mirroring the stock IndexDistanceScanIterator. The optimizer
// (custom_index_knn_optimizer.cc) recognizes the scan and fills a
// CustomHypergraphDistanceScanSpec; this unit consumes it at execution time.

#include <cstdint>

#include "my_alloc.h"
#include "my_base.h"

class RowIterator;
class THD;
struct MEM_ROOT;
struct TABLE;

namespace villagesql {

// Per-query KNN scan spec. Allocated on thd->mem_root by
// CollectCustomKnnOrderingsForHypergraph and threaded through
// SpatialDistanceScanInfo::custom_scan_spec →
// AccessPath::index_distance_scan().custom_scan_spec →
// CreateCustomHypergraphDistanceIterator. No process-wide registry.
//
// query_key is the encoded query vector -- the value of the single indexed key
// column -- handed to ha_index_read_map as the nearest-neighbour seek key. The
// LIMIT that bounds the KNN result is applied by the LIMIT operator above the
// scan (as for the spatial distance scan), so it is not carried here.
struct CustomHypergraphDistanceScanSpec {
  TABLE *table = nullptr;
  const unsigned char *query_key = nullptr;
  uint32_t query_key_len = 0;
};

// `custom_scan_spec` is the opaque pointer parked on
// AccessPath::index_distance_scan().custom_scan_spec.
unique_ptr_destroy_only<RowIterator> CreateCustomHypergraphDistanceIterator(
    THD *thd, MEM_ROOT *mem_root, TABLE *table, int key_idx,
    void *custom_scan_spec, double expected_rows, ha_rows *examined_rows);

}  // namespace villagesql

#endif  // VILLAGESQL_SQL_CUSTOM_INDEX_KNN_SCAN_H_
