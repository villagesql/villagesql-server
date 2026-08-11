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

// KNN scan execution for custom indexes: the scan-dispatch entry points that
// forward to the extension's scan callbacks on the loaded index, and the row
// iterator that drives them. The optimizer (custom_index_knn_optimizer.cc)
// recognizes the scan and fills a CustomHypergraphDistanceScanSpec; this unit
// consumes it at execution time.

#include <cstdint>

#include "my_alloc.h"
#include "my_base.h"

class RowIterator;
class THD;
struct MEM_ROOT;
struct TABLE;

namespace villagesql {

struct CustomIndexKnnScan;

// Per-query KNN scan spec. Allocated on thd->mem_root by
// CollectCustomKnnOrderingsForHypergraph and threaded through
// SpatialDistanceScanInfo::custom_scan_spec →
// AccessPath::index_distance_scan().custom_scan_spec →
// CreateCustomHypergraphDistanceIterator. No process-wide registry.
struct CustomHypergraphDistanceScanSpec {
  TABLE *table = nullptr;
  const unsigned char *query_key = nullptr;
  uint32_t query_key_len = 0;
  uint32_t limit = 0;
};

bool custom_index_knn_scan_begin(TABLE *table, uint key_idx,
                                 const char *index_name,
                                 const unsigned char *query_key,
                                 uint32_t query_key_len, uint32_t limit,
                                 CustomIndexKnnScan **scan, char *error_msg,
                                 uint32_t error_msg_len);
bool custom_index_knn_scan_next(CustomIndexKnnScan *scan, uint64_t *out_key_ref,
                                bool *eof, char *error_msg,
                                uint32_t error_msg_len);
void custom_index_knn_scan_end(CustomIndexKnnScan **scan);

// `custom_scan_spec` is the opaque pointer parked on
// AccessPath::index_distance_scan().custom_scan_spec.
unique_ptr_destroy_only<RowIterator> CreateCustomHypergraphDistanceIterator(
    THD *thd, MEM_ROOT *mem_root, TABLE *table, int key_idx,
    void *custom_scan_spec, double expected_rows, ha_rows *examined_rows);

}  // namespace villagesql

#endif  // VILLAGESQL_SQL_CUSTOM_INDEX_KNN_SCAN_H_
