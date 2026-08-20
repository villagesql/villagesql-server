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

// KNN scan execution for custom indexes.
//
// This is deliberately a near-clone of the stock IndexDistanceScanIterator
// (sql/iterators/basic_row_iterators.*): a custom KNN index is a distance-scan
// index, so at execution time it is driven exactly like the spatial nearest-
// neighbour scan -- through the standard handler index-scan verbs:
//
//   Init(): ha_index_init(key_idx)
//   Read(): first  -> ha_index_read_map(query_vector, keypart 0,
//                                        HA_READ_NEAREST_NEIGHBOR)
//           after  -> ha_index_next(record)
//
// The storage engine returns rows already in ascending-distance order; the
// LIMIT that bounds a KNN query is applied by the LIMIT operator ABOVE this
// scan, not pushed into the seek (again mirroring spatial). The engine owns the
// extension cursor and the col_ref -> row resolution internally; this iterator
// holds no extension state and makes no VEF calls.
//
// (Transitional: this stays a distinct iterator only because the classic vs
// hypergraph plumbing routes the custom variant here. Once ha_innobase serves
// HA_READ_NEAREST_NEIGHBOR for a custom index, this collapses into the stock
// IndexDistanceScanIterator and this file goes away.)

#include "villagesql/sql/custom_index_knn_scan.h"

#include "my_base.h"
#include "sql/handler.h"
#include "sql/iterators/row_iterator.h"
#include "sql/iterators/timing_iterator.h"
#include "sql/key.h"
#include "sql/sql_executor.h"
#include "sql/table.h"
#include "villagesql/include/error.h"

namespace villagesql {

namespace {

// Drives a custom KNN index through the standard handler index-scan verbs,
// mirroring IndexDistanceScanIterator. The query vector is the seek key
// (keypart 0) with HA_READ_NEAREST_NEIGHBOR; subsequent rows come from
// ha_index_next in distance order.
class CustomHypergraphDistanceIterator final : public TableRowIterator {
 public:
  CustomHypergraphDistanceIterator(THD *thd, TABLE *table, int key_idx,
                                   const CustomHypergraphDistanceScanSpec *spec,
                                   double expected_rows, ha_rows *examined_rows)
      : TableRowIterator(thd, table),
        m_record(table->record[0]),
        m_key_idx(key_idx),
        m_spec(spec),
        m_expected_rows(expected_rows),
        m_examined_rows(examined_rows) {}

  bool Init() override {
    if (!table()->file->inited) {
      int error = table()->file->ha_index_init(m_key_idx, /*sorted=*/true);
      if (error) {
        PrintError(error);
        return true;
      }
      if (set_record_buffer(table(), m_expected_rows)) {
        return true;
      }
    }
    m_first = true;
    return false;
  }

  int Read() override {
    int error;
    if (m_first) {
      // The query vector is the value of the single indexed key column
      // (keypart 0). HA_READ_NEAREST_NEIGHBOR asks the engine for rows in
      // ascending distance from it.
      error = table()->file->ha_index_read_map(
          m_record, m_spec->query_key, make_keypart_map(0),
          HA_READ_NEAREST_NEIGHBOR);
      m_first = false;
    } else {
      error = table()->file->ha_index_next(m_record);
    }

    if (error) return HandleError(error);
    if (m_examined_rows != nullptr) {
      ++*m_examined_rows;
    }
    return 0;
  }

 private:
  uchar *const m_record;
  const int m_key_idx;
  const CustomHypergraphDistanceScanSpec *const m_spec;
  const double m_expected_rows;
  ha_rows *const m_examined_rows;
  bool m_first = true;
};

}  // namespace

unique_ptr_destroy_only<RowIterator> CreateCustomHypergraphDistanceIterator(
    THD *thd, MEM_ROOT *mem_root, TABLE *table, int key_idx,
    void *custom_scan_spec, double expected_rows, ha_rows *examined_rows) {
  const auto *spec =
      static_cast<const CustomHypergraphDistanceScanSpec *>(custom_scan_spec);
  if (spec == nullptr || spec->table != table) {
    return unique_ptr_destroy_only<RowIterator>(nullptr);
  }
  return NewIterator<CustomHypergraphDistanceIterator>(
      thd, mem_root, table, key_idx, spec, expected_rows, examined_rows);
}

}  // namespace villagesql
