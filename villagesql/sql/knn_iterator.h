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

#ifndef VILLAGESQL_SQL_KNN_ITERATOR_H_
#define VILLAGESQL_SQL_KNN_ITERATOR_H_

#include "my_base.h"
#include "my_inttypes.h"
#include "sql/iterators/row_iterator.h"

class Item;
class THD;
struct TABLE;

namespace villagesql {

// Perform a K-Nearest-Neighbor (KNN) scan along a VillageSQL custom index,
// ordering rows by increasing distance from m_query_point and stopping after
// m_limit rows.
class IndexKnnScanIterator final : public TableRowIterator {
 public:
  // "expected_rows" is used for scaling the record buffer.
  // If zero or less, no record buffer will be set up.
  // "examined_rows", if not nullptr, is incremented for each successful Read().
  IndexKnnScanIterator(THD *thd, TABLE *table, int idx, Item *query_point,
                       ha_rows limit, bool limit_is_exact, double expected_rows,
                       ha_rows *examined_rows);

  bool Init() override;
  int Read() override;

 private:
  uchar *const m_record;
  const int m_idx;
  Item *const m_query_point;
  const ha_rows m_limit;
  const bool m_limit_is_exact;
  const double m_expected_rows;
  ha_rows *const m_examined_rows;
  bool m_first = true;
};

}  // namespace villagesql

#endif  // VILLAGESQL_SQL_KNN_ITERATOR_H_
