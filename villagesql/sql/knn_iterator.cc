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

#include "villagesql/sql/knn_iterator.h"

#include "sql/handler.h"
#include "sql/sql_executor.h"
#include "sql/table.h"

namespace villagesql {

IndexKnnScanIterator::IndexKnnScanIterator(THD *thd, TABLE *table, int idx,
                                           Item *query_point, ha_rows limit,
                                           double expected_rows,
                                           ha_rows *examined_rows)
    : TableRowIterator(thd, table),
      m_record(table->record[0]),
      m_idx(idx),
      m_query_point(query_point),
      m_limit(limit),
      m_expected_rows(expected_rows),
      m_examined_rows(examined_rows) {}

bool IndexKnnScanIterator::Init() {
  int error = table()->file->ha_index_init(m_idx, true);
  if (error) {
    PrintError(error);
    return true;
  }
  return set_record_buffer(table(), m_expected_rows);
}

int IndexKnnScanIterator::Read() { return HandleError(HA_ERR_UNSUPPORTED); }

}  // namespace villagesql
