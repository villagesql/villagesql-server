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
#include "sql/sql_select.h"
#include "sql/table.h"

namespace villagesql {

IndexKnnScanIterator::IndexKnnScanIterator(THD *thd, TABLE *table, int idx,
                                           Item *query_point, ha_rows limit,
                                           bool limit_is_exact,
                                           double expected_rows,
                                           ha_rows *examined_rows)
    : TableRowIterator(thd, table),
      m_record(table->record[0]),
      m_idx(idx),
      m_query_point(query_point),
      m_limit(limit),
      m_limit_is_exact(limit_is_exact),
      m_expected_rows(expected_rows),
      m_examined_rows(examined_rows) {}

bool IndexKnnScanIterator::Init() {
  if (!table()->file->inited) {
    // Serialize m_query_point into a key image.
    const KEY_PART_INFO *key_part = table()->key_info[m_idx].key_part;
    const uint maybe_null = key_part->null_bit ? 1 : 0;
    uchar *key_buff =
        thd()->mem_root->ArrayAlloc<uchar>(key_part->store_length);
    if (key_buff == nullptr) return true;
    store_key key_copier(thd(), key_part->field, key_buff + maybe_null,
                         maybe_null ? key_buff : nullptr, key_part->length,
                         m_query_point);
    if (key_copier.copy() == store_key::STORE_KEY_FATAL) return true;

    int error = table()->file->ha_knn_index_init(
        m_idx, key_buff, key_part->store_length, m_limit, m_limit_is_exact);
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

int IndexKnnScanIterator::Read() {  // Forward read.
  int error;
  if (m_first) {
    error = table()->file->ha_index_first(m_record);
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

}  // namespace villagesql
