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

#include "villagesql/sql/custom_index_runtime.h"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <functional>
#include <map>
#include <string>
#include <vector>

#include "my_base.h"
#include "mysql/strings/m_ctype.h"
#include "sql/field.h"
#include "sql/handler.h"
#include "sql/key.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/table.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/descriptor/index_type_descriptor.h"
#include "villagesql/schema/schema_manager.h"
#include "villagesql/schema/systable/custom_index_columns.h"
#include "villagesql/schema/systable/custom_indexes.h"
#include "villagesql/schema/victionary_client.h"
#include "villagesql/sql/custom_index_runtime_internal.h"

namespace villagesql {

// Reads the committed index columns. The scan path always reads committed
// columns, so thd is unused here. Declared in custom_index_runtime_internal.h
// so custom_index_knn_scan.cc can call it.
//
// TODO(villagesql-indexing): add the rebuild variant (thd != nullptr, reading
// uncommitted columns via custom_index_columns().get_prefix) needed by
// ALTER-rebuild maintenance.
bool get_custom_index_columns(THD * /*thd*/, uint64_t index_id,
                              std::vector<std::string> *out) {
  VictionaryClient &vclient = VictionaryClient::instance();
  std::vector<const IndexColumnEntry *> columns =
      vclient.GetColumnsForIndex(/*thd=*/nullptr, index_id);
  std::sort(columns.begin(), columns.end(),
            [](const IndexColumnEntry *a, const IndexColumnEntry *b) {
              return a->key_position() < b->key_position();
            });
  out->clear();
  out->reserve(columns.size());
  for (const IndexColumnEntry *column : columns)
    out->push_back(column->column_name);
  return false;
}

}  // namespace villagesql
