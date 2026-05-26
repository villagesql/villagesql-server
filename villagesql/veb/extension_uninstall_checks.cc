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

#include "villagesql/veb/extension_uninstall_checks.h"

#include <unordered_map>

#include "my_sys.h"

#include "villagesql/include/error.h"

namespace villagesql {

bool check_for_indexes_of_extension(
    const ExtensionEntry &ext_entry,
    const std::vector<const IndexEntry *> &all_indexes,
    const std::vector<const IndexColumnEntry *> &all_index_columns) {
  const IndexEntry *first_index = nullptr;
  int index_count = 0;

  for (const IndexEntry *entry : all_indexes) {
    if (entry->extension_name == ext_entry.extension_name() &&
        entry->extension_version == ext_entry.extension_version) {
      if (index_count == 0) first_index = entry;
      index_count++;
    }
  }

  if (first_index != nullptr) {
    villagesql_error(
        "Cannot drop extension `%s` as %d custom index(es) depend on it, "
        "e.g. %s.%s.%s uses index type %s",
        MYF(0), ext_entry.extension_name().c_str(), index_count,
        first_index->db_name().c_str(), first_index->table_name().c_str(),
        first_index->index_name().c_str(),
        first_index->index_type_name.c_str());
    return true;
  }

  // Build index_id -> IndexEntry map for profile error message context.
  std::unordered_map<uint64_t, const IndexEntry *> index_by_id;
  for (const IndexEntry *entry : all_indexes) {
    index_by_id.emplace(entry->index_id, entry);
  }

  const IndexColumnEntry *first_col = nullptr;
  int col_count = 0;

  for (const IndexColumnEntry *col : all_index_columns) {
    if (col->profile_extension_name == ext_entry.extension_name() &&
        col->profile_extension_version == ext_entry.extension_version) {
      if (col_count == 0) first_col = col;
      col_count++;
    }
  }

  if (first_col != nullptr) {
    auto it = index_by_id.find(first_col->index_id());
    const IndexEntry *parent = (it != index_by_id.end()) ? it->second : nullptr;
    assert(parent);
    villagesql_error(
        "Cannot drop extension `%s` as %d index column(s) depend on its"
        " profile `%s`, e.g. %s.%s.%s (column %s)",
        MYF(0), ext_entry.extension_name().c_str(), col_count,
        first_col->profile_name.c_str(), parent->db_name().c_str(),
        parent->table_name().c_str(), parent->index_name().c_str(),
        first_col->column_name.c_str());
    return true;
  }

  return false;
}

}  // namespace villagesql
