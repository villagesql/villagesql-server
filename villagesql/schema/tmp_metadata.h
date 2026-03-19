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

#ifndef VILLAGESQL_SCHEMA_TMP_METADATA_H_
#define VILLAGESQL_SCHEMA_TMP_METADATA_H_

#include <map>
#include <memory>
#include <string>

#include "villagesql/schema/descriptor/type_context.h"
#include "villagesql/schema/systable/custom_columns.h"

class Field;
class THD;

namespace villagesql {

/**
  Session-local storage for custom column metadata of temporary tables.
  Each THD has its own instance of this class, allowing it to track
  custom types for its own temporary tables without global locking.
*/
class TmpMetadata {
 public:
  TmpMetadata() = default;

  // Insert or update the TypeContext for a temporary column. The shared_ptr
  // keeps the extension refcount elevated, blocking uninstall while the
  // temporary table exists.
  void insert(const ColumnKey &key,
              std::shared_ptr<const TypeContext> tc_owner) {
    m_columns[key.str()] = std::move(tc_owner);
  }

  // Get the TypeContext for a temporary column. Returns nullptr if not found.
  const TypeContext *get(const std::string &key_str) const {
    auto it = m_columns.find(key_str);
    if (it == m_columns.end()) return nullptr;
    return it->second.get();
  }

  // Delete all metadata for a specific temporary table.
  void delete_table(const std::string &db_name, const std::string &table_name) {
    ColumnKeyPrefix prefix(db_name, table_name);
    const std::string &prefix_str = prefix.str();

    for (auto it = m_columns.begin(); it != m_columns.end();) {
      if (it->first.compare(0, prefix_str.length(), prefix_str) == 0) {
        it = m_columns.erase(it);
      } else {
        ++it;
      }
    }
  }

  bool empty() const { return m_columns.empty(); }

  // Acquire a TypeContext from the victionary and insert it into thd's
  // TmpMetadata. If field is non-null, also calls set_type_context on it
  // and validates the field length.
  static void insert_for_thd(THD *thd, const ColumnKey &key,
                              const TypeContextKey &source_key,
                              Field *field = nullptr);

 private:
  // Map from normalized ColumnKey string to owned TypeContext reference.
  std::map<std::string, std::shared_ptr<const TypeContext>> m_columns;
};

}  // namespace villagesql

#endif  // VILLAGESQL_SCHEMA_TMP_METADATA_H_
