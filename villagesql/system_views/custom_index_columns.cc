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

#include "villagesql/system_views/custom_index_columns.h"

#include <string>

#include "sql/stateless_allocator.h"
#include "villagesql/schema/identifier_names.h"

namespace villagesql {
namespace system_views {

const Custom_index_columns &Custom_index_columns::instance() {
  static Custom_index_columns *s_instance = new Custom_index_columns();
  return *s_instance;
}

Custom_index_columns::Custom_index_columns() {
  m_target_def.set_view_name(view_name());

  // The join key back to I_S.CUSTOM_INDEXES, which is where the schema, table
  // and index names live. Mirrors INNODB_FIELDS.INDEX_ID.
  m_target_def.add_field(FIELD_INDEX_ID, "INDEX_ID", "vcic.index_id");
  // key_position is 0-based; STATISTICS.SEQ_IN_INDEX is 1-based. Aligning them
  // lets a query reach STATISTICS through CUSTOM_INDEXES and match key column
  // for key column.
  m_target_def.add_field(FIELD_SEQ_IN_INDEX, "SEQ_IN_INDEX",
                         "vcic.key_position + 1");
  // Column names are stored as entered on both sides, so no DD round trip is
  // needed here -- unlike the schema and table names, which is why those are
  // resolved in CUSTOM_INDEXES rather than taken from villagesql. The collation
  // is utf8mb4_0900_ai_ci rather than the utf8mb3_tolower_ci that STATISTICS
  // uses: the source column is utf8mb4, there is no utf8mb4_tolower_ci, and
  // naming utf8mb3 would need a CONVERT that emits a deprecation warning on
  // every query of the view. Both collations are case-insensitive, so the
  // observable behaviour matches.
  m_target_def.add_field(FIELD_COLUMN_NAME, "COLUMN_NAME",
                         "vcic.column_name COLLATE utf8mb4_0900_ai_ci");
  // The extension providing the *profile*, which villagesql records per key
  // column and separately from the index type's extension in CUSTOM_INDEXES.
  m_target_def.add_field(FIELD_EXTENSION_NAME, "EXTENSION_NAME",
                         "vcic.extension_name COLLATE utf8mb4_0900_ai_ci");
  m_target_def.add_field(FIELD_EXTENSION_VERSION, "EXTENSION_VERSION",
                         "vcic.extension_version COLLATE utf8mb4_0900_ai_ci");
  m_target_def.add_field(FIELD_PROFILE_NAME, "PROFILE_NAME",
                         "vcic.profile_name COLLATE utf8mb4_0900_ai_ci");

  m_target_def.add_from("villagesql.custom_index_columns vcic");
  m_target_def.add_from(
      "JOIN villagesql.custom_indexes vci ON vci.index_id = vcic.index_id");

  // These joins project nothing. They exist for the WHERE clause below:
  // sch/tbl feed CAN_ACCESS_TABLE, and idx feeds IS_VISIBLE_DD_OBJECT while
  // also dropping any villagesql row whose data-dictionary index has gone.
  // mysql.catalogs is deliberately absent -- TABLE_CATALOG was its only
  // consumer and does not appear in this view.
  const std::string sch_join =
      std::string("JOIN mysql.schemata sch ON ") +
      database_name_match_sql("vci.db_name", "sch.name");
  m_target_def.add_from(sch_join.c_str());

  const std::string tbl_join =
      std::string("JOIN mysql.tables tbl ON tbl.schema_id = sch.id AND ") +
      table_name_match_sql("vci.table_name", "tbl.name");
  m_target_def.add_from(tbl_join.c_str());

  const std::string idx_join =
      std::string("JOIN mysql.indexes idx ON idx.table_id = tbl.id AND ") +
      index_name_match_sql("vci.index_name", "idx.name");
  m_target_def.add_from(idx_join.c_str());

  // COLUMN_NAME is a user column name, so these rows describe user objects even
  // though no schema or table is projected. The filter must stay.
  m_target_def.add_where("CAN_ACCESS_TABLE(sch.name, tbl.name)");
  m_target_def.add_where(
      "AND IS_VISIBLE_DD_OBJECT(tbl.hidden, idx.hidden, idx.options)");
}

}  // namespace system_views
}  // namespace villagesql
