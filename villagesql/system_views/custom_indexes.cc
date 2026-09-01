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

#include "villagesql/system_views/custom_indexes.h"

#include <string>

#include "sql/stateless_allocator.h"
#include "villagesql/schema/identifier_names.h"

namespace villagesql {
namespace system_views {

const Custom_indexes &Custom_indexes::instance() {
  static Custom_indexes *s_instance = new Custom_indexes();
  return *s_instance;
}

Custom_indexes::Custom_indexes() {
  m_target_def.set_view_name(view_name());

  m_target_def.add_field(FIELD_TABLE_CATALOG, "TABLE_CATALOG",
                         "cat.name" + m_target_def.fs_name_collation());
  m_target_def.add_field(FIELD_TABLE_SCHEMA, "TABLE_SCHEMA",
                         "sch.name" + m_target_def.fs_name_collation());
  m_target_def.add_field(FIELD_TABLE_NAME, "TABLE_NAME",
                         "tbl.name" + m_target_def.fs_name_collation());
  m_target_def.add_field(FIELD_INDEX_NAME, "INDEX_NAME",
                         "idx.name COLLATE utf8mb3_tolower_ci");
  // key_position is 0-based; STATISTICS.SEQ_IN_INDEX is 1-based. Aligning them
  // is what lets the two views join on (schema, table, index, seq).
  m_target_def.add_field(FIELD_SEQ_IN_INDEX, "SEQ_IN_INDEX",
                         "vcic.key_position + 1");
  // Column names are stored as entered on both sides, so no DD round trip is
  // needed here -- unlike the schema and table names above. The collation is
  // utf8mb4_0900_ai_ci rather than the utf8mb3_tolower_ci that STATISTICS uses:
  // the source column is utf8mb4, there is no utf8mb4_tolower_ci, and naming
  // utf8mb3 would need a CONVERT that emits a deprecation warning on every
  // query of the view. Both collations are case-insensitive, so the observable
  // behaviour matches.
  m_target_def.add_field(FIELD_COLUMN_NAME, "COLUMN_NAME",
                         "vcic.column_name COLLATE utf8mb4_0900_ai_ci");
  m_target_def.add_field(FIELD_EXTENSION_NAME, "EXTENSION_NAME",
                         "vci.extension_name COLLATE utf8mb4_0900_ai_ci");
  m_target_def.add_field(FIELD_EXTENSION_VERSION, "EXTENSION_VERSION",
                         "vci.extension_version COLLATE utf8mb4_0900_ai_ci");
  m_target_def.add_field(FIELD_INDEX_TYPE_NAME, "INDEX_TYPE_NAME",
                         "vci.index_type_name COLLATE utf8mb4_0900_ai_ci");
  // Kept as JSON rather than rendered. Parameter values are stored as strings
  // even when the user wrote a number (vef_index_param_t renders numeric
  // literals as decimal strings), so a numeric predicate needs an explicit
  // CAST: CAST(INDEX_TYPE_PARAMETERS->>'$.M' AS UNSIGNED) < 16. Converting here
  // would mean guessing each parameter's type, which only the extension knows.
  m_target_def.add_field(FIELD_INDEX_TYPE_PARAMETERS, "INDEX_TYPE_PARAMETERS",
                         "vci.index_type_parameters");
  m_target_def.add_field(FIELD_PROFILE_NAME, "PROFILE_NAME",
                         "vcic.profile_name COLLATE utf8mb4_0900_ai_ci");

  // villagesql.custom_indexes drives the join, so only custom indexes appear.
  m_target_def.add_from("villagesql.custom_indexes vci");
  m_target_def.add_from(
      "JOIN villagesql.custom_index_columns vcic "
      "ON vcic.index_id = vci.index_id");

  // Resolve the DD objects. The name predicates fold case exactly as the
  // I_S.STATISTICS and I_S.COLUMNS overrides do; see identifier_names.h for why
  // database and table names follow lower_case_table_names while index names do
  // not.
  const std::string sch_join =
      std::string("JOIN mysql.schemata sch ON ") +
      database_name_match_sql("vci.db_name", "sch.name");
  m_target_def.add_from(sch_join.c_str());

  const std::string tbl_join =
      std::string("JOIN mysql.tables tbl ON tbl.schema_id = sch.id AND ") +
      table_name_match_sql("vci.table_name", "tbl.name");
  m_target_def.add_from(tbl_join.c_str());

  // Also acts as a consistency guard: a villagesql row whose data-dictionary
  // index has gone is not reported.
  const std::string idx_join =
      std::string("JOIN mysql.indexes idx ON idx.table_id = tbl.id AND ") +
      index_name_match_sql("vci.index_name", "idx.name");
  m_target_def.add_from(idx_join.c_str());

  m_target_def.add_from("JOIN mysql.catalogs cat ON cat.id = sch.catalog_id");

  // Rows name user objects, so they must be filtered the way I_S.TABLES and
  // I_S.COLUMNS are. Without this the view would list every schema's indexes to
  // every user.
  m_target_def.add_where("CAN_ACCESS_TABLE(sch.name, tbl.name)");
  m_target_def.add_where(
      "AND IS_VISIBLE_DD_OBJECT(tbl.hidden, idx.hidden, idx.options)");
}

}  // namespace system_views
}  // namespace villagesql
