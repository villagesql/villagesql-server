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

  m_target_def.add_field(FIELD_INDEX_ID, "INDEX_ID", "vci.index_id");
  m_target_def.add_field(FIELD_TABLE_CATALOG, "TABLE_CATALOG",
                         "cat.name" + m_target_def.fs_name_collation());
  m_target_def.add_field(FIELD_TABLE_SCHEMA, "TABLE_SCHEMA",
                         "sch.name" + m_target_def.fs_name_collation());
  m_target_def.add_field(FIELD_TABLE_NAME, "TABLE_NAME",
                         "tbl.name" + m_target_def.fs_name_collation());
  m_target_def.add_field(FIELD_INDEX_NAME, "INDEX_NAME",
                         "idx.name COLLATE utf8mb3_tolower_ci");
  m_target_def.add_field(FIELD_INDEX_TYPE_EXTENSION_NAME,
                         "INDEX_TYPE_EXTENSION_NAME",
                         "vci.extension_name COLLATE utf8mb4_0900_ai_ci");
  m_target_def.add_field(FIELD_INDEX_TYPE_NAME, "INDEX_TYPE_NAME",
                         "vci.index_type_name COLLATE utf8mb4_0900_ai_ci");
  // Kept as JSON rather than rendered.
  m_target_def.add_field(FIELD_INDEX_TYPE_PARAMETERS, "INDEX_TYPE_PARAMETERS",
                         "vci.index_type_parameters");

  //   village_store -> STORAGE_REF: opaque uint64 the extension writes at
  //     storage-allocation time. Zero means "no storage allocated yet";
  //     non-zero is the extension-encoded reference.
  //
  //   space_id, root -> MAPPING_INDEX_SPACE, MAPPING_INDEX_PAGE_NO:
  //     the InnoDB tablespace and stub root page InnoDB wrote at index
  //     creation. Every custom index goes through InnoDB's DDL path, so
  //     both are populated on every row. An extension built on InnoDB
  //     storage reads from there;
  //
  // STORAGE_REF is written by the extension and is independent of the
  // mapping pair, which InnoDB writes.
  m_target_def.add_field(FIELD_STORAGE_REF, "STORAGE_REF",
                         "INTERNAL_GET_CUSTOM_INDEX_PROPERTY("
                         "idx.se_private_data, 'village_store')");
  m_target_def.add_field(FIELD_MAPPING_INDEX_SPACE, "MAPPING_INDEX_SPACE",
                         "INTERNAL_GET_CUSTOM_INDEX_PROPERTY("
                         "idx.se_private_data, 'space_id')");
  m_target_def.add_field(FIELD_MAPPING_INDEX_PAGE_NO, "MAPPING_INDEX_PAGE_NO",
                         "INTERNAL_GET_CUSTOM_INDEX_PROPERTY("
                         "idx.se_private_data, 'root')");

  m_target_def.add_from("villagesql.custom_indexes vci");

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

  m_target_def.add_from("JOIN mysql.catalogs cat ON cat.id = sch.catalog_id");

  m_target_def.add_where("CAN_ACCESS_TABLE(sch.name, tbl.name)");
  m_target_def.add_where(
      "AND IS_VISIBLE_DD_OBJECT(tbl.hidden, idx.hidden, idx.options)");
}

}  // namespace system_views
}  // namespace villagesql
