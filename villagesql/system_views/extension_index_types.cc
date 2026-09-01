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

// INFORMATION_SCHEMA.EXTENSION_INDEX_TYPES fill-type table.
//
// One row per custom index type provided by a loaded extension, with the
// optimizer capabilities and storage properties it declares. This answers "what
// can an index of this type actually do", which INDEX_TYPE in I_S.STATISTICS
// names but cannot describe.
//
// Deliberately NOT privilege-filtered. Rows name no user object -- only
// installed software -- which is the line MySQL draws: I_S.TABLES and
// I_S.COLUMNS filter because their rows identify user schemas, while
// I_S.CHARACTER_SETS, I_S.COLLATIONS and I_S.PLUGINS do not. The sibling table
// I_S.CUSTOM_INDEXES does filter, because its rows do name user objects.

#include "villagesql/system_views/extension_index_types.h"

#include <cstring>

#include "sql/field.h"
#include "sql/sql_show.h"
#include "sql/table.h"
#include "villagesql/schema/descriptor/index_type_descriptor.h"
#include "villagesql/schema/victionary_client.h"

namespace {

// Field indices matching the order in villagesql_extension_index_types_fields.
enum {
  FIELD_EXTENSION_NAME,
  FIELD_EXTENSION_VERSION,
  FIELD_INDEX_TYPE_NAME,
  FIELD_SUPPORTS_POINT_LOOKUP,
  FIELD_SUPPORTS_RANGE_SCAN,
  FIELD_SUPPORTS_REVERSE_SCAN,
  FIELD_SUPPORTS_ORDER_BY,
  FIELD_SUPPORTS_KNN,
  FIELD_HAS_COLUMN_REF,
  FIELD_HAS_ROW_REF,
  FIELD_SUPPORTS_REF_LOOKUP,
  FIELD_CAPABILITIES,
  FIELD_STORAGE_PROPS
};

void store_yes_no(Field *field, bool value) {
  const char *str = value ? "YES" : "NO";
  field->store(str, strlen(str), system_charset_info);
}

}  // namespace

ST_FIELD_INFO villagesql_extension_index_types_fields[] = {
    {"EXTENSION_NAME", 64, MYSQL_TYPE_STRING, 0, 0, nullptr, 0},
    {"EXTENSION_VERSION", 64, MYSQL_TYPE_STRING, 0, 0, nullptr, 0},
    {"INDEX_TYPE_NAME", 64, MYSQL_TYPE_STRING, 0, 0, nullptr, 0},
    {"SUPPORTS_POINT_LOOKUP", 3, MYSQL_TYPE_STRING, 0, 0, nullptr, 0},
    {"SUPPORTS_RANGE_SCAN", 3, MYSQL_TYPE_STRING, 0, 0, nullptr, 0},
    {"SUPPORTS_REVERSE_SCAN", 3, MYSQL_TYPE_STRING, 0, 0, nullptr, 0},
    {"SUPPORTS_ORDER_BY", 3, MYSQL_TYPE_STRING, 0, 0, nullptr, 0},
    {"SUPPORTS_KNN", 3, MYSQL_TYPE_STRING, 0, 0, nullptr, 0},
    {"HAS_COLUMN_REF", 3, MYSQL_TYPE_STRING, 0, 0, nullptr, 0},
    {"HAS_ROW_REF", 3, MYSQL_TYPE_STRING, 0, 0, nullptr, 0},
    {"SUPPORTS_REF_LOOKUP", 3, MYSQL_TYPE_STRING, 0, 0, nullptr, 0},
    // The raw bitmasks are exposed alongside the decoded columns, as
    // I_S.INNODB_TABLES exposes both FLAG and the derived ROW_FORMAT. If the
    // SDK gains a VEF_INDEX_CAP_* or VEF_INDEX_STORAGE_* bit before this table
    // learns its name, the bit is still visible here rather than silently
    // dropped.
    {"CAPABILITIES", 10, MYSQL_TYPE_LONGLONG, 0, MY_I_S_UNSIGNED, nullptr, 0},
    {"STORAGE_PROPS", 10, MYSQL_TYPE_LONGLONG, 0, MY_I_S_UNSIGNED, nullptr, 0},
    {nullptr, 0, MYSQL_TYPE_STRING, 0, 0, nullptr, 0}};

int fill_extension_index_types(THD *thd, Table_ref *tables, Item *) {
  TABLE *table = tables->table;
  CHARSET_INFO *cs = system_charset_info;

  villagesql::VictionaryClient &vclient =
      villagesql::VictionaryClient::instance();
  auto lock = vclient.get_read_lock();

  for (const villagesql::IndexTypeDescriptor *desc :
       vclient.index_type_descriptors().get_all_committed()) {
    restore_record(table, s->default_values);

    const std::string &ext_name = desc->extension_name();
    table->field[FIELD_EXTENSION_NAME]->store(ext_name.c_str(),
                                              ext_name.length(), cs);

    const std::string &ext_version = desc->extension_version();
    table->field[FIELD_EXTENSION_VERSION]->store(ext_version.c_str(),
                                                 ext_version.length(), cs);

    const std::string &type_name = desc->index_type_name();
    table->field[FIELD_INDEX_TYPE_NAME]->store(type_name.c_str(),
                                               type_name.length(), cs);

    const vef_type_index_intf_t &intf = desc->intf();

    store_yes_no(table->field[FIELD_SUPPORTS_POINT_LOOKUP],
                 (intf.capabilities & VEF_INDEX_CAP_POINT_LOOKUP) != 0);
    store_yes_no(table->field[FIELD_SUPPORTS_RANGE_SCAN],
                 (intf.capabilities & VEF_INDEX_CAP_RANGE_SCAN) != 0);
    store_yes_no(table->field[FIELD_SUPPORTS_REVERSE_SCAN],
                 (intf.capabilities & VEF_INDEX_CAP_REVERSE_SCAN) != 0);
    store_yes_no(table->field[FIELD_SUPPORTS_ORDER_BY],
                 (intf.capabilities & VEF_INDEX_CAP_ORDER_BY) != 0);
    store_yes_no(table->field[FIELD_SUPPORTS_KNN],
                 (intf.capabilities & VEF_INDEX_CAP_KNN) != 0);

    store_yes_no(table->field[FIELD_HAS_COLUMN_REF],
                 (intf.storage_props & VEF_INDEX_STORAGE_HAS_COLUMN_REF) != 0);
    store_yes_no(table->field[FIELD_HAS_ROW_REF],
                 (intf.storage_props & VEF_INDEX_STORAGE_HAS_ROW_REF) != 0);
    store_yes_no(table->field[FIELD_SUPPORTS_REF_LOOKUP],
                 (intf.storage_props & VEF_INDEX_STORAGE_REF_LOOKUP) != 0);

    table->field[FIELD_CAPABILITIES]->store(
        static_cast<longlong>(intf.capabilities), true);
    table->field[FIELD_STORAGE_PROPS]->store(
        static_cast<longlong>(intf.storage_props), true);

    if (schema_table_store_record(thd, table)) return 1;
  }

  return 0;
}
