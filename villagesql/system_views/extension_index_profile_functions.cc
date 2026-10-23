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

// INFORMATION_SCHEMA.EXTENSION_INDEX_PROFILE_FUNCTIONS fill-type table.
//
// One row per (profile, function) pair, where "function" is a user-visible
// SQL function the profile registers via IndexProfileBuilder::with_function().
//
// Deliberately NOT privilege-filtered, like EXTENSION_INDEX_PROFILES: rows
// name installed software, never a user object.

#include "villagesql/system_views/extension_index_profile_functions.h"

#include <cstring>

#include "sql/field.h"
#include "sql/sql_show.h"
#include "sql/table.h"
#include "villagesql/schema/descriptor/index_profile_descriptor.h"
#include "villagesql/schema/victionary_client.h"

namespace {

enum { FIELD_PROFILE_EXTENSION_NAME, FIELD_PROFILE_NAME, FIELD_FUNCTION_NAME };

void store_str(Field *field, const std::string &value) {
  field->store(value.c_str(), value.length(), system_charset_info);
}

void store_cstr(Field *field, const char *value) {
  field->store(value, strlen(value), system_charset_info);
}

}  // namespace

ST_FIELD_INFO villagesql_extension_index_profile_functions_fields[] = {
    // The profile this function belongs to. The (PROFILE_EXTENSION_NAME,
    // PROFILE_NAME) pair joins EXTENSION_INDEX_PROFILES, and reaches
    // CUSTOM_INDEX_COLUMNS to answer "which indexes expose this function."
    {"PROFILE_EXTENSION_NAME", 64, MYSQL_TYPE_STRING, 0, 0, nullptr, 0},
    {"PROFILE_NAME", 64, MYSQL_TYPE_STRING, 0, 0, nullptr, 0},
    {"FUNCTION_NAME", 64, MYSQL_TYPE_STRING, 0, 0, nullptr, 0},
    {nullptr, 0, MYSQL_TYPE_STRING, 0, 0, nullptr, 0}};

int fill_extension_index_profile_functions(THD *thd, Table_ref *tables,
                                           Item *) {
  TABLE *table = tables->table;

  villagesql::VictionaryClient &vclient =
      villagesql::VictionaryClient::instance();
  auto lock = vclient.get_read_lock();

  for (const villagesql::IndexProfileDescriptor *desc :
       vclient.index_profile_descriptors().get_all_committed()) {
    for (const vef_index_profile_fn_binding_t &binding : desc->functions()) {
      if (binding.name == nullptr) continue;

      restore_record(table, s->default_values);

      store_str(table->field[FIELD_PROFILE_EXTENSION_NAME],
                desc->extension_name());
      store_str(table->field[FIELD_PROFILE_NAME], desc->profile_name());
      store_cstr(table->field[FIELD_FUNCTION_NAME], binding.name);

      if (schema_table_store_record(thd, table)) return 1;
    }
  }

  return 0;
}
