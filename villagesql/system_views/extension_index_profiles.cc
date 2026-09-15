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

// INFORMATION_SCHEMA.EXTENSION_INDEX_PROFILES fill-type table.
//
// One row per index profile provided by a loaded extension. A profile binds a
// data type to an index type, so three extensions can be involved in one row
// and each is reported separately: the profile's own, the data type's, and the
// index type's. Nothing requires them to be the same -- the profile carries its
// own optional qualifier in the grammar, and the default-profile lookup in
// villagesql/sql/metadata_modifier.cc searches every extension's profiles.
//
// This is what closes the (PROFILE_EXTENSION_NAME, PROFILE_EXTENSION_VERSION,
// PROFILE_NAME) triple published by I_S.CUSTOM_INDEX_COLUMNS, which otherwise
// joins to nothing. The names match on both sides, so that hop is a plain
// natural join.
//
// Deliberately NOT privilege-filtered, like EXTENSION_INDEX_TYPES: rows name
// installed software, never a user object.

#include "villagesql/system_views/extension_index_profiles.h"

#include <cstring>

#include "sql/field.h"
#include "sql/sql_show.h"
#include "sql/table.h"
#include "villagesql/schema/descriptor/index_profile_descriptor.h"
#include "villagesql/schema/victionary_client.h"

namespace {

// Field indices matching the order in
// villagesql_extension_index_profiles_fields.
enum {
  FIELD_PROFILE_EXTENSION_NAME,
  FIELD_PROFILE_EXTENSION_VERSION,
  FIELD_PROFILE_NAME,
  FIELD_DATA_TYPE_EXTENSION_NAME,
  FIELD_DATA_TYPE_NAME,
  FIELD_INDEX_TYPE_EXTENSION_NAME,
  FIELD_INDEX_TYPE_NAME,
  FIELD_ORDERING_ASC,
  FIELD_ORDERING_DESC,
  FIELD_IS_DEFAULT_PROFILE
};

void store_yes_no(Field *field, bool value) {
  const char *str = value ? "YES" : "NO";
  field->store(str, strlen(str), system_charset_info);
}

void store_str(Field *field, const std::string &value) {
  field->store(value.c_str(), value.length(), system_charset_info);
}

}  // namespace

ST_FIELD_INFO villagesql_extension_index_profiles_fields[] = {
    // The profile's own extension. Distinct from the two below.
    {"PROFILE_EXTENSION_NAME", 64, MYSQL_TYPE_STRING, 0, 0, nullptr, 0},
    {"PROFILE_EXTENSION_VERSION", 64, MYSQL_TYPE_STRING, 0, 0, nullptr, 0},
    {"PROFILE_NAME", 64, MYSQL_TYPE_STRING, 0, 0, nullptr, 0},
    // The data type this profile binds, and the extension providing it.
    {"DATA_TYPE_EXTENSION_NAME", 64, MYSQL_TYPE_STRING, 0, 0, nullptr, 0},
    {"DATA_TYPE_NAME", 64, MYSQL_TYPE_STRING, 0, 0, nullptr, 0},
    // The index type this profile binds, and the extension providing it. Note
    // there is no version here: a profile binds "hnsw from vsql_vector", not a
    // particular version of it, so joining EXTENSION_INDEX_TYPES uses two of
    // that table's three key columns.
    {"INDEX_TYPE_EXTENSION_NAME", 64, MYSQL_TYPE_STRING, 0, 0, nullptr, 0},
    {"INDEX_TYPE_NAME", 64, MYSQL_TYPE_STRING, 0, 0, nullptr, 0},
    // Scan ordering AS DECLARED, which is not the same as what the index can
    // do. The SDK's IndexOrdering has only ASC and DESC, with no way to say
    // "unordered", and the descriptor defaults to ASC -- so a profile whose
    // author never considered ordering still reports ORDERING_ASC = YES. The
    // TODO(villagesql-indexing) in statistics.cc records why
    // STATISTICS.COLLATION deliberately does not trust this field.
    {"ORDERING_ASC", 3, MYSQL_TYPE_STRING, 0, 0, nullptr, 0},
    {"ORDERING_DESC", 3, MYSQL_TYPE_STRING, 0, 0, nullptr, 0},
    // Whether this profile is chosen when a key column names no profile.
    {"IS_DEFAULT_PROFILE", 3, MYSQL_TYPE_STRING, 0, 0, nullptr, 0},
    {nullptr, 0, MYSQL_TYPE_STRING, 0, 0, nullptr, 0}};

int fill_extension_index_profiles(THD *thd, Table_ref *tables, Item *) {
  TABLE *table = tables->table;

  villagesql::VictionaryClient &vclient =
      villagesql::VictionaryClient::instance();
  auto lock = vclient.get_read_lock();

  for (const villagesql::IndexProfileDescriptor *desc :
       vclient.index_profile_descriptors().get_all_committed()) {
    restore_record(table, s->default_values);

    store_str(table->field[FIELD_PROFILE_EXTENSION_NAME],
              desc->extension_name());
    store_str(table->field[FIELD_PROFILE_EXTENSION_VERSION],
              desc->extension_version());
    store_str(table->field[FIELD_PROFILE_NAME], desc->profile_name());

    store_str(table->field[FIELD_DATA_TYPE_EXTENSION_NAME],
              desc->type_ref().extension_name());
    store_str(table->field[FIELD_DATA_TYPE_NAME], desc->type_name());

    store_str(table->field[FIELD_INDEX_TYPE_EXTENSION_NAME],
              desc->index_type_ref().extension_name());
    store_str(table->field[FIELD_INDEX_TYPE_NAME], desc->index_type_name());

    store_yes_no(table->field[FIELD_ORDERING_ASC], desc->ordering_asc());
    store_yes_no(table->field[FIELD_ORDERING_DESC], desc->ordering_desc());
    store_yes_no(table->field[FIELD_IS_DEFAULT_PROFILE],
                 desc->default_for_type());

    if (schema_table_store_record(thd, table)) return 1;
  }

  return 0;
}
