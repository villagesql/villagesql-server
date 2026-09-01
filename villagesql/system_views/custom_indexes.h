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

#ifndef VILLAGESQL_SYSTEM_VIEWS_CUSTOM_INDEXES_INCLUDED
#define VILLAGESQL_SYSTEM_VIEWS_CUSTOM_INDEXES_INCLUDED

#include "sql/dd/impl/system_views/system_view_definition_impl.h"
#include "sql/dd/impl/system_views/system_view_impl.h"
#include "sql/dd/string_type.h"

namespace villagesql {
namespace system_views {

/**
  The class representing INFORMATION_SCHEMA.CUSTOM_INDEXES system view
  definition.

  One row per (custom index, key column), matching I_S.STATISTICS granularity so
  the two join 1:1 on (TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX).
  Regular indexes do not appear: the villagesql tables are the driving side of
  an inner join, so only custom indexes are listed by construction.

  Joins the data dictionary rather than reporting the names villagesql stores,
  because at lower_case_table_names = 2 the villagesql tables hold lower-cased
  schema and table names while the DD keeps the CREATE-time case. Reporting the
  stored form would disagree with every other I_S surface for the same table.
*/
class Custom_indexes
    : public dd::system_views::System_view_impl<
          dd::system_views::System_view_select_definition_impl> {
 public:
  enum enum_fields {
    FIELD_TABLE_CATALOG,
    FIELD_TABLE_SCHEMA,
    FIELD_TABLE_NAME,
    FIELD_INDEX_NAME,
    FIELD_SEQ_IN_INDEX,
    FIELD_COLUMN_NAME,
    FIELD_EXTENSION_NAME,
    FIELD_EXTENSION_VERSION,
    FIELD_INDEX_TYPE_NAME,
    FIELD_INDEX_TYPE_PARAMETERS,
    FIELD_PROFILE_NAME
  };

  Custom_indexes();

  static const Custom_indexes &instance();

  static const dd::String_type &view_name() {
    static dd::String_type s_view_name("CUSTOM_INDEXES");
    return s_view_name;
  }

  const dd::String_type &name() const override {
    return Custom_indexes::view_name();
  }
};

}  // namespace system_views
}  // namespace villagesql

#endif  // VILLAGESQL_SYSTEM_VIEWS_CUSTOM_INDEXES_INCLUDED
