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

#ifndef VILLAGESQL_SYSTEM_VIEWS_COLUMNS_INCLUDED
#define VILLAGESQL_SYSTEM_VIEWS_COLUMNS_INCLUDED

#include "sql/dd/impl/system_views/system_view_definition_impl.h"
#include "sql/dd/impl/system_views/system_view_impl.h"
#include "sql/dd/string_type.h"

namespace villagesql {
namespace system_views {

/*
  The class representing INFORMATION_SCHEMA.COLUMNS system view definition.
*/
class Columns : public dd::system_views::System_view_impl<
                    dd::system_views::System_view_select_definition_impl> {
 public:
  enum enum_fields {
    FIELD_TABLE_CATALOG,
    FIELD_TABLE_SCHEMA,
    FIELD_TABLE_NAME,
    FIELD_COLUMN_NAME,
    FIELD_ORDINAL_POSITION,
    FIELD_COLUMN_DEFAULT,
    FIELD_IS_NULLABLE,
    FIELD_DATA_TYPE,
    FIELD_CHARACTER_MAXIMUM_LENGTH,
    FIELD_CHARACTER_OCTET_LENGTH,
    FIELD_NUMERIC_PRECISION,
    FIELD_NUMERIC_SCALE,
    FIELD_DATETIME_PRECISION,
    FIELD_CHARACTER_SET_NAME,
    FIELD_COLLATION_NAME,
    FIELD_COLUMN_TYPE,
    FIELD_COLUMN_KEY,
    FIELD_EXTRA,
    FIELD_PRIVILEGES,
    FIELD_COLUMN_COMMENT,
    FIELD_GENERATION_EXPRESSION,
    FIELD_SRS_ID
  };

  Columns();

  static const Columns &instance();

  static const dd::String_type &view_name() {
    static dd::String_type s_view_name("COLUMNS");
    return s_view_name;
  }

  const dd::String_type &name() const override { return Columns::view_name(); }
};

}  // namespace system_views
}  // namespace villagesql

#endif  // VILLAGESQL_SYSTEM_VIEWS_COLUMNS_INCLUDED
