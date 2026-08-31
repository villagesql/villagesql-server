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

#ifndef VILLAGESQL_SYSTEM_VIEWS_STATISTICS_INCLUDED
#define VILLAGESQL_SYSTEM_VIEWS_STATISTICS_INCLUDED

#include "sql/dd/impl/system_views/system_view_definition_impl.h"
#include "sql/dd/impl/system_views/system_view_impl.h"
#include "sql/dd/string_type.h"

namespace villagesql {
namespace system_views {

// Shared definition behind INFORMATION_SCHEMA.STATISTICS and the hidden
// SHOW_STATISTICS view that backs SHOW INDEX. Both are registered as
// NON_DD_BASED overrides of the upstream views of the same name, because the
// definition joins villagesql.custom_indexes, which does not exist yet when
// the DD-based views are created during bootstrap.
class Statistics_base
    : public dd::system_views::System_view_impl<
          dd::system_views::System_view_select_definition_impl> {
 public:
  enum enum_fields {
    FIELD_TABLE_CATALOG,
    FIELD_TABLE_SCHEMA,
    FIELD_TABLE_NAME,
    FIELD_NON_UNIQUE,
    FIELD_INDEX_SCHEMA,
    FIELD_INDEX_NAME,
    FIELD_SEQ_IN_INDEX,
    FIELD_COLUMN_NAME,
    FIELD_COLLATION,
    FIELD_CARDINALITY,
    FIELD_SUB_PART,
    FIELD_PACKED,
    FIELD_NULLABLE,
    FIELD_INDEX_TYPE,
    FIELD_COMMENT,
    FIELD_INDEX_COMMENT,
    FIELD_IS_VISIBLE,
    FIELD_INDEX_ORDINAL_POSITION,
    FIELD_COLUMN_ORDINAL_POSITION,
    FIELD_EXPRESSION
  };

  Statistics_base();

  const dd::String_type &name() const override = 0;
};

/*
  The class representing INFORMATION_SCHEMA.STATISTICS system view definition.
*/
class Statistics : public Statistics_base {
 public:
  Statistics();

  static const Statistics_base &instance();

  static const dd::String_type &view_name() {
    static dd::String_type s_view_name("STATISTICS");
    return s_view_name;
  }

  const dd::String_type &name() const override {
    return Statistics::view_name();
  }
};

/*
  The class representing the system view definition used by SHOW INDEX.
*/
class Show_statistics : public Statistics {
 public:
  Show_statistics();

  static const Statistics_base &instance();

  static const dd::String_type &view_name() {
    static dd::String_type s_view_name("SHOW_STATISTICS");
    return s_view_name;
  }

  const dd::String_type &name() const override {
    return Show_statistics::view_name();
  }

  // This view definition is hidden from user.
  bool hidden() const override { return true; }
};

}  // namespace system_views
}  // namespace villagesql

#endif  // VILLAGESQL_SYSTEM_VIEWS_STATISTICS_INCLUDED
