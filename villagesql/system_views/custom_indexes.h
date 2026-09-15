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

  One row per custom index, mirroring villagesql.custom_indexes. The per-key-
  column half lives in I_S.CUSTOM_INDEX_COLUMNS, joined on INDEX_ID -- the same
  split, and the same join key, that upstream uses for INNODB_INDEXES /
  INNODB_FIELDS.

  Two extensions are involved in a custom index and they need not be the same
  one, so each column says which role it names. INDEX_TYPE_EXTENSION_NAME and
  INDEX_TYPE_EXTENSION_VERSION here are the extension providing the index
  *type*; the extension providing a key column's *profile* is a separate pair,
  published as PROFILE_EXTENSION_* by I_S.CUSTOM_INDEX_COLUMNS.

  Regular indexes do not appear: the villagesql table is the driving side of an
  inner join, so only custom indexes are listed by construction.
*/
class Custom_indexes
    : public dd::system_views::System_view_impl<
          dd::system_views::System_view_select_definition_impl> {
 public:
  enum enum_fields {
    FIELD_INDEX_ID,
    FIELD_TABLE_CATALOG,
    FIELD_TABLE_SCHEMA,
    FIELD_TABLE_NAME,
    FIELD_INDEX_NAME,
    FIELD_INDEX_TYPE_EXTENSION_NAME,
    FIELD_INDEX_TYPE_EXTENSION_VERSION,
    FIELD_INDEX_TYPE_NAME,
    FIELD_INDEX_TYPE_PARAMETERS
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
