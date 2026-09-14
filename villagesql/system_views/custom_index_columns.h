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

#ifndef VILLAGESQL_SYSTEM_VIEWS_CUSTOM_INDEX_COLUMNS_INCLUDED
#define VILLAGESQL_SYSTEM_VIEWS_CUSTOM_INDEX_COLUMNS_INCLUDED

#include "sql/dd/impl/system_views/system_view_definition_impl.h"
#include "sql/dd/impl/system_views/system_view_impl.h"
#include "sql/dd/string_type.h"

namespace villagesql {
namespace system_views {

/**
  The class representing INFORMATION_SCHEMA.CUSTOM_INDEX_COLUMNS system view
  definition.

  One row per key column of a custom index, mirroring
  villagesql.custom_index_columns. Keyed on INDEX_ID and joined to
  I_S.CUSTOM_INDEXES, following upstream's INNODB_INDEXES / INNODB_FIELDS pair
  rather than repeating the schema, table and index names on every row.

  Reaching a custom index's key columns therefore goes through CUSTOM_INDEXES:

      SELECT i.TABLE_NAME, i.INDEX_NAME, c.SEQ_IN_INDEX, c.COLUMN_NAME
      FROM INFORMATION_SCHEMA.CUSTOM_INDEXES i
      JOIN INFORMATION_SCHEMA.CUSTOM_INDEX_COLUMNS c USING (INDEX_ID)

  EXTENSION_NAME and EXTENSION_VERSION here are the extension providing the
  *profile* named by PROFILE_NAME -- a different pair from the one in
  CUSTOM_INDEXES, which names the extension providing the index type. The two
  coincide for every in-tree extension but are recorded separately because
  nothing requires it, and keeping the halves in separate views is what lets the
  name mean one thing in each.

  The cost of that is at join time: with both views in one FROM the bare name
  EXTENSION_NAME is ambiguous, so reaching I_S.EXTENSION_INDEX_PROFILES must
  qualify each side rather than use USING:

      JOIN INFORMATION_SCHEMA.EXTENSION_INDEX_PROFILES p
        ON p.EXTENSION_NAME    = c.EXTENSION_NAME
       AND p.EXTENSION_VERSION = c.EXTENSION_VERSION
       AND p.PROFILE_NAME      = c.PROFILE_NAME

  Joining i.EXTENSION_NAME there instead is wrong and silently agrees for every
  in-tree extension, since each supplies its own profiles.

  SEQ_IN_INDEX is 1-based to match I_S.STATISTICS, while the underlying
  key_position is 0-based.

  Although no column here names a schema or table, the rows still describe user
  objects -- COLUMN_NAME is a user column -- so the view keeps the same per-row
  CAN_ACCESS_TABLE filter as CUSTOM_INDEXES. The schema and table joins remain
  in the definition to feed that predicate even though they are not projected.
*/
class Custom_index_columns
    : public dd::system_views::System_view_impl<
          dd::system_views::System_view_select_definition_impl> {
 public:
  enum enum_fields {
    FIELD_INDEX_ID,
    FIELD_SEQ_IN_INDEX,
    FIELD_COLUMN_NAME,
    FIELD_EXTENSION_NAME,
    FIELD_EXTENSION_VERSION,
    FIELD_PROFILE_NAME
  };

  Custom_index_columns();

  static const Custom_index_columns &instance();

  static const dd::String_type &view_name() {
    static dd::String_type s_view_name("CUSTOM_INDEX_COLUMNS");
    return s_view_name;
  }

  const dd::String_type &name() const override {
    return Custom_index_columns::view_name();
  }
};

}  // namespace system_views
}  // namespace villagesql

#endif  // VILLAGESQL_SYSTEM_VIEWS_CUSTOM_INDEX_COLUMNS_INCLUDED
