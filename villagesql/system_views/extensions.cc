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

#include "villagesql/system_views/extensions.h"

#include <string>

#include "sql/dd/string_type.h"
#include "string_with_len.h"
#include "villagesql/schema/systable/extensions.h"

namespace {
enum {
  FIELD_EXTENSION_NAME,
  FIELD_EXTENSION_VERSION,
  FIELD_PENDING_VERSION,
  FIELD_PENDING_REQUESTED_AT,
  FIELD_PENDING_LAST_ERROR,
  FIELD_PENDING_LAST_ERROR_AT
};

const dd::String_type s_view_name{STRING_WITH_LEN("EXTENSIONS")};
const villagesql::system_views::Extensions *s_instance =
    new villagesql::system_views::Extensions(s_view_name);

}  // namespace

namespace villagesql {
namespace system_views {

const Extensions &Extensions::instance() { return *s_instance; }

Extensions::Extensions(const dd::String_type &n) {
  m_target_def.set_view_name(n);

  // SELECT fields. Without the COLLATE the columns inherit utf8mb4_bin
  // from the table, making user comparisons against them case-sensitive.
  m_target_def.add_field(FIELD_EXTENSION_NAME, "EXTENSION_NAME",
                         "ext.extension_name COLLATE utf8mb4_0900_ai_ci");
  m_target_def.add_field(FIELD_EXTENSION_VERSION, "EXTENSION_VERSION",
                         "ext.extension_version COLLATE utf8mb4_0900_ai_ci");
  // Pending-action projection. PendingAction supplies the SQL expressions
  // that yield each logical field, so the underlying storage shape stays
  // encapsulated. The columns are NULL when no action is pending.
  m_target_def.add_field(FIELD_PENDING_VERSION, "PENDING_VERSION",
                         PendingAction::TargetVersionSqlExpr("ext").c_str());
  m_target_def.add_field(FIELD_PENDING_REQUESTED_AT, "PENDING_REQUESTED_AT",
                         PendingAction::RequestedAtSqlExpr("ext").c_str());
  m_target_def.add_field(FIELD_PENDING_LAST_ERROR, "PENDING_LAST_ERROR",
                         PendingAction::LastErrorSqlExpr("ext").c_str());
  m_target_def.add_field(FIELD_PENDING_LAST_ERROR_AT, "PENDING_LAST_ERROR_AT",
                         PendingAction::LastErrorAtSqlExpr("ext").c_str());

  // FROM
  m_target_def.add_from("villagesql.extensions ext");

  // No WHERE clause - all users can see installed extensions
}

const dd::String_type &Extensions::view_name() { return s_view_name; }
}  // namespace system_views
}  // namespace villagesql
