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

#ifndef VILLAGESQL_SYSTEM_VIEWS__EXTENSIONS_INCLUDED
#define VILLAGESQL_SYSTEM_VIEWS__EXTENSIONS_INCLUDED

#include "sql/dd/impl/system_views/system_view_definition_impl.h"
#include "sql/dd/impl/system_views/system_view_impl.h"

namespace villagesql {
namespace system_views {

/**
   The class representing INFORMATION_SCHEMA.EXTENSIONS system
   view definition.

   Exposes VillageSQL extension registry information from
   villagesql.extensions table.
*/
class Extensions : public dd::system_views::System_view_impl<
                       dd::system_views::System_view_select_definition_impl> {
 public:
  using dd::system_views::System_view_impl<
      dd::system_views::System_view_select_definition_impl>::view_definition;

  explicit Extensions(const dd::String_type &);

  static const Extensions &instance();
  static const dd::String_type &view_name();
  const dd::String_type &name() const override {
    return Extensions::view_name();
  }
};

}  // namespace system_views
}  // namespace villagesql

#endif  // VILLAGESQL_SYSTEM_VIEWS__EXTENSIONS_INCLUDED
