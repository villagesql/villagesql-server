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

#include "villagesql/schema/systable/properties.h"

#include "sql/field.h"
#include "sql/table.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/systable/helpers.h"

namespace villagesql {

bool TableTraits<PropertyEntry>::read_from_table(TABLE &table,
                                                 PropertyEntry &entry) {
  Field **field = table.field;

  // name (field 0) - read key component
  std::string prop_name;
  read_string_field(field[0], prop_name);
  entry.set_key(PropertyKey(std::move(prop_name)));

  // value (field 1)
  read_string_field(field[1], entry.value);

  // description (field 2)
  read_string_field(field[2], entry.description);

  return false;  // Success
}

bool TableTraits<PropertyEntry>::write_to_table(TABLE &table,
                                                const PropertyEntry &entry) {
  Field **field = table.field;

  // Set all fields for the row
  // name (field 0) - required
  field[0]->store(entry.name().c_str(), entry.name().length(),
                  &my_charset_utf8mb4_bin);

  // value (field 1) - can be NULL
  if (!entry.value.empty()) {
    field[1]->store(entry.value.c_str(), entry.value.length(),
                    &my_charset_utf8mb4_bin);
  } else {
    field[1]->set_null();
  }

  // description (field 2) - can be NULL
  if (!entry.description.empty()) {
    field[2]->store(entry.description.c_str(), entry.description.length(),
                    &my_charset_utf8mb4_bin);
  } else {
    field[2]->set_null();
  }

  // Write the row - use ha_write_row for INSERT
  int error = table.file->ha_write_row(table.record[0]);
  if (should_assert_if_true(error)) {
    LogVSQL(ERROR_LEVEL, "Failed to write property '%s': error %d",
            entry.name().c_str(), error);
    return true;
  }

  return false;  // Success
}

}  // namespace villagesql
