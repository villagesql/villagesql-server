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

#ifndef VILLAGESQL_SCHEMA_UPGRADE_H_
#define VILLAGESQL_SCHEMA_UPGRADE_H_

#include "my_inttypes.h"

class THD;

namespace villagesql {
namespace upgrade {

// Version-specific upgrade functions
bool upgrade_villagesql_from_0_0_1_to_0_0_3(THD *thd);
bool upgrade_villagesql_from_0_0_4_to_0_0_5(THD *thd);

}  // namespace upgrade
}  // namespace villagesql

#endif  // VILLAGESQL_SCHEMA_UPGRADE_H_
