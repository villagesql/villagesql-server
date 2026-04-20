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

#ifndef VILLAGESQL_SERVICES_STATUS_VARS_H_
#define VILLAGESQL_SERVICES_STATUS_VARS_H_

#include <string>

#include "villagesql/sdk/include/villagesql/abi/types.h"
#include "villagesql/veb/veb_file.h"

namespace villagesql {
namespace services {

// Register status variables declared by an extension via the MySQL
// status_variable_registration component service. Called after successful
// install, outside the victionary lock. Returns false on success, true on
// error.
bool register_status_vars_from_extension(
    const std::string &extension_name,
    const veb::ExtensionRegistration &ext_reg);

// Unregister status variables that belong to the given extension.
// Called when an extension is uninstalled.
void unregister_status_vars_from_extension(const std::string &extension_name);

}  // namespace services
}  // namespace villagesql

#endif  // VILLAGESQL_SERVICES_STATUS_VARS_H_
