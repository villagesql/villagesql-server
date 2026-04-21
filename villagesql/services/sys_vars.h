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

#ifndef VILLAGESQL_SERVICES_SYS_VARS_H_
#define VILLAGESQL_SERVICES_SYS_VARS_H_

#include <string>

#include "villagesql/sdk/include/villagesql/abi/types.h"
#include "villagesql/veb/veb_file.h"

class THD;

namespace villagesql {
namespace services {

// Register system variables declared by an extension as MySQL component system
// variables. Called after load_vef_extension(), outside the victionary lock.
// Returns false on success, true on error.
bool register_sys_vars_from_extension(
    const std::string &extension_name,
    const veb::ExtensionRegistration &ext_reg);

// Unregister system variables that belong to the given extension and, if thd
// is non-null, also remove any persisted values from mysqld-auto.cnf.
// Pass thd on explicit UNINSTALL EXTENSION; pass nullptr on server shutdown
// (persisted values should survive a shutdown/restart cycle).
void unregister_sys_vars_from_extension(const std::string &extension_name,
                                        THD *thd);

// Implementations of the system variable access functions passed to extensions
// via vef_register_arg_t.
bool get_variable(const char *component_name, const char *name, void **val,
                  size_t *val_len);
bool set_variable(const char *component_name, const char *name,
                  const char *scope, const char *val);

}  // namespace services
}  // namespace villagesql

#endif  // VILLAGESQL_SERVICES_SYS_VARS_H_
