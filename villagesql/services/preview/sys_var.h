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

#ifndef VILLAGESQL_SERVICES_PREVIEW_SYS_VAR_H
#define VILLAGESQL_SERVICES_PREVIEW_SYS_VAR_H

#include <string>
#include <string_view>

#include "villagesql/sdk/include/villagesql/abi/preview/sys_var.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"
#include "villagesql/services/capability_registry.h"

namespace villagesql::services {

// Returns the server-side vtable for the "vsql::sys_var" preview capability.
vef_preview_sys_var_t *preview_sys_var_vtable();

// on_populate callback: registers the extension's system variables with MySQL.
// Returns true on error (sets error_message), false on success.
bool on_populate_sys_var(const PopulateContext &ctx,
                         std::string &error_message);

// on_depopulate callback: unregisters system variables for this extension.
// When ctx.reason is kUninstall (explicit UNINSTALL EXTENSION), also removes
// persisted values from mysqld-auto.cnf using ctx.thd. On kShutdown, persisted
// values are intentionally left intact to survive a shutdown/restart cycle.
void on_depopulate_sys_var(const DepopulateContext &ctx);

}  // namespace villagesql::services

#endif  // VILLAGESQL_SERVICES_PREVIEW_SYS_VAR_H
