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

#ifndef VILLAGESQL_SERVICES_PREVIEW_SESSION_VAR_H
#define VILLAGESQL_SERVICES_PREVIEW_SESSION_VAR_H

#include <string>

#include "villagesql/sdk/include/villagesql/abi/preview/session_var.h"
#include "villagesql/services/capability_registry.h"

namespace villagesql::services {

// Returns the server-side vtable for the "vsql::session_var" preview
// capability.
vef_preview_session_var_t *preview_session_var_vtable();

// on_populate callback: registers the extension's session-scoped (THD-local)
// system variables with MySQL. Returns true on error (sets error_message),
// false on success.
bool on_populate_session_var(const PopulateContext &ctx,
                             std::string &error_message);

// on_depopulate callback: unregisters the extension's session variables.
void on_depopulate_session_var(const DepopulateContext &ctx);

}  // namespace villagesql::services

#endif  // VILLAGESQL_SERVICES_PREVIEW_SESSION_VAR_H
