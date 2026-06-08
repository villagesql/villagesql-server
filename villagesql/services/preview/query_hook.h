// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, see <https://www.gnu.org/licenses/>.

#ifndef VILLAGESQL_SERVICES_PREVIEW_QUERY_HOOK_H
#define VILLAGESQL_SERVICES_PREVIEW_QUERY_HOOK_H

#include <string>

#include "villagesql/sdk/include/villagesql/abi/preview/query_hook.h"
#include "villagesql/services/capability_registry.h"

class THD;

namespace villagesql::services {

// Called at extension load time (via capability on_populate) to register
// the hook in the global per-phase dispatch list.
bool on_populate_query_hook(const PopulateContext &ctx,
                            std::string &error_message);

// Called at extension unload time to remove the hook with this capability
// config from the global dispatch list.
void on_depopulate_query_hook(const DepopulateContext &ctx);

// Returns the server-side vtable for the "vsql::preview::query_hook"
// capability.
vef_preview_query_hook_t *preview_query_hook_vtable();

// Invoked from mysql_event_tracking_query_notify in sql_audit.cc on
// EVENT_TRACKING_QUERY_STATUS_END. Dispatches to registered POSTEXECUTE
// hooks. No-op if no hooks are registered.
void on_query_status_end(THD *thd);

}  // namespace villagesql::services

#endif  // VILLAGESQL_SERVICES_PREVIEW_QUERY_HOOK_H
