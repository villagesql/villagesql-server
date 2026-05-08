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

#ifndef VILLAGESQL_SERVICES_PREVIEW_PING_H
#define VILLAGESQL_SERVICES_PREVIEW_PING_H

#include "villagesql/sdk/include/villagesql/abi/preview/ping.h"
#include "villagesql/services/capability_registry.h"

namespace villagesql::services {

// Returns the server-side vtable for the "vsql::preview::ping" preview
// capability.
vef_preview_ping_t *preview_ping_vtable();

// Custom compatibility check for the ping capability.
// Accepts extensions compiled against any ping ABI version the server
// satisfies (min_version <= server version), skipping the strict hash check.
// This allows extensions compiled against a future ping ABI (e.g. with pong)
// to load against an older server, with the extension responsible for guarding
// access to fields beyond what the server provides.
bool preview_ping_compat(const vef_required_capability_t &req, void *vtable,
                         std::string &error_message);

}  // namespace villagesql::services

#endif  // VILLAGESQL_SERVICES_PREVIEW_PING_H
