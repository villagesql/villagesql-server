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

#ifndef VILLAGESQL_SERVICES_PREVIEW_MYSQL_SERVICES_H
#define VILLAGESQL_SERVICES_PREVIEW_MYSQL_SERVICES_H

// Server-side of the vsql::preview::mysql_services capability.
//
// on_populate reads the extension's capability_config (the list of services it
// consumes), acquires each from the MySQL component registry, and writes the
// pointer back into the extension. on_depopulate releases them at unload.
// Per-extension state is keyed by the config pointer.

#include <string>

#include "villagesql/services/capability_registry.h"

namespace villagesql::services {

bool on_populate_mysql_services(const PopulateContext &ctx,
                                std::string &error_message);
void on_depopulate_mysql_services(const DepopulateContext &ctx);

}  // namespace villagesql::services

#endif  // VILLAGESQL_SERVICES_PREVIEW_MYSQL_SERVICES_H
