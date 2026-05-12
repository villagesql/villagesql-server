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

#ifndef VSQL_MYSQL_SERVICES_REGISTRY_ABI_H
#define VSQL_MYSQL_SERVICES_REGISTRY_ABI_H

// Bridge ABI for the MySQL Services airlock channels. Shared between the
// server-side bridge (which handles the request) and the extension-side
// bridge SDK (which builds it).
//
// Two channels:
//   vsql::mysql_service_required/v1   — request the server acquire a service
//                                       and write the pointer to *destination
//   vsql::mysql_service_provided/v1   — request the server register an impl
//
// "Push" model: the server does all the work synchronously inside the
// airlock dispatch. No callback into the extension. Server tracks acquired
// handles and registered impls per-extension for cleanup at unload.

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define VSQL_MYSQL_SERVICE_REQUIRED_V1_CHANNEL "vsql::mysql_service_required/v1"

#define VSQL_MYSQL_SERVICE_PROVIDED_V1_CHANNEL "vsql::mysql_service_provided/v1"

// Payload for a "required service" airlock request. The server acquires
// `service_name` (default implementation) from the MySQL component registry
// and writes the resulting SERVICE_TYPE* into *destination. On error the
// extension load is aborted with a message naming the missing service.
typedef struct {
  const char *service_name;
  const void **destination;
} vsql_mysql_service_required_v1_t;

// Payload for a "provided service" airlock request. The server registers
// `impl` (a pointer to a SERVICE_TYPE_NO_CONST struct in the extension's
// .so) under `full_name`, which must be of the form
// "service_name.implementation_name".
typedef struct {
  const char *full_name;
  const void *impl;
} vsql_mysql_service_provided_v1_t;

#ifdef __cplusplus
}
#endif

#endif  // VSQL_MYSQL_SERVICES_REGISTRY_ABI_H
