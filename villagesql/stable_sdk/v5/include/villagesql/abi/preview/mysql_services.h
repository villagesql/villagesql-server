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

// =============================================================================
// VEF PREVIEW ABI HEADER — UNSTABLE BINARY INTERFACE
// =============================================================================
// This header is both:
//   - an ABI header — extension authors should use the C++ API in
//     <villagesql/vsql.h>, not these raw types. See villagesql/abi/README.md.
//   - a preview capability — API and ABI may change or be removed without
//     notice. See villagesql/preview/README.md.
// =============================================================================

#ifndef VILLAGESQL_ABI_PREVIEW_MYSQL_SERVICES_H
#define VILLAGESQL_ABI_PREVIEW_MYSQL_SERVICES_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Preview capability: "vsql::preview::mysql_services"
//
// Lets an extension consume MySQL registry services (provided by either a
// component or the server core). The extension declares every service it uses
// in one capability_config; the server acquires them at load and releases them
// at unload.
//
// (Providing a service — an extension registering its own implementation into
// the registry — is a planned follow-up and is not part of this capability
// yet.)
//
// Capability name: VEF_PREVIEW_MYSQL_SERVICES_NAME

#define VEF_PREVIEW_MYSQL_SERVICES_NAME "vsql::preview::mysql_services"

// One service the extension consumes. The server acquires `service_name`'s
// default implementation from the MySQL component registry and writes the
// resulting SERVICE_TYPE* into *destination.
typedef struct {
  const char *service_name;
  const void **destination;
} vef_mysql_service_required_t;

// capability_config passed in vef_required_capability_t. The array points into
// the extension's MysqlServices wrapper and must outlive registration.
typedef struct {
  const vef_mysql_service_required_t *required;
  size_t required_count;
} vef_preview_mysql_services_t;

#ifdef __cplusplus
}
#endif

#endif  // VILLAGESQL_ABI_PREVIEW_MYSQL_SERVICES_H
