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

#ifndef VILLAGESQL_SERVICES_VSQL_CLONE_PROTOCOL_IMP_H_
#define VILLAGESQL_SERVICES_VSQL_CLONE_PROTOCOL_IMP_H_

#include <mysql/components/component_implementation.h>

#include "villagesql/services/vsql_clone_protocol_service.h"

#include <stddef.h>

class THD;

// Server-side implementation of the vsql_clone_protocol service. Registered in
// sql/server_component/server_component.cc. Thin wrappers over villagesql/veb.

DEFINE_METHOD(int, mysql_vsql_clone_get_extensions,
              (THD * thd, unsigned char **payload, size_t *length));

DEFINE_METHOD(void, mysql_vsql_clone_free_payload, (unsigned char *payload));

DEFINE_METHOD(int, mysql_vsql_clone_validate_extensions,
              (THD * thd, const unsigned char *payload, size_t length,
               char *err_buf, size_t err_buf_len));

#endif  // VILLAGESQL_SERVICES_VSQL_CLONE_PROTOCOL_IMP_H_
