/* Copyright (c) 2026 VillageSQL Contributors

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef VSQL_CLONE_PROTOCOL_SERVICE
#define VSQL_CLONE_PROTOCOL_SERVICE

/**
  @file
  This service lets the clone plugin gather and validate VillageSQL-specific
  information (currently installed extensions) without linking against
  VillageSQL server internals directly. It mirrors the way the upstream
  clone_protocol service exposes charset/config gathering and validation, but
  is owned entirely by VillageSQL so upstream files stay untouched.

  The extension payload is opaque to the clone plugin: the donor obtains it
  via mysql_vsql_clone_get_extensions() and ships it verbatim to the recipient,
  which passes it to mysql_vsql_clone_validate_extensions(). Only villagesql/veb
  interprets the bytes.
*/

#ifdef __cplusplus
class THD;
#else
#define THD void
#endif

#include <mysql/components/service.h>
#include <stddef.h>

// The methods use a C ABI (no std::string across the boundary): this service is
// implemented in villagesql/services and registered in the server component,
// which are compiled into separate objects; a std::string in the signature
// would mangle inconsistently between them (the libstdc++ CXX11 string ABI) and
// fail to link. Almost all MySQL component services use C types for the same
// reason.

BEGIN_SERVICE_DEFINITION(vsql_clone_protocol)

/**
  Get the opaque payload describing the installed VillageSQL extensions, for the
  donor to send to the recipient. On success *payload points to a heap buffer of
  *length bytes that the caller must release with mysql_vsql_clone_free_payload;
  *payload is null and *length is 0 when nothing is installed.
  @param[in,out]  thd      server session THD
  @param[out]     payload  opaque extension payload (encoding private to
                           villagesql/veb); heap-allocated, caller frees
  @param[out]     length   payload length in bytes
  @return error code.
*/
DECLARE_METHOD(int, mysql_vsql_clone_get_extensions,
               (THD * thd, unsigned char **payload, size_t *length));

/**
  Release a payload buffer returned by mysql_vsql_clone_get_extensions().
  @param[in]  payload  buffer to free (may be null)
*/
DECLARE_METHOD(void, mysql_vsql_clone_free_payload, (unsigned char *payload));

/**
  Validate that every VillageSQL extension described by a payload from
  mysql_vsql_clone_get_extensions() is available on this (recipient) server.
  @param[in,out]  thd          server session THD
  @param[in]      payload      opaque extension payload from the donor
  @param[in]      length       payload length in bytes
  @param[out]     err_buf      buffer for a human-readable reason on mismatch
  @param[in]      err_buf_len  size of err_buf in bytes
  @return error code (0 = all available).
*/
DECLARE_METHOD(int, mysql_vsql_clone_validate_extensions,
               (THD * thd, const unsigned char *payload, size_t length,
                char *err_buf, size_t err_buf_len));

END_SERVICE_DEFINITION(vsql_clone_protocol)

#endif /* VSQL_CLONE_PROTOCOL_SERVICE */
