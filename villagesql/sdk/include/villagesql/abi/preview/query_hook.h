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
// GNU General Public License, version 2.0, for more details.
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

#ifndef VILLAGESQL_ABI_PREVIEW_QUERY_HOOK_H
#define VILLAGESQL_ABI_PREVIEW_QUERY_HOOK_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "../types.h"  // VEF_MAX_ERROR_LEN

#ifdef __cplusplus
extern "C" {
#endif

// Preview capability: "vsql::preview::query_hook"
//
// An extension declares one QueryHookCapability per hook function. The server
// invokes registered hooks at the corresponding phase of query processing.
// Only the POSTEXECUTE phase is wired up in this version; the other phase
// values are reserved for future use without ABI break.
//
// Capability name: VEF_PREVIEW_QUERY_HOOK_NAME

#define VEF_PREVIEW_QUERY_HOOK_NAME "vsql::preview::query_hook"

// Capability ABI version compiled into this SDK snapshot.
#define VEF_PREVIEW_QUERY_HOOK_ABI_VERSION 1

// Phase at which a hook fires.
//
// Only POSTEXECUTE is implemented today. The other values are reserved so
// later versions can implement them without renumbering the ABI. Until they
// are wired up, the server rejects INSTALL EXTENSION for any extension that
// declares a hook for a reserved phase.
typedef enum {
  // Reserved: client connection established. Fires from
  // EVENT_TRACKING_CONNECTION_CONNECT. Args populated: user, host,
  // connection_id, port. Query fields and timing are unset.
  // Result.error_msg refuses the connection.
  VEF_QUERY_HOOK_CONNECT = 0,

  // Reserved: client connection closing. Fires from
  // EVENT_TRACKING_CONNECTION_DISCONNECT. Same args as CONNECT.
  // Result is ignored — the connection is going away.
  VEF_QUERY_HOOK_DISCONNECT = 1,

  // Reserved: before the parser runs. Only `query` is populated. Hooked
  // inside sql_parse.cc rather than the audit path. Result.error_msg
  // blocks the query.
  VEF_QUERY_HOOK_PREPARSE = 2,

  // Reserved: after the parser runs. Adds sql_command, is_prepared, and
  // the SHA-256 digest of the normalized query to args. Result.error_msg
  // blocks the query.
  VEF_QUERY_HOOK_POSTPARSE = 3,

  // Reserved: after parsing, before execution begins. Fires from
  // EVENT_TRACKING_QUERY_START. Args populated: query, user, host,
  // connection_id, port, schema, sql_command, in_transaction. Timing/rows/
  // status are zero. Result.error_msg blocks the query.
  VEF_QUERY_HOOK_PREEXECUTE = 4,

  // After execution completes (success or failure). All args fields are
  // populated; result.error_msg is logged but does not affect the client.
  VEF_QUERY_HOOK_POSTEXECUTE = 5,
} vef_query_hook_phase_t;

// Read-only arguments passed to a hook invocation. Which fields are populated
// depends on the phase; POSTEXECUTE populates all of them.
typedef struct {
  vef_query_hook_phase_t phase;

  // Query text. Not null-terminated; use query_len.
  const char *query;
  size_t query_len;

  // Authenticated user name (priv_user), or empty string.
  const char *user;
  // Client IP address, or empty string.
  const char *host;
  unsigned long connection_id;
  uint16_t port;
  bool in_transaction;

  // SQL command as a lowercase string (e.g. "select", "insert",
  // "create_table"). Stable across MySQL version rebases. NULL if unknown.
  // The pointed-to string has static lifetime and need not be copied.
  const char *sql_command;

  // Default schema (USE <db>), or NULL if none selected.
  const char *schema;

  // Execution status. 0 on success, otherwise the MySQL error code.
  int status;
  // 5-character SQLSTATE + NUL, or NULL on success.
  const char *sqlstate;
  // Error message text, or NULL on success.
  const char *error_message;

  // Query start time, microseconds since epoch.
  uint64_t query_start_utime;
  double query_time_secs;
  double lock_time_secs;
  uint64_t rows_sent;
  uint64_t rows_examined;
  uint64_t rows_affected;
  uint64_t bytes_sent;
  uint64_t bytes_received;
} vef_query_hook_args_t;

// Writable result. For POSTEXECUTE error_msg is advisory: the server logs it
// but does not propagate to the client.
typedef struct {
  // Caller-provided buffer for error message (size VEF_MAX_ERROR_LEN).
  // Write a null-terminated string here if an error occurred; leave it empty
  // (first byte '\0') otherwise. The server pre-allocates the buffer and sets
  // this pointer before calling the hook. Matches the convention used by
  // vef_vdf_result_t and vef_prerun_result_t.
  char *error_msg;
} vef_query_hook_result_t;

// Hook callback type. Invoked synchronously on the query's thread.
// args is read-only; result is the extension's writeback channel.
typedef void (*vef_query_hook_fn_t)(const vef_query_hook_args_t *args,
                                    vef_query_hook_result_t *result);

// Capability config (cc) filled in by the extension and passed to the server
// via vef_required_capability_t.capability_config. The phase determines when
// the hook fires; the function pointer must remain valid for the lifetime of
// the extension.
typedef struct {
  vef_query_hook_phase_t phase;
  vef_query_hook_fn_t hook;
} vef_query_hook_cc_t;

// Server-side vtable. The version field is always first, matching the
// convention used by other preview capabilities. The server exposes no
// methods to the extension for this capability — registration happens via
// capability_config.
typedef struct {
  uint32_t version;
} vef_preview_query_hook_t;

#ifdef __cplusplus
}
#endif

#endif  // VILLAGESQL_ABI_PREVIEW_QUERY_HOOK_H
