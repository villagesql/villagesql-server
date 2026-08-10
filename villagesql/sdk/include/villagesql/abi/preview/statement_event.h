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

#ifndef VILLAGESQL_ABI_PREVIEW_STATEMENT_EVENT_H
#define VILLAGESQL_ABI_PREVIEW_STATEMENT_EVENT_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "../types.h"  // VEF_MAX_ERROR_LEN

#ifdef __cplusplus
extern "C" {
#endif

// Preview capability: "vsql::preview::statement_event"
//
// An extension declares one StatementEventCapability per handler function. The
// server invokes registered handlers at the corresponding phase of query
// processing. Only the POSTEXECUTE phase is wired up in this version; the other
// phase values are reserved for future use without ABI break.
//
// Capability name: VEF_PREVIEW_STATEMENT_EVENT_NAME

#define VEF_PREVIEW_STATEMENT_EVENT_NAME "vsql::preview::statement_event"

// Capability ABI version compiled into this SDK snapshot.
#define VEF_PREVIEW_STATEMENT_EVENT_ABI_VERSION 1

// Phase at which a handler fires.
//
// Only POSTEXECUTE is implemented today. The other values are reserved so
// later versions can implement them without renumbering the ABI. Until they
// are wired up, the server rejects INSTALL EXTENSION for any extension that
// declares a handler for a reserved phase.
typedef enum {
  // Reserved: client connection established. Fires from
  // EVENT_TRACKING_CONNECTION_CONNECT. Args populated: user, client_ip,
  // connection_id, port. Query fields and timing are unset.
  // Result.error_msg refuses the connection.
  VEF_STATEMENT_EVENT_CONNECT = 0,

  // Reserved: client connection closing. Fires from
  // EVENT_TRACKING_CONNECTION_DISCONNECT. Same args as CONNECT.
  // Result is ignored — the connection is going away.
  VEF_STATEMENT_EVENT_DISCONNECT = 1,

  // Reserved: before the parser runs. Only `query` is populated. Registered
  // inside sql_parse.cc rather than the audit path. Result.error_msg
  // blocks the query.
  VEF_STATEMENT_EVENT_PREPARSE = 2,

  // Reserved: after the parser runs. Adds sql_command, is_prepared, and
  // the SHA-256 digest of the normalized query to args. Result.error_msg
  // blocks the query.
  VEF_STATEMENT_EVENT_POSTPARSE = 3,

  // Reserved: after parsing, before execution begins. Fires from
  // EVENT_TRACKING_QUERY_START. Args populated: query, user, client_ip,
  // connection_id, port, schema, sql_command, in_transaction. Timing/rows/
  // status are zero. Result.error_msg blocks the query.
  VEF_STATEMENT_EVENT_PREEXECUTE = 4,

  // After execution completes (success or failure). All args fields are
  // populated; result.error_msg is logged but does not affect the client.
  VEF_STATEMENT_EVENT_POSTEXECUTE = 5,
} vef_statement_event_phase_t;

// Read-only arguments passed to a handler invocation. Which fields are
// populated depends on the phase; POSTEXECUTE populates all of them.
//
// Pointer lifetime summary (see per-field comments):
//   process lifetime    — safe to retain indefinitely; no copy needed.
//   connection lifetime — valid until the client disconnects; safe to retain
//                         across handler calls for the same connection, but
//                         copy if you may use it after the connection ends.
//   copy before return  — valid only during this handler invocation; copy the
//                         string before the handler returns if you need
//                         to keep it.
typedef struct {
  vef_statement_event_phase_t phase;

  // Query text. Not null-terminated; use query_len. This is the server's
  // rewritten (redacted) form when one exists -- the same text the general,
  // slow, and binary logs use -- so credential-bearing statements (SET
  // PASSWORD, CREATE/ALTER USER ... IDENTIFIED BY, replication SOURCE_PASSWORD,
  // CREATE SERVER OPTIONS(PASSWORD ...)) arrive with the secret obfuscated, not
  // in cleartext. For grouping regardless of literals, prefer digest_text.
  // Lifetime: copy before return.
  const char *query;
  size_t query_len;

  // Authenticated user name, or empty string.
  // Lifetime: connection lifetime.
  const char *user;
  // Client IP address, or empty string.
  // Lifetime: connection lifetime.
  const char *client_ip;
  unsigned long connection_id;
  uint16_t port;
  bool in_transaction;

  // SQL command as a lowercase string (e.g. "select", "insert",
  // "create_table"). Stable across MySQL versions. NULL if unknown.
  // Lifetime: process lifetime.
  const char *sql_command;

  // Current default schema (USE <db>), or NULL if none selected.
  // Lifetime: connection lifetime.
  const char *schema;

  // Execution status. 0 on success, otherwise the MySQL error code.
  int status;
  // 5-character SQLSTATE + NUL, or NULL on success.
  // Lifetime: copy before return.
  const char *sqlstate;
  // Error message text, or NULL on success.
  // Lifetime: copy before return.
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

  // Number of warnings raised during execution (in addition to any error).
  uint64_t warning_count;

  // Normalized (digested) form of the query, suitable for grouping similar
  // queries regardless of literal values. NULL if digest computation was
  // disabled or the query was too long to digest.
  // Lifetime: copy before return.
  const char *digest_text;

  // Optimizer and execution quality indicators. Non-zero values suggest
  // potentially inefficient execution.
  uint64_t select_full_join;        // joins without usable index
  uint64_t select_full_range_join;  // joins using range on ref table
  uint64_t select_range;            // range scans on first table
  uint64_t select_range_check;      // joins with key check per row
  uint64_t select_scan;             // full scans of first table

  // Sort metrics.
  uint64_t sort_merge_passes;  // number of merge passes (high = large sort)
  uint64_t sort_range;         // sorts using a range
  uint64_t sort_rows;          // rows sorted
  uint64_t sort_scan;          // sorts using a full table scan

  // Temporary table usage.
  uint64_t created_tmp_tables;       // tmp tables created (memory or disk)
  uint64_t created_tmp_disk_tables;  // tmp tables spilled to disk

  // Index usage flags. Non-zero means the query ran without a usable index.
  uint8_t no_index_used;       // 1 if no index was used
  uint8_t no_good_index_used;  // 1 if no good index was found

  // --- Added in vtable_hash "ver-2" ---

  // Statement identity hash: the value performance_schema exposes as DIGEST
  // (SHA-256 over the token stream, 64 lowercase hex chars). A compact GROUP BY
  // key, stable across servers. NULL when digest_text is. Copy before return.
  const char *digest_hash;

  // Handler row-access counters (per-statement deltas): the ha_read_* call
  // counts, i.e. the slow log's Read_* fields. Quantifies the access method the
  // no_index_used / select_scan flags only flag.
  uint64_t read_first;     // index-first reads (MIN / index start)
  uint64_t read_last;      // index-last reads (MAX / index end)
  uint64_t read_key;       // reads via an index lookup
  uint64_t read_next;      // forward index-order walks (range scan)
  uint64_t read_prev;      // backward index-order walks (reverse scan)
  uint64_t read_rnd;       // reads by position (post-filesort row fetch)
  uint64_t read_rnd_next;  // next-row in a full scan (high = table scan)
} vef_statement_event_args_t;

// Writable result. For POSTEXECUTE error_msg is advisory: the server logs it
// but does not propagate to the client.
typedef struct {
  // Caller-provided buffer for error message (size VEF_MAX_ERROR_LEN).
  // Write a null-terminated string here if an error occurred; leave it empty
  // (first byte '\0') otherwise. The server pre-allocates the buffer and sets
  // this pointer before calling the handler. Matches the convention used by
  // vef_vdf_result_t and vef_prerun_result_t.
  char *error_msg;
} vef_statement_event_result_t;

// Handler type. Invoked synchronously on the query's thread.
// args is read-only; result is the extension's writeback channel.
typedef void (*vef_statement_event_fn_t)(const vef_statement_event_args_t *args,
                                         vef_statement_event_result_t *result);

// Capability config (cc) filled in by the extension and passed to the server
// via vef_required_capability_t.capability_config. The phase determines when
// the handler fires; the function pointer must remain valid for the lifetime of
// the extension.
typedef struct {
  vef_statement_event_phase_t phase;
  vef_statement_event_fn_t handler;
} vef_statement_event_cc_t;

// Server-side vtable. The version field is always first, matching the
// convention used by other preview capabilities. The server exposes no
// methods to the extension for this capability — registration happens via
// capability_config.
typedef struct {
  uint32_t version;
} vef_preview_statement_event_t;

#ifdef __cplusplus
}
#endif

#endif  // VILLAGESQL_ABI_PREVIEW_STATEMENT_EVENT_H
