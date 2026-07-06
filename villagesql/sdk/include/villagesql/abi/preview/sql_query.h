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

#ifndef VILLAGESQL_ABI_PREVIEW_SQL_QUERY_H
#define VILLAGESQL_ABI_PREVIEW_SQL_QUERY_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Preview capability: "vsql::preview::sql_query"
//
// Provides SQL execution for background threads. The server wraps
// MySQL's command service interface behind this vtable so that extensions
// require no MySQL plugin service headers.
//
// Two execution modes are provided:
//   execute + fetch_row  — buffers the full result set; suitable for small
//                          result sets or when random access is needed.
//   for_each_row         — invokes a callback per row as rows are produced,
//                          without buffering the full result set.
//
// Both modes return a vef_sql_result_t * which carries diagnostics
// (statement error + warnings). for_each_row's returned result has no
// buffered rows — it exists only to surface diagnostics. The result must
// be closed with close_result either way.
//
// Capability name: VEF_PREVIEW_SQL_QUERY_NAME

#define VEF_PREVIEW_SQL_QUERY_NAME "vsql::preview::sql_query"
#define VEF_PREVIEW_SQL_QUERY_ABI_VERSION 1

// Forward declaration — defined in abi/preview/thread_worker.h.
struct vef_thread_handle_t;

// Opaque handle for an open SQL session.
typedef struct vef_sql_session_t vef_sql_session_t;

// Opaque handle for a query result (rows + diagnostics).
typedef struct vef_sql_result_t vef_sql_result_t;

// Diagnostic severity. Matches MySQL's Sql_condition severity levels for the
// two levels exposed via SHOW WARNINGS.
typedef enum {
  VEF_SQL_DIAG_NOTE = 1,
  VEF_SQL_DIAG_WARNING = 2,
  VEF_SQL_DIAG_ERROR = 3,
} vef_sql_diag_severity_t;

// One diagnostic entry — either the statement error or a warning/note.
// All pointer fields point into server-owned storage tied to the lifetime
// of the vef_sql_result_t and are invalidated by close_result.
typedef struct {
  uint32_t errno_;
  vef_sql_diag_severity_t severity;
  // 5-char SQLSTATE + NUL. Never null.
  const char *sqlstate;
  // UTF-8 message. Never null; may be empty.
  const char *message;
  uint64_t message_len;
} vef_sql_diag_t;

// Open a session bound to the background thread's security context.
// Returns NULL on failure. Must be closed with close_session.
typedef vef_sql_session_t *(*vef_sql_open_session_fn)(
    struct vef_thread_handle_t *handle);

// Close a session opened with open_session.
typedef void (*vef_sql_close_session_fn)(vef_sql_session_t *session);

// Execute a SQL statement. sql is UTF-8, sql_len bytes.
// Returns a result handle. NULL only on allocation / setup failure
// (no session, OOM, etc.). A non-NULL result may still represent a failed
// query — check has_error() / get_error() on the returned handle.
// The entire result set is buffered before returning.
typedef vef_sql_result_t *(*vef_sql_execute_fn)(vef_sql_session_t *session,
                                                const char *sql,
                                                uint64_t sql_len);

// Row callback for for_each_row. Called once per row with the same row/lengths
// pointers as fetch_row. Return true to continue, false to stop early.
typedef bool (*vef_sql_row_cb)(const char **row, const unsigned long *lengths,
                               uint32_t num_columns, void *ctx);

// Execute a SQL statement and invoke cb once per row as rows are produced,
// without buffering the full result set. sql is UTF-8, sql_len bytes.
// Returns a result handle that carries diagnostics only — no buffered rows.
// NULL only on allocation / setup failure. Check has_error() / get_error()
// on the returned handle to distinguish a successful query from a failed one.
// The handle must be closed with close_result.
typedef vef_sql_result_t *(*vef_sql_for_each_row_fn)(vef_sql_session_t *session,
                                                     const char *sql,
                                                     uint64_t sql_len,
                                                     vef_sql_row_cb cb,
                                                     void *ctx);

// Fetch the next row. Returns true when a row was fetched; row_out and
// lengths_out are then valid until the next fetch_row or close_result call.
// NULL entries in row_out indicate SQL NULL column values.
typedef bool (*vef_sql_fetch_row_fn)(vef_sql_result_t *result,
                                     const char ***row_out,
                                     const unsigned long **lengths_out);

// Number of columns in the result set. Valid after a successful execute().
typedef uint32_t (*vef_sql_num_columns_fn)(vef_sql_result_t *result);

// Close the result handle. Must be called for every non-NULL handle returned
// by execute or for_each_row.
typedef void (*vef_sql_close_result_fn)(vef_sql_result_t *result);

// True if the statement produced an error.
typedef bool (*vef_sql_has_error_fn)(const vef_sql_result_t *result);

// Copy the statement error into *out. Returns true if there is an error;
// false (and leaves *out untouched) if the statement succeeded.
// out->message / out->sqlstate point into server-owned storage valid until
// close_result.
typedef bool (*vef_sql_get_error_fn)(const vef_sql_result_t *result,
                                     vef_sql_diag_t *out);

// Number of warning/note diagnostics attached to the result.
typedef uint32_t (*vef_sql_warning_count_fn)(const vef_sql_result_t *result);

// Copy the i-th warning into *out. Returns true if i < warning_count();
// false (and leaves *out untouched) otherwise.
typedef bool (*vef_sql_get_warning_fn)(const vef_sql_result_t *result,
                                       uint32_t i, vef_sql_diag_t *out);

// Server-provided vtable for SQL execution.
typedef struct {
  // Capability ABI version. Always the first field in every capability vtable.
  uint32_t version;

  // version >= 1
  vef_sql_open_session_fn open_session;
  vef_sql_close_session_fn close_session;
  vef_sql_execute_fn execute;
  vef_sql_fetch_row_fn fetch_row;
  vef_sql_num_columns_fn num_columns;
  vef_sql_close_result_fn close_result;
  vef_sql_for_each_row_fn for_each_row;
  vef_sql_has_error_fn has_error;
  vef_sql_get_error_fn get_error;
  vef_sql_warning_count_fn warning_count;
  vef_sql_get_warning_fn get_warning;
} vef_preview_sql_query_t;

#ifdef __cplusplus
}
#endif

#endif  // VILLAGESQL_ABI_PREVIEW_SQL_QUERY_H
