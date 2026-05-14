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
// Note: the current implementation buffers the entire result set in memory
// before the first row is returned, regardless of whether for_each or
// execute+fetch_row is used.  Avoid large result sets.
//
// TODO(villagesql-preview): Return richer error information to the extension
// rather than logging to the server error log and returning NULL.
//
// Capability name: VEF_PREVIEW_SQL_QUERY_NAME

#define VEF_PREVIEW_SQL_QUERY_NAME "vsql::preview::sql_query"
#define VEF_PREVIEW_SQL_QUERY_ABI_VERSION 1

// Forward declaration — defined in abi/preview/thread_worker.h.
struct vef_thread_handle_t;

// Opaque handle for an open SQL session.
typedef struct vef_sql_session_t vef_sql_session_t;

// Opaque handle for a buffered query result.
typedef struct vef_sql_result_t vef_sql_result_t;

// Open a session bound to the background thread's security context.
// Returns NULL on failure. Must be closed with close_session.
typedef vef_sql_session_t *(*vef_sql_open_session_fn)(
    struct vef_thread_handle_t *handle);

// Close a session opened with open_session.
typedef void (*vef_sql_close_session_fn)(vef_sql_session_t *session);

// Execute a SQL statement. sql is UTF-8, sql_len bytes.
// Returns a result handle on success, NULL on error.
typedef vef_sql_result_t *(*vef_sql_execute_fn)(vef_sql_session_t *session,
                                                const char *sql,
                                                size_t sql_len);

// Fetch the next row. Returns true when a row was fetched; row_out and
// lengths_out are then valid until the next fetch_row or close_result call.
// NULL entries in row_out indicate SQL NULL column values.
typedef bool (*vef_sql_fetch_row_fn)(vef_sql_result_t *result,
                                     const char ***row_out,
                                     const unsigned long **lengths_out);

// Number of columns in the result set. Valid after a successful execute().
typedef unsigned int (*vef_sql_num_columns_fn)(vef_sql_result_t *result);

// Close the result handle. Must be called for every handle returned by execute.
typedef void (*vef_sql_close_result_fn)(vef_sql_result_t *result);

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
} vef_preview_sql_query_t;

#ifdef __cplusplus
}
#endif

#endif  // VILLAGESQL_ABI_PREVIEW_SQL_QUERY_H
