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

#ifndef VILLAGESQL_SERVICES_RUN_QUERY_H_
#define VILLAGESQL_SERVICES_RUN_QUERY_H_

#include "villagesql/sdk/include/villagesql/abi/types.h"

// Server-side definition of the opaque vef_thread_t handle. Extensions see
// only the forward declaration in abi/types.h; the server owns the definition.
// TODO(villagesql): populate this with actual thread state once background
// thread registration is implemented.
struct vef_thread_t {};

namespace villagesql {
namespace services {

// Server-side implementation of the run_query service exposed to extensions
// via vef_register_arg_t.
//
// Executes `sql` using an internal server session (mysql_admin_session) with
// the "skip_grants" security context (same as other internal server services).
// Results are streamed to the caller via the meta_cb and row_cb callbacks.
//
// Must be called from a thread that has been initialized for server use (i.e.,
// a thread registered via register_vef_background_thread, or the main server
// thread). Calling from an uninitialized thread results in undefined behavior.
vef_run_query_result_t run_query(vef_thread_t *thread, const char *sql,
                                 size_t sql_len, vef_column_meta_fn meta_cb,
                                 vef_row_fn row_cb, void *ctx, char *error_msg);

}  // namespace services
}  // namespace villagesql

#endif  // VILLAGESQL_SERVICES_RUN_QUERY_H_
