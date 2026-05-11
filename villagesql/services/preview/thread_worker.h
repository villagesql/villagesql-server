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

#ifndef VILLAGESQL_SERVICES_PREVIEW_THREAD_WORKER_H
#define VILLAGESQL_SERVICES_PREVIEW_THREAD_WORKER_H

#include <string_view>

#include "mysql/psi/mysql_cond.h"
#include "mysql/psi/mysql_mutex.h"
#include "villagesql/sdk/include/villagesql/abi/preview/thread_worker.h"

// Full definition of the opaque handle type. Defined here (not in the ABI
// header) so that the server-side implementation can see the complete type,
// while extension-facing ABI only sees the forward declaration.
struct vef_thread_handle_t {
  class THD *thd;

  // Mutex and condition variable used by timed sleep.
  mysql_mutex_t sleep_mutex;
  mysql_cond_t sleep_cond;

  // Self-pipe used when poll_fd sleep is requested. stop() writes a byte to
  // stop_pipe[1] to wake a thread blocked in poll() on a user fd.
  // stop_pipe[0] = read end, stop_pipe[1] = write end. Both -1 if the
  // pipe could not be created at registration time.
  int stop_pipe[2];
};

namespace villagesql::services {

// Register the PSI thread/mutex/cond keys for extension background threads.
// Called once at server startup from register_builtin_capabilities().
void init_thread_worker_psi_keys();

// Called at extension load time (via capability on_populate) to store the
// descriptor and extension name so the server can start the thread later.
void on_populate_thread_worker(std::string_view extension_name,
                               const void *extension_data);

// Called at extension unload time to stop any running thread and remove the
// worker state for the given descriptor.
void on_depopulate_thread_worker(const void *extension_data);

// Returns the server-side vtable for the "vsql::thread_worker" capability.
vef_preview_thread_worker_t *preview_thread_worker_vtable();

}  // namespace villagesql::services

#endif  // VILLAGESQL_SERVICES_PREVIEW_THREAD_WORKER_H
