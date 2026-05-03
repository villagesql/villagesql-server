// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

#ifndef VILLAGESQL_SERVICES_BACKGROUND_THREAD_H_
#define VILLAGESQL_SERVICES_BACKGROUND_THREAD_H_

#include "mysql/psi/mysql_cond.h"
#include "mysql/psi/mysql_mutex.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"

// Full definition of the opaque handle type. Defined here (not in the ABI
// header) so that both background_thread.cc and any server code that
// calls unregister_vef_background_thread() see the same complete type,
// while the extension-facing ABI only sees the forward declaration.
struct vef_thread_handle_t {
  class THD *thd;

  // Mutex and condition variable used by sleep_vef_background_thread().
  // enter_cond() registers these with the THD so that THD::awake() can
  // interrupt a sleeping thread (e.g. during UNINSTALL EXTENSION or shutdown).
  mysql_mutex_t sleep_mutex;
  mysql_cond_t sleep_cond;

  // Self-pipe used by sleep_vef_background_thread_fd(). stop() writes a byte
  // to stop_pipe[1] to wake a thread blocked in poll() on a user fd.
  // stop_pipe[0] = read end, stop_pipe[1] = write end. Both are -1 if the
  // pipe could not be created at registration time.
  int stop_pipe[2];
};

namespace villagesql {
namespace services {

// Register the PSI thread key used for all VEF extension background threads.
// Must be called once at server startup, before any extension is loaded.
void init_vef_background_thread_psi_key();

// Server-side implementations matching the ABI function pointer types in
// vef_register_arg_t. Signatures use the arg/result structs from types.h.
vef_register_background_thread_result_t register_vef_background_thread(
    const vef_register_background_thread_arg_t *arg);
void unregister_vef_background_thread(
    const vef_unregister_background_thread_arg_t *arg);
vef_sleep_background_thread_result_t sleep_vef_background_thread(
    const vef_sleep_background_thread_arg_t *arg);
vef_sleep_background_thread_result_t sleep_vef_background_thread_fd(
    const vef_sleep_background_thread_fd_arg_t *arg);
void stop_vef_background_thread(const vef_stop_background_thread_arg_t *arg);

}  // namespace services
}  // namespace villagesql

#endif  // VILLAGESQL_SERVICES_BACKGROUND_THREAD_H_
