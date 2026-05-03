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

#include "villagesql/services/background_thread.h"

#include <poll.h>
#include <unistd.h>

#include "my_systime.h"
#include "mysql/psi/mysql_cond.h"
#include "mysql/psi/mysql_mutex.h"
#include "mysql/psi/mysql_thread.h"
#include "sql/mysqld.h"
#include "sql/mysqld_thd_manager.h"
#include "sql/protocol_classic.h"
#include "sql/sql_class.h"
#include "sql/sql_thd_internal_api.h"
#include "villagesql/include/error.h"

#ifdef HAVE_PSI_INTERFACE
#include "mysql/psi/psi_thread.h"
#endif

namespace villagesql {
namespace services {

// PSI thread key shared by all VEF extension background threads.
// Registered at server startup via init_vef_background_thread_psi_key().
PSI_thread_key key_thread_vef_extension_worker;

// PSI mutex/cond keys for the per-handle sleep synchronisation primitives.
PSI_mutex_key key_mutex_vef_sleep;
PSI_cond_key key_cond_vef_sleep;

#ifdef HAVE_PSI_INTERFACE
static PSI_thread_info vef_extension_worker_thread_info[] = {
    {&key_thread_vef_extension_worker, "vef_extension_worker", "vef_wkr", 0, 0,
     PSI_DOCUMENT_ME}};

static PSI_mutex_info vef_sleep_mutex_info[] = {
    {&key_mutex_vef_sleep, "vef_sleep_mutex", 0, 0, PSI_DOCUMENT_ME}};

static PSI_cond_info vef_sleep_cond_info[] = {
    {&key_cond_vef_sleep, "vef_sleep_cond", 0, 0, PSI_DOCUMENT_ME}};
#endif

void init_vef_background_thread_psi_key() {
#ifdef HAVE_PSI_INTERFACE
  const char *category = "villagesql";
  mysql_thread_register(
      category, vef_extension_worker_thread_info,
      static_cast<int>(array_elements(vef_extension_worker_thread_info)));
  mysql_mutex_register(category, vef_sleep_mutex_info, 1);
  mysql_cond_register(category, vef_sleep_cond_info, 1);
#endif
}

vef_register_background_thread_result_t register_vef_background_thread(
    const vef_register_background_thread_arg_t *arg) {
  const char *thread_name = arg->thread_name;
  if (my_thread_init()) {
    LogVSQL(ERROR_LEVEL,
            "register_vef_background_thread: my_thread_init() failed");
    return {nullptr, "my_thread_init() failed"};
  }

  THD *thd = new (std::nothrow) THD;
  if (thd == nullptr) {
    LogVSQL(ERROR_LEVEL,
            "register_vef_background_thread: failed to allocate THD");
    return {nullptr, "failed to allocate THD"};
  }

  thd->system_thread = SYSTEM_THREAD_BACKGROUND;
  thd->security_context()->skip_grants();
  thd->security_context()->set_host_or_ip_ptr(my_localhost,
                                              strlen(my_localhost));
  thd->security_context()->set_user_ptr(STRING_WITH_LEN("vef_worker"));
  thd->get_protocol_classic()->init_net(nullptr);
  thd->set_new_thread_id();
  thd->set_command(COM_DAEMON);
  thd->set_proc_info(thread_name);
  thd->set_time();
  thd->variables.lock_wait_timeout = LONG_TIMEOUT;

  // thread_stack must be set before calling store_globals().
  // We use a local variable address as an approximation of the thread stack
  // top; this is the same approach used by event_scheduler_thread().
  thd->thread_stack = reinterpret_cast<char *>(&thd);

  mysql_thread_set_psi_id(thd->thread_id());

#ifdef HAVE_PSI_THREAD_INTERFACE
  // The extension background thread was created via std::thread, which does
  // not go through mysql_thread_create and therefore has no PSI pre-
  // registration. Call new_thread() to create a PSI instrument for the current
  // OS thread so it appears in performance_schema.processlist.
  PSI_thread *psi = PSI_THREAD_CALL(new_thread)(key_thread_vef_extension_worker,
                                                0, nullptr, thd->thread_id());
  PSI_THREAD_CALL(set_thread_os_id)(psi);
  PSI_THREAD_CALL(set_thread_THD)(psi, thd);
  PSI_THREAD_CALL(set_thread)(psi);
  thd_set_psi(thd, psi);
  PSI_THREAD_CALL(set_thread_account)
  (thd->security_context()->user().str, thd->security_context()->user().length,
   thd->security_context()->host_or_ip().str,
   thd->security_context()->host_or_ip().length);
  PSI_THREAD_CALL(set_thread_command)(thd->get_command());
  PSI_THREAD_CALL(set_thread_start_time)(thd->query_start_in_secs());
  PSI_THREAD_CALL(set_thread_info)(thread_name, strlen(thread_name));
#endif

  thd->store_globals();

  Global_THD_manager *thd_manager = Global_THD_manager::get_instance();
  thd_manager->add_thd(thd);
  thd_manager->inc_thread_running();

  auto *handle = new (std::nothrow) vef_thread_handle_t;
  if (handle == nullptr) {
    thd_manager->remove_thd(thd);
    thd_manager->dec_thread_running();
    thd->release_resources();
    delete thd;
    LogVSQL(ERROR_LEVEL,
            "register_vef_background_thread: failed to allocate handle");
    return {nullptr, "failed to allocate handle"};
  }
  handle->thd = thd;
  mysql_mutex_init(key_mutex_vef_sleep, &handle->sleep_mutex,
                   MY_MUTEX_INIT_FAST);
  mysql_cond_init(key_cond_vef_sleep, &handle->sleep_cond);
  if (pipe(handle->stop_pipe) != 0) {
    handle->stop_pipe[0] = -1;
    handle->stop_pipe[1] = -1;
    LogVSQL(WARNING_LEVEL,
            "register_vef_background_thread: failed to create stop pipe "
            "for '%s'; poll_fd sleep will fall back to timed sleep",
            thread_name);
  }

  LogVSQL(INFORMATION_LEVEL, "VEF background thread registered: '%s' (id=%lu)",
          thread_name, static_cast<unsigned long>(thd->thread_id()));
  return {handle, ""};
}

void unregister_vef_background_thread(
    const vef_unregister_background_thread_arg_t *arg) {
  vef_thread_handle_t *handle = arg->handle;
  if (handle == nullptr) return;

  THD *thd = handle->thd;
  mysql_mutex_destroy(&handle->sleep_mutex);
  mysql_cond_destroy(&handle->sleep_cond);
  if (handle->stop_pipe[0] != -1) close(handle->stop_pipe[0]);
  if (handle->stop_pipe[1] != -1) close(handle->stop_pipe[1]);
  delete handle;

  Global_THD_manager *thd_manager = Global_THD_manager::get_instance();
  thd->set_proc_info("Clearing");
  thd->get_protocol_classic()->end_net();
  thd->release_resources();
  thd_manager->remove_thd(thd);
  thd_manager->dec_thread_running();
  delete thd;

#ifdef HAVE_PSI_THREAD_INTERFACE
  PSI_THREAD_CALL(delete_current_thread)();
#endif

  my_thread_end();
}

void stop_vef_background_thread(const vef_stop_background_thread_arg_t *arg) {
  vef_thread_handle_t *handle = arg->handle;
  if (handle == nullptr) return;

  THD *thd = handle->thd;

  // Set KILL_CONNECTION so the thread stops, then wake it if it is sleeping.
  mysql_mutex_lock(&thd->LOCK_thd_data);
  thd->awake(THD::KILL_CONNECTION);
  mysql_mutex_unlock(&thd->LOCK_thd_data);

  // Also wake a thread blocked in poll() via sleep_vef_background_thread_fd.
  if (handle->stop_pipe[1] != -1) {
    char byte = 1;
    // Ignore errors: if the write fails the poll will time out and the kill
    // flag set above will still cause the thread to stop.
    (void)write(handle->stop_pipe[1], &byte, 1);
  }
}

vef_sleep_background_thread_result_t sleep_vef_background_thread(
    const vef_sleep_background_thread_arg_t *arg) {
  vef_thread_handle_t *handle = arg->handle;
  if (handle == nullptr) return {false, false};

  THD *thd = handle->thd;

  // Check for kill before sleeping so we don't sleep at all if already killed.
  if (thd->killed != THD::NOT_KILLED) return {false, false};

  struct timespec abstime;
  set_timespec_nsec(&abstime,
                    static_cast<ulonglong>(arg->milliseconds) * 1000000ULL);

  mysql_mutex_lock(&handle->sleep_mutex);

  // enter_cond registers sleep_cond/sleep_mutex as the THD's current_cond and
  // current_mutex, allowing THD::awake() to broadcast on our condition and wake
  // us early during UNINSTALL EXTENSION or server shutdown.
  PSI_stage_info stage_sleeping = {0, "VEF background thread sleep", 0,
                                   PSI_DOCUMENT_ME};
  thd->enter_cond(&handle->sleep_cond, &handle->sleep_mutex, &stage_sleeping,
                  nullptr, __func__, __FILE__, __LINE__);

  mysql_cond_timedwait(&handle->sleep_cond, &handle->sleep_mutex, &abstime);

  // exit_cond requires that current_mutex is already unlocked before the call.
  mysql_mutex_unlock(&handle->sleep_mutex);
  thd->exit_cond(nullptr, __func__, __FILE__, __LINE__);

  return {thd->killed == THD::NOT_KILLED, false};
}

vef_sleep_background_thread_result_t sleep_vef_background_thread_fd(
    const vef_sleep_background_thread_fd_arg_t *arg) {
  vef_thread_handle_t *handle = arg->handle;
  if (handle == nullptr) return {false, false};

  THD *thd = handle->thd;

  if (thd->killed != THD::NOT_KILLED) return {false, false};

  // If we have no stop pipe or no user fd, fall back to the cond-var sleep.
  if (handle->stop_pipe[0] == -1 || arg->poll_fd == -1) {
    vef_sleep_background_thread_arg_t sleep_arg = {handle, arg->milliseconds};
    return sleep_vef_background_thread(&sleep_arg);
  }

  // Poll on both the user fd and the stop pipe. Whichever fires first wins:
  // - poll_fd readable: a connection (or other event) arrived, run the tick
  // - stop_pipe readable: stop() was called, exit the loop
  // - timeout: periodic wakeup, run the tick
  struct pollfd fds[2];
  fds[0].fd = arg->poll_fd;
  fds[0].events = POLLIN;
  fds[1].fd = handle->stop_pipe[0];
  fds[1].events = POLLIN;

  int ret = poll(fds, 2, static_cast<int>(arg->milliseconds));
  bool woke_on_fd = (ret > 0) && (fds[0].revents & POLLIN);

  return {thd->killed == THD::NOT_KILLED, woke_on_fd};
}

}  // namespace services
}  // namespace villagesql
