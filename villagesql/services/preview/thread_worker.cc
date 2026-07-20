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

#include "villagesql/services/preview/thread_worker.h"

#include <poll.h>
#include <unistd.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

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
#include "villagesql/services/sys_var_access.h"

#ifdef HAVE_PSI_INTERFACE
#include "mysql/psi/psi_thread.h"
#endif

namespace villagesql::services {

PSI_thread_key key_thread_vef_extension_worker;
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

void init_thread_worker_psi_keys() {
#ifdef HAVE_PSI_INTERFACE
  const char *category = "villagesql";
  mysql_thread_register(
      category, vef_extension_worker_thread_info,
      static_cast<int>(array_elements(vef_extension_worker_thread_info)));
  mysql_mutex_register(category, vef_sleep_mutex_info, 1);
  mysql_cond_register(category, vef_sleep_cond_info, 1);
#endif
}

// Sentinel: handle not yet set by background thread.
static vef_thread_handle_t *const kHandlePending =
    reinterpret_cast<vef_thread_handle_t *>(static_cast<uintptr_t>(1));

// Per-worker server-side state.
struct WorkerState {
  const vef_thread_worker_descriptor_t *descriptor;
  std::string extension_name;
  std::thread thread;
  std::atomic<vef_thread_handle_t *> handle{kHandlePending};

  // Serializes signal_stop() (run by the stopping thread) against the
  // background thread's handle teardown, so the handle and its THD cannot be
  // freed out from under an in-progress signal_stop(). Lives in WorkerState,
  // which outlives the handle, so it stays valid across the teardown.
  std::mutex stop_mu;

  // Current dynamic wakeup config, updated from work_fn return values.
  int current_poll_fd{-1};
  unsigned int current_sleep_ms{0};

  // Storage for the auto-registered "{suffix}_enabled" sys var.
  bool enabled{false};
  char enabled_var_name[64]{};
};

// Global map from descriptor pointer to WorkerState. unique_ptr keeps
// WorkerState addresses stable across map rehashes. Protected by g_mu.
static std::mutex g_mu;
static std::unordered_map<const vef_thread_worker_descriptor_t *,
                          std::unique_ptr<WorkerState>>
    g_workers;

static vef_thread_handle_t *create_thread_handle(const char *thread_name) {
  if (my_thread_init()) {
    LogVSQL(ERROR_LEVEL, "thread_worker: my_thread_init() failed for '%s'",
            thread_name);
    return nullptr;
  }

  THD *thd = new (std::nothrow) THD;
  if (thd == nullptr) {
    LogVSQL(ERROR_LEVEL, "thread_worker: failed to allocate THD for '%s'",
            thread_name);
    my_thread_end();
    return nullptr;
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
  thd->thread_stack = reinterpret_cast<char *>(&thd);

  mysql_thread_set_psi_id(thd->thread_id());

#ifdef HAVE_PSI_THREAD_INTERFACE
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
    my_thread_end();
    LogVSQL(ERROR_LEVEL, "thread_worker: failed to allocate handle for '%s'",
            thread_name);
    return nullptr;
  }

  handle->thd = thd;
  mysql_mutex_init(key_mutex_vef_sleep, &handle->sleep_mutex,
                   MY_MUTEX_INIT_FAST);
  mysql_cond_init(key_cond_vef_sleep, &handle->sleep_cond);
  if (pipe(handle->stop_pipe) != 0) {
    handle->stop_pipe[0] = -1;
    handle->stop_pipe[1] = -1;
    LogVSQL(WARNING_LEVEL,
            "thread_worker: failed to create stop pipe for '%s'; "
            "poll_fd sleep will fall back to timed sleep",
            thread_name);
  }

  LogVSQL(INFORMATION_LEVEL, "VEF background thread registered: '%s' (id=%lu)",
          thread_name, static_cast<unsigned long>(thd->thread_id()));
  return handle;
}

static void destroy_thread_handle(vef_thread_handle_t *handle) {
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

#ifdef HAVE_PSI_THREAD_INTERFACE
  // Decouple the THD from the thread instrumentation before deleting it.
  // create_thread_handle() associated this THD with the PFS thread via
  // set_thread_THD(); if we don't clear that association, the
  // delete_current_thread() call below aggregates this thread's status by
  // dereferencing the (already freed) THD -- a use-after-free that UBSan's
  // vptr check reports as a dynamic-type cache miss in get_thd_status_var().
  // This mirrors the teardown order in connection_handler_per_thread.cc.
  thd_set_psi(thd, nullptr);
  mysql_thread_set_psi_THD(nullptr);
#endif

  delete thd;

#ifdef HAVE_PSI_THREAD_INTERFACE
  PSI_THREAD_CALL(delete_current_thread)();
#endif

  my_thread_end();
}

static bool sleep_handle(vef_thread_handle_t *handle, unsigned int ms,
                         int poll_fd, vef_wakeup_reason_t *out_reason) {
  THD *thd = handle->thd;
  if (thd->killed != THD::NOT_KILLED) return false;

  *out_reason = VEF_WAKEUP_PERIODIC;

  if (poll_fd >= 0 && handle->stop_pipe[0] != -1) {
    struct pollfd fds[2];
    fds[0].fd = poll_fd;
    fds[0].events = POLLIN;
    fds[1].fd = handle->stop_pipe[0];
    fds[1].events = POLLIN;
    int rc = poll(fds, 2, static_cast<int>(ms));
    if (rc > 0 && (fds[0].revents & POLLIN)) *out_reason = VEF_WAKEUP_POLL_FD;
  } else {
    struct timespec abstime;
    set_timespec_nsec(&abstime, static_cast<ulonglong>(ms) * 1000000ULL);

    mysql_mutex_lock(&handle->sleep_mutex);
    PSI_stage_info stage_sleeping = {0, "VEF background thread sleep", 0,
                                     PSI_DOCUMENT_ME};
    thd->enter_cond(&handle->sleep_cond, &handle->sleep_mutex, &stage_sleeping,
                    nullptr, __func__, __FILE__, __LINE__);
    mysql_cond_timedwait(&handle->sleep_cond, &handle->sleep_mutex, &abstime);
    mysql_mutex_unlock(&handle->sleep_mutex);
    thd->exit_cond(nullptr, __func__, __FILE__, __LINE__);
  }

  return thd->killed == THD::NOT_KILLED;
}

static void signal_stop(vef_thread_handle_t *handle) {
  THD *thd = handle->thd;
  mysql_mutex_lock(&thd->LOCK_thd_data);
  thd->awake(THD::KILL_CONNECTION);
  mysql_mutex_unlock(&thd->LOCK_thd_data);

  if (handle->stop_pipe[1] != -1) {
    char byte = 1;
    // Best-effort wakeup: if the pipe already holds a pending byte this write
    // may fail, which is harmless. Assign the result (rather than a (void)
    // cast, which GCC ignores for warn_unused_result) and mark it unused.
    [[maybe_unused]] ssize_t written = write(handle->stop_pipe[1], &byte, 1);
  }
}

static void apply_next_wakeup(WorkerState *state,
                              const vef_next_wakeup_t &next) {
  if (next.sleep_ms != 0) state->current_sleep_ms = next.sleep_ms;
  if (next.poll_fd == -1)
    state->current_poll_fd = -1;  // -1 means "clear poll fd"
  else if (next.poll_fd != 0)
    state->current_poll_fd = next.poll_fd;
}

static void thread_main(WorkerState *state) {
  const vef_thread_worker_descriptor_t *desc = state->descriptor;

  char thread_name_buf[128];
  std::snprintf(thread_name_buf, sizeof(thread_name_buf), "%s/%s",
                state->extension_name.c_str(),
                desc->suffix != nullptr ? desc->suffix : "worker");

  vef_thread_handle_t *handle = create_thread_handle(thread_name_buf);
  state->handle.store(handle, std::memory_order_release);

  if (handle != nullptr) {
    // ENABLE was already called synchronously in worker_start; just loop.
    while (true) {
      vef_wakeup_reason_t reason;
      if (!sleep_handle(handle, state->current_sleep_ms, state->current_poll_fd,
                        &reason))
        break;
      apply_next_wakeup(state, desc->work_fn(reason, handle, desc->arg));
    }

    desc->work_fn(VEF_WAKEUP_DISABLE, handle, desc->arg);

    // Publish nullptr and free the handle under stop_mu so that a concurrent
    // worker_stop()/signal_stop() cannot dereference the handle, nor its THD,
    // after it has been freed.
    {
      std::lock_guard<std::mutex> lock(state->stop_mu);
      state->handle.store(nullptr, std::memory_order_release);
      destroy_thread_handle(handle);
    }
  }
}

static void worker_start(WorkerState *state) {
  if (state->thread.joinable()) return;

  const vef_thread_worker_descriptor_t *desc = state->descriptor;
  state->current_sleep_ms =
      (desc->sleep_ms != 0) ? desc->sleep_ms : 24u * 60u * 60u * 1000u;
  state->current_poll_fd = -1;

  // Call ENABLE synchronously so the extension can open its socket (or do
  // any other setup) before SET GLOBAL returns to the client.
  apply_next_wakeup(state,
                    desc->work_fn(VEF_WAKEUP_ENABLE, nullptr, desc->arg));

  state->handle.store(kHandlePending, std::memory_order_relaxed);
  state->thread = std::thread(thread_main, state);
}

static void worker_stop(WorkerState *state) {
  if (!state->thread.joinable()) return;

  vef_thread_handle_t *handle;
  while ((handle = state->handle.load(std::memory_order_acquire)) ==
         kHandlePending) {
    std::this_thread::yield();
  }
  // Signal the worker while holding stop_mu so its teardown cannot race with
  // signal_stop(). The handle is re-read under the lock because the worker may
  // already have torn down and published nullptr.
  {
    std::lock_guard<std::mutex> lock(state->stop_mu);
    handle = state->handle.load(std::memory_order_acquire);
    if (handle != nullptr) signal_stop(handle);
  }
  state->thread.join();
}

static void on_enabled_change(const char *var_name, bool new_val) {
  if (var_name == nullptr) return;

  WorkerState *state = nullptr;
  {
    std::lock_guard<std::mutex> lock(g_mu);
    for (auto &kv : g_workers) {
      if (strcmp(kv.second->enabled_var_name, var_name) == 0) {
        state = kv.second.get();
        break;
      }
    }
  }
  if (state == nullptr) return;

  if (new_val) {
    worker_start(state);
  } else {
    worker_stop(state);
  }
}

bool on_populate_thread_worker(const PopulateContext &ctx,
                               std::string &error_message) {
  if (ctx.capability_config == nullptr || ctx.extension_name.empty())
    return false;

  auto *desc = static_cast<const vef_thread_worker_descriptor_t *>(
      ctx.capability_config);

  auto state = std::make_unique<WorkerState>();
  state->descriptor = desc;
  state->extension_name = ctx.extension_name;

  const char *suffix = (desc->suffix != nullptr) ? desc->suffix : "worker";
  if (desc->var_name != nullptr) {
    std::snprintf(state->enabled_var_name, sizeof(state->enabled_var_name),
                  "%s", desc->var_name);
  } else {
    std::snprintf(state->enabled_var_name, sizeof(state->enabled_var_name),
                  "%s_enabled", suffix);
  }

  SysVarDesc enabled_desc{state->enabled_var_name,
                          "Enable the background worker thread",
                          &state->enabled, false, on_enabled_change};

  if (register_one_sys_var(ctx.extension_name, enabled_desc)) {
    error_message = std::string("thread_worker: failed to register '") +
                    state->enabled_var_name + "' for '" +
                    std::string(ctx.extension_name) + "'";
    return true;
  }

  std::lock_guard<std::mutex> lock(g_mu);
  g_workers[desc] = std::move(state);
  return false;
}

void on_depopulate_thread_worker(const DepopulateContext &ctx) {
  if (ctx.capability_config == nullptr) return;

  auto *desc = static_cast<const vef_thread_worker_descriptor_t *>(
      ctx.capability_config);

  WorkerState *state = nullptr;
  {
    std::lock_guard<std::mutex> lock(g_mu);
    auto it = g_workers.find(desc);
    if (it == g_workers.end()) return;
    state = it->second.get();
  }

  worker_stop(state);

  unregister_one_sys_var(state->extension_name, state->enabled_var_name);

  std::lock_guard<std::mutex> lock(g_mu);
  g_workers.erase(desc);
}

namespace {
vef_preview_thread_worker_t g_thread_worker_vtable{
    VEF_PREVIEW_THREAD_WORKER_ABI_VERSION};
}  // namespace

vef_preview_thread_worker_t *preview_thread_worker_vtable() {
  return &g_thread_worker_vtable;
}

}  // namespace villagesql::services
