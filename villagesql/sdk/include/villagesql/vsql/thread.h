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

#ifndef VILLAGESQL_VSQL_THREAD_H
#define VILLAGESQL_VSQL_THREAD_H

// villagesql::thread — internal thread registration machinery
//
// Extension authors should not use this header directly.
// Use make_thread() + .thread() on the extension builder instead:
//
//   VEF_GENERATE_ENTRY_POINTS(
//       make_extension()
//           .thread(make_thread<&my_work_fn>("worker").periodic(100)))
//
// This header is included transitively via <villagesql/vsql.h> but its
// types are implementation details of ManagedThread in extension_builder.h.

#include <chrono>

#include <villagesql/abi/types.h>

namespace villagesql {
namespace thread {

// Extension-local storage for function pointers, set during vef_register()
// by vef_register_impl() in extension_builder.h.
inline vef_register_background_thread_func_t g_register_background_thread =
    nullptr;
inline vef_unregister_background_thread_func_t g_unregister_background_thread =
    nullptr;
inline vef_sleep_background_thread_func_t g_sleep_background_thread = nullptr;
inline vef_stop_background_thread_func_t g_stop_background_thread = nullptr;
inline vef_sleep_background_thread_fd_func_t g_sleep_background_thread_fd =
    nullptr;

// Internal RAII registration handle used by ManagedThread.
// Extension authors should not use this directly — use make_thread() in
// extension_builder.h instead, which manages the thread lifetime safely.
// Non-copyable; pass by reference if needed within the thread.
class ThreadRegistration {
 public:
  explicit ThreadRegistration(const char *thread_name) {
    if (g_register_background_thread == nullptr) return;
    vef_register_background_thread_arg_t arg = {thread_name};
    handle_ = g_register_background_thread(&arg).handle;
  }

  ~ThreadRegistration() {
    if (handle_ == nullptr) return;
    if (g_unregister_background_thread == nullptr) return;
    vef_unregister_background_thread_arg_t arg = {handle_};
    g_unregister_background_thread(&arg);
  }

  ThreadRegistration(const ThreadRegistration &) = delete;
  ThreadRegistration &operator=(const ThreadRegistration &) = delete;

  // Returns true if registration succeeded.
  explicit operator bool() const { return handle_ != nullptr; }

  // Interruptible sleep. Sleeps for the given duration, waking early if the
  // server requests termination (shutdown or UNINSTALL EXTENSION).
  // Returns true if the thread should keep running, false if it should stop.
  //
  // Typical usage:
  //   while (reg.sleep(std::chrono::milliseconds(100))) {
  //     // ... do periodic work ...
  //   }
  template <typename Rep, typename Period>
  bool sleep(std::chrono::duration<Rep, Period> duration) const {
    if (g_sleep_background_thread == nullptr) return false;
    auto ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    vef_sleep_background_thread_arg_t arg = {handle_,
                                             static_cast<unsigned int>(ms)};
    return g_sleep_background_thread(&arg).keep_running;
  }

  // Like sleep(), but also wakes when poll_fd becomes readable. Useful for
  // threads waiting for I/O (e.g. a listening socket). Wakes on whichever
  // fires first: poll_fd readable, stop signal, or timeout.
  // If poll_fd is -1, behaves identically to sleep().
  // Returns the full result so callers can distinguish why the sleep ended.
  template <typename Rep, typename Period>
  vef_sleep_background_thread_result_t sleep_fd(
      std::chrono::duration<Rep, Period> duration, int poll_fd) const {
    if (g_sleep_background_thread_fd == nullptr) return {false, false};
    auto ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    vef_sleep_background_thread_fd_arg_t arg = {
        handle_, static_cast<unsigned int>(ms), poll_fd};
    return g_sleep_background_thread_fd(&arg);
  }

  // Returns the raw handle, for use by ManagedThread which needs to store it
  // atomically and call stop() from another thread.
  vef_thread_handle_t *raw_handle() const { return handle_; }

 private:
  vef_thread_handle_t *handle_{nullptr};
};

namespace detail {

// Used by ManagedThread in extension_builder.h to signal a background thread
// to stop from on_unload(). Not part of the public extension API.
inline void stop(vef_thread_handle_t *handle) {
  if (g_stop_background_thread == nullptr) return;
  vef_stop_background_thread_arg_t arg = {handle};
  g_stop_background_thread(&arg);
}

}  // namespace detail

}  // namespace thread
}  // namespace villagesql

#endif  // VILLAGESQL_VSQL_THREAD_H
