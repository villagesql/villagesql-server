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

#ifndef VILLAGESQL_DETAIL_THREAD_BUILDER_H
#define VILLAGESQL_DETAIL_THREAD_BUILDER_H

// ManagedThread, ThreadDescriptor, make_thread — shared by both the stable
// extension_builder::ExtensionBuilder and the vsql::ExtensionBuilder.
// Not part of the public API; use make_thread() + .thread() on the builder.

#include <atomic>
#include <chrono>
#include <cstdio>
#include <string_view>
#include <thread>

#include <villagesql/abi/types.h>
#include <villagesql/vsql/thread.h>

namespace villagesql {
namespace extension_builder {

// Sentinel stored in g_handle before the background thread has called
// thread::init(). Allows on_unload to distinguish "not yet initialised"
// from "already finished and cleared".
inline vef_thread_handle_t *const kHandlePending =
    reinterpret_cast<vef_thread_handle_t *>(static_cast<uintptr_t>(1));

// When no .periodic(ms) is set, use a large timeout so the thread still
// wakes on stop signals but does not spin. 24 hours expressed in milliseconds.
static constexpr unsigned int kNoPeriodicSleepMs = 24u * 60u * 60u * 1000u;

template <void (*WorkFn)()>
struct ManagedThread {
  static inline std::thread g_thread;
  // Starts as kHandlePending. thread_main replaces it with the real handle
  // (or nullptr on init failure). on_unload replaces it with nullptr to take
  // ownership. nullptr means either init failed or thread already finished.
  static inline std::atomic<vef_thread_handle_t *> g_handle{kHandlePending};
  // Full thread name ("extname/suffix"), set by setup() at registration.
  static inline char g_full_name[256]{};
  // Trigger settings. sleep_ms == kNoPeriodicSleepMs means no .periodic() set.
  static inline unsigned int g_sleep_ms{kNoPeriodicSleepMs};
  // poll_fd_ptr != nullptr means .poll_fd() was set.
  static inline const int *g_poll_fd_ptr{nullptr};

  static void setup(std::string_view ext_name, const char *suffix,
                    unsigned int sleep_ms, const int *poll_fd_ptr) {
    snprintf(g_full_name, sizeof(g_full_name), "%.*s/%s",
             static_cast<int>(ext_name.size()), ext_name.data(), suffix);
    g_sleep_ms = sleep_ms;
    g_poll_fd_ptr = poll_fd_ptr;
  }

  static void thread_main() {
    villagesql::thread::ThreadRegistration reg(g_full_name);
    // Publish handle (may be nullptr if registration failed). Either way, not
    // pending.
    g_handle.store(reg.raw_handle(), std::memory_order_release);

    if (reg) {
      int poll_fd = (g_poll_fd_ptr != nullptr) ? *g_poll_fd_ptr : -1;
      bool has_periodic = (g_sleep_ms != kNoPeriodicSleepMs);
      auto duration = std::chrono::milliseconds(g_sleep_ms);
      // Returns true if the thread should keep running. Sets woke_on_fd if
      // the wakeup was caused by poll_fd becoming readable.
      auto wait = [&](bool &woke_on_fd) -> bool {
        if (poll_fd >= 0 && villagesql::thread::g_sleep_background_thread_fd) {
          auto result = reg.sleep_fd(duration, poll_fd);
          woke_on_fd = result.woke_on_fd;
          return result.keep_running;
        }
        woke_on_fd = false;
        return reg.sleep(duration);
      };
      bool woke_on_fd = false;
      while (wait(woke_on_fd)) {
        // Call WorkFn if woken by the fd trigger, or by the periodic timer
        // (i.e. not a pure-poll-only setup that timed out with no fd event).
        if (woke_on_fd || has_periodic) WorkFn();
      }
      // Clear before reg destructor runs: on_unload's exchange will get nullptr
      // and skip stop(), which is correct since the thread is already exiting.
      g_handle.store(nullptr, std::memory_order_release);
    }
  }

  static bool on_load(char * /*error_msg*/) {
    g_handle.store(kHandlePending, std::memory_order_relaxed);
    g_thread = std::thread(thread_main);
    return false;
  }

  static void on_unload() {
    if (!g_thread.joinable()) return;
    // Wait until thread_main has replaced kHandlePending with the real handle
    // (or nullptr on init failure). This covers the rare window between
    // on_load starting the thread and thread_main calling thread::init().
    vef_thread_handle_t *handle;
    while ((handle = g_handle.load(std::memory_order_acquire)) ==
           kHandlePending) {
      std::this_thread::yield();
    }
    // Atomically take ownership. If the thread already cleared it (loop exited
    // naturally before on_unload ran), stop() receives nullptr and is a no-op.
    handle = g_handle.exchange(nullptr, std::memory_order_acq_rel);
    villagesql::thread::detail::stop(handle);
    g_thread.join();
  }
};

// Descriptor produced by make_thread<&fn>("suffix").
// Passed to .thread() on the extension builder after configuring triggers.
template <void (*WorkFn)()>
struct ThreadDescriptor {
  const char *suffix;
  // Trigger settings. Defaults mean: no periodic timer, no fd poll.
  unsigned int sleep_ms{kNoPeriodicSleepMs};
  const int *poll_fd_ptr{nullptr};

  // Call WorkFn on a periodic timer. ms is the interval in milliseconds.
  // Can be combined with .poll_fd(): WorkFn is called on whichever fires first.
  constexpr ThreadDescriptor periodic(unsigned int ms) const {
    return {suffix, ms, poll_fd_ptr};
  }

  // Call WorkFn immediately when fd becomes readable (e.g. a listening socket).
  // Can be combined with .periodic(ms): WorkFn is called on whichever fires
  // first.
  constexpr ThreadDescriptor poll_fd(const int *ptr) const {
    return {suffix, sleep_ms, ptr};
  }
};

// make_thread<&work_fn>("suffix")
//
// Returns a ThreadDescriptor. Chain .periodic(ms) and/or .poll_fd(&fd) to
// control when WorkFn is called. The thread is started in on_load() and
// stopped in on_unload() automatically.
//
// Safety: do not start background threads from inside a VDF (a function called
// during SQL query execution). VDFs run on a connection thread that may hold
// locks; spawning threads there can cause deadlocks. Use make_thread() and the
// .thread() builder method instead, which starts threads from on_load() where
// it is safe to do so.
//
// TODO(villagesql-beta): Extensions that use .thread() need a way to declare
// that dependency so the server can reject loading them with a clear error on
// older servers that do not support Protocol 2 background threads.
template <void (*WorkFn)()>
constexpr ThreadDescriptor<WorkFn> make_thread(const char *suffix) {
  return {suffix};
}

}  // namespace extension_builder
}  // namespace villagesql

#endif  // VILLAGESQL_DETAIL_THREAD_BUILDER_H
