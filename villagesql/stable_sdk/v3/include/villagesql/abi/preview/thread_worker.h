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

#ifndef VILLAGESQL_ABI_PREVIEW_THREAD_WORKER_H
#define VILLAGESQL_ABI_PREVIEW_THREAD_WORKER_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Reason the server is calling work_fn.
typedef enum {
  // Worker was just enabled (sys var flipped ON). Open resources here.
  // Return value sets the initial poll_fd and sleep_ms for subsequent wakeups.
  // Note: thread handle is NULL for this call — do not use it.
  VEF_WAKEUP_ENABLE = 0,
  // Periodic timer fired (sleep_ms elapsed).
  VEF_WAKEUP_PERIODIC = 1,
  // A watched file descriptor became readable.
  VEF_WAKEUP_POLL_FD = 2,
  // Worker was disabled (sys var flipped OFF) or server is shutting down.
  // Close resources here. Return value is ignored.
  VEF_WAKEUP_DISABLE = 3,
} vef_wakeup_reason_t;

// Forward declaration — full definition is server-internal.
// Passed to work_fn so the extension can call sql_query functions.
struct vef_thread_handle_t;

// Returned by work_fn to update the next wakeup configuration.
// Zero values mean "keep current setting unchanged":
//   sleep_ms == 0  -> keep current sleep interval
//   poll_fd  == 0  -> keep current poll fd (or no poll fd if none set)
// To set a new poll fd, return its value (must be > 0).
// To explicitly clear the poll fd, return poll_fd == -1.
typedef struct {
  unsigned int sleep_ms;
  int poll_fd;
} vef_next_wakeup_t;

// Work function called by the server on each wakeup.
// Returns the next wakeup configuration; zero fields mean "no change".
typedef vef_next_wakeup_t (*vef_work_fn_t)(vef_wakeup_reason_t reason,
                                           struct vef_thread_handle_t *thread,
                                           void *arg);

// Descriptor filled in by the extension and passed to the server as
// capability_config in vef_required_capability_t. The server stores it at
// extension load time and manages the thread entirely — the extension only
// provides the work function and configuration.
//
// All pointers must remain valid for the lifetime of the extension.
typedef struct {
  // Called by the server on each wakeup. Required.
  vef_work_fn_t work_fn;

  // Passed through to work_fn on every call. May be NULL.
  void *arg;

  // Default interval between wakeups in milliseconds. Used as the initial
  // sleep_ms until work_fn returns a non-zero value. 0 means no periodic
  // wakeup (thread only wakes on poll_fd or explicit stop).
  unsigned int sleep_ms;

  // Thread name suffix (e.g. "monitor"). The server prepends the extension
  // name to form the full thread name (e.g. "my_ext/monitor").
  // Must remain valid for the lifetime of the thread.
  const char *suffix;

  // Optional. Name of the control system variable. If non-null, the server
  // registers this name instead of the default "{suffix}_enabled".
  // Must remain valid for the lifetime of the extension.
  const char *var_name;
} vef_thread_worker_descriptor_t;

// Preview capability: "vsql::preview::thread_worker"
//
// The extension sets capability_config = &descriptor in
// vef_required_capability_t. The server registers a control sys var on the
// extension's behalf at load time. The var name is descriptor->var_name if set,
// otherwise
// "{suffix}_enabled". Setting it ON starts the background thread; OFF stops it.
//
// Capability name: VEF_PREVIEW_THREAD_WORKER_NAME

#define VEF_PREVIEW_THREAD_WORKER_NAME "vsql::preview::thread_worker"

// Current ABI version for the "vsql::preview::thread_worker" capability.
#define VEF_PREVIEW_THREAD_WORKER_ABI_VERSION 1

// Server-side vtable. The version field is always first, matching the
// convention used by other preview capabilities.
typedef struct {
  uint32_t version;
} vef_preview_thread_worker_t;

#ifdef __cplusplus
}
#endif

#endif  // VILLAGESQL_ABI_PREVIEW_THREAD_WORKER_H
