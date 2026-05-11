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

#ifndef VILLAGESQL_PREVIEW_THREAD_WORKER_H
#define VILLAGESQL_PREVIEW_THREAD_WORKER_H

#include <villagesql/abi/preview/thread_worker.h>

namespace vsql::preview_thread_worker {

// ThreadWorkerCapability<WorkFn> is a concrete capability wrapper for the
// "vsql::thread_worker" preview capability. The work function is a template
// parameter so each distinct work function gets its own static descriptor.
//
// Usage:
//   static vef_next_wakeup_t my_work(vef_wakeup_reason_t reason,
//                                    struct vef_thread_handle_t *thread,
//                                    void *arg) { ... }
//
//   static vsql::preview_thread_worker::ThreadWorkerCapability<&my_work>
//       g_cap{"suffix"};
//
//   VEF_GENERATE_ENTRY_POINTS(make_extension().with(g_cap))

template <auto WorkFn>
class ThreadWorkerCapability {
 public:
  // Constructor populates the per-WorkFn static descriptor. Runs at
  // static-init time (before vef_register).
  ThreadWorkerCapability(const char *suffix,
                         const char *var_name = nullptr) noexcept;

  // VEF writes this during registration. Public for the registration
  // glue; do not access from extension code.
  const vef_preview_thread_worker_t *abi = nullptr;

  // One static descriptor per WorkFn instantiation. The constructor
  // populates it. The trait's extension_data() returns its address so
  // the wire format carries a pointer to it.
  static inline vef_thread_worker_descriptor_t descriptor{};
};

}  // namespace vsql::preview_thread_worker

#include <villagesql/preview/thread_worker_impl.h>
#include <villagesql/preview/thread_worker_register.h>

#endif  // VILLAGESQL_PREVIEW_THREAD_WORKER_H
