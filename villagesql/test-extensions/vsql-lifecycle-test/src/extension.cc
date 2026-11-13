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

// VillageSQL test extension demonstrating managed background thread_worker.
//
// The thread is controlled via the auto-registered monitor_enabled sys var.
// Set vsql_lifecycle_test.monitor_enabled = ON to start the thread.
// The thread appears in performance_schema.processlist as 'vef_worker'
// with info 'vsql_lifecycle_test/monitor' while running.

#include <atomic>

#include <villagesql/preview/thread_worker.h>
#include <villagesql/vsql.h>

using namespace vsql;

static std::atomic<long long> g_tick_count{0};

static vef_next_wakeup_t monitor_work(vef_wakeup_reason_t reason,
                                      struct vef_thread_handle_t *, void *) {
  if (reason == VEF_WAKEUP_ENABLE) return {100, 0};
  if (reason == VEF_WAKEUP_PERIODIC) g_tick_count.fetch_add(1);
  return {};
}

static vsql::preview_thread_worker::ThreadWorkerCapability<&monitor_work>
    g_monitor_worker{"monitor"};

void tick_count(IntResult out) { out.set(g_tick_count.load()); }

VEF_GENERATE_ENTRY_POINTS(make_extension()
                              .func(make_func<&tick_count>("tick_count")
                                        .returns(INT)
                                        .no_params()
                                        .build())
                              .with(g_monitor_worker))
