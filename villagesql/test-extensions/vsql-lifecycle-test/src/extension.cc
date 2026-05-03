// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is designed to work with certain software (including
// but not limited to OpenSSL) that is licensed under separate terms,
// as designated in a particular file or component or in included license
// documentation.  The authors of MySQL hereby grant you an additional
// permission to link the program and your derivative works with the
// separately licensed software that they have either included with
// the program or referenced in the documentation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

// VillageSQL test extension demonstrating managed background thread
// registration combined with on_load/on_unload lifecycle callbacks.
//
// on_load initialises shared state before the background thread starts.
// on_unload cleans it up after the thread has stopped.
// This mirrors the pattern for extensions that open a resource (e.g. a socket)
// in on_load, use it from the background thread, and close it in on_unload.
//
// The thread appears in performance_schema.processlist as user 'vef_worker'
// with info 'vsql_lifecycle_test/monitor' while the extension is installed.

#include <atomic>

#include <villagesql/vsql.h>

using namespace vsql;

static std::atomic<long long> g_tick_count{0};
static std::atomic<long long> g_on_load_count{0};

static void on_load(villagesql::LoadResult & /*result*/) {
  g_tick_count.store(0);
  g_on_load_count.fetch_add(1);
}

static void on_unload() { g_tick_count.store(0); }

static void monitor_work() { g_tick_count.fetch_add(1); }

void tick_count(IntResult out) { out.set(g_tick_count.load()); }
void on_load_count(IntResult out) { out.set(g_on_load_count.load()); }

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .on_load<&on_load>()
        .on_unload<&on_unload>()
        .func(make_func<&tick_count>("tick_count").returns(INT).build())
        .func(make_func<&on_load_count>("on_load_count").returns(INT).build())
        .thread(make_thread<&monitor_work>("monitor").periodic(100)))
