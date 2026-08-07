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

// VillageSQL test extension exercising the extension-side on_init / on_deinit
// builder hooks. on_init() runs once when the extension is loaded and accepted;
// it records the count in a process-local static. The init_count() VDF returns
// that count so a test can assert the hook ran exactly once per load.

#include <atomic>

#include <villagesql/vsql.h>

using namespace vsql;

static std::atomic<long long> g_init_count{0};
static std::atomic<long long> g_deinit_count{0};

// Runs extension-side, once, after the extension is validated and accepted.
static void on_init_hook() { g_init_count.fetch_add(1); }

// Runs extension-side at unload. Recorded for symmetry; the count is not
// observable across an unload but the hook must compile and link.
static void on_deinit_hook() { g_deinit_count.fetch_add(1); }

void init_count(IntResult out) { out.set(g_init_count.load()); }

VEF_GENERATE_ENTRY_POINTS(
    make_extension().on_init<&on_init_hook>().on_deinit<&on_deinit_hook>().func(
        make_func<&init_count>("init_count").returns(INT).no_params().build()))
