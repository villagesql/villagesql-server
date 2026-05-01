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

// vsql_preview_ping_test extension: exercises the preview capability
// registration system using the "vsql::ping" capability.
//
// Provides a single VDF vsql_preview_ping_test.ping() that calls the
// server-provided ping capability and returns its counter value. This
// verifies end-to-end that the capability system is wired up correctly.
//
// VDFs provided:
//   ping() -> INT   Calls the server ping capability and returns the counter.

#include <villagesql/preview/ping.h>
#include <villagesql/vsql.h>

using namespace vsql;

static auto g_ping = vsql::preview::ping::make_capability();

static void ping_impl(IntResult out) {
  if (!g_ping.available()) {
    out.set_null();
    return;
  }
  out.set(static_cast<long long>(g_ping.ping()));
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .func(make_func<&ping_impl>("ping").returns(INT).build())
        .preview_require_ping(g_ping))
