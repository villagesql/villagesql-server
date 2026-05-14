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

// vsql_preview_ping_test extension: exercises the (consolidated)
// preview capability system using the "vsql::ping" capability.
//
// Provides a single VDF vsql_preview_ping_test.ping() that calls the
// server-provided ping capability and returns its counter value.

#include <villagesql/preview/ping.h>
#include <villagesql/vsql.h>

using namespace vsql;

static vsql::preview_ping::PingCapability g_ping;

static void ping_impl(IntResult out) { out.set(g_ping.ping()); }

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .func(make_func<&ping_impl>("ping").returns(INT).build())
        .with(g_ping))
