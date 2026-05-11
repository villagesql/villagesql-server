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

// vsql_preview_ping_no_cap_test extension: verifies that omitting .with()
// for a capability causes it to be unavailable (abi stays null), and that
// the extension degrades gracefully rather than crashing.
//
// VDFs provided:
//   ping_available() -> INT   Returns 1 if ping cap was populated, else 0.
//   ping_value()     -> INT   Returns ping counter, or NULL if unavailable.

#include <villagesql/preview/ping.h>
#include <villagesql/vsql.h>

using namespace vsql;

// g_ping is never registered via .with(g_ping), so
// abi stays null.
static vsql::preview_ping::PingCapability g_ping;

static void ping_available_impl(IntResult out) {
  out.set(g_ping.abi != nullptr ? 1 : 0);
}

static void ping_value_impl(IntResult out) {
  if (g_ping.abi == nullptr) {
    out.set_null();
    return;
  }
  out.set(g_ping.ping());
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .func(make_func<&ping_available_impl>("ping_available")
                  .returns(INT)
                  .build())
        .func(make_func<&ping_value_impl>("ping_value").returns(INT).build()))
