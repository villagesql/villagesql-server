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

// vsql_preview_ping_no_cap_test extension: declares a PingCapability without
// ever passing it to .with(). The SDK's per-.so capability registry detects
// this at extension load time and fails vef_register, so INSTALL EXTENSION
// must report a clear "capability declared but never passed to .with()"
// error rather than silently producing an extension with a null vtable.

#include <villagesql/preview/ping.h>
#include <villagesql/vsql.h>

using namespace vsql;

// Declared but never registered via .with(g_ping).
static vsql::preview_ping::PingCapability g_ping;

VEF_GENERATE_ENTRY_POINTS(make_extension())
