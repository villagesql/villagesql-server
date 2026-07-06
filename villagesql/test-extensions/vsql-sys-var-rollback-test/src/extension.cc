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

// VillageSQL extension for testing sys var rollback on partial registration
// failure. Declares a valid INT variable followed by a DOUBLE variable.
// DOUBLE registration fails because MySQL's component_sys_variable_register
// does not support PLUGIN_VAR_DOUBLE. The server must roll back the INT
// variable so that INSTALL EXTENSION fails cleanly with no variables left.

#include <villagesql/preview/sys_var.h>
#include <villagesql/vsql.h>

using namespace vsql;
namespace sv = vsql::preview_sys_var;

static int64_t g_good_int;
static double g_bad_double;

static auto SYS_VARS = sv::make_capability({
    sv::make_int("good_int", "An INT variable that registers successfully",
                 &g_good_int, 0, 0, 1000),
    sv::make_double("bad_double",
                    "A DOUBLE variable that fails to register (unsupported)",
                    &g_bad_double, 1.0, 0.0, 10.0),
});

VEF_GENERATE_ENTRY_POINTS(make_extension().with(SYS_VARS))
