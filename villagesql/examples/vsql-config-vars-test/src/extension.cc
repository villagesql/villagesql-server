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

// VillageSQL extension for testing config variable registration.
// Declares one INT variable and one STRING variable with known defaults.

#include <villagesql/extension.h>

static long long g_max_items = 100;
static char *g_label = nullptr;

VEF_GENERATE_ENTRY_POINTS(
    make_extension("vsql_config_vars_test", "0.0.1")
        .config_var(make_config_var_int("max_items",
                                        "Maximum number of items to process",
                                        &g_max_items, 100, 0, 1000000))
        .config_var(make_config_var_str("label",
                                        "A label string for this extension",
                                        &g_label, "default_label")))
