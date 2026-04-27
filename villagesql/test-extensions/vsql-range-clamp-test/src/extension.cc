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

// VillageSQL test extension demonstrating on_change callbacks.
//
// Exposes two system variables:
//   vsql_range_clamp_test.min_setting  INT  (default 0,  range 0..1000)
//   vsql_range_clamp_test.max_setting  INT  (default 100, range 0..1000)
//
// The on_change callback enforces min_setting <= max_setting:
//   - Setting min_setting above the current max_setting raises max_setting
//     to match.
//   - Setting max_setting below the current min_setting lowers min_setting
//     to match.
//
// The on_change callback reads the committed value from the change struct
// (rather than the global storage pointer) to avoid a race where a concurrent
// SET overwrites the global before the callback runs. It then clamps the other
// variable by writing directly to its storage pointer — sys_var::set() cannot
// be used here because on_change is called while MySQL holds the sys_var lock,
// and calling set() would attempt to re-acquire the same lock and deadlock.
//
// Direct writes to the other variable's long long global are safe: MySQL reads
// them under the same lock that protects the write path, so the adjusted value
// is visible to other sessions on their next read.

#include <villagesql/vsql.h>

using namespace vsql;

static long long g_min_setting = 0;
static long long g_max_setting = 100;

static void on_var_change(const vef_sys_var_change_t *change) {
  if (strcmp(change->var_name, "min_setting") == 0) {
    if (change->int_val > g_max_setting) g_max_setting = change->int_val;
  } else if (strcmp(change->var_name, "max_setting") == 0) {
    if (change->int_val < g_min_setting) g_min_setting = change->int_val;
  }
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .sys_var(make_sys_var_int("min_setting", "Lower bound of the range",
                                  &g_min_setting, 0, 0, 1000)
                     .on_change(&on_var_change))
        .sys_var(make_sys_var_int("max_setting", "Upper bound of the range",
                                  &g_max_setting, 100, 0, 1000)
                     .on_change(&on_var_change)))
