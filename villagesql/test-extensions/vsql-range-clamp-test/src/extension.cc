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
// Exposes three system variables:
//   vsql_range_clamp_test.min_setting  INT  (default 0,  range 0..1000)
//   vsql_range_clamp_test.max_setting  INT  (default 100, range 0..1000)
//   vsql_range_clamp_test.label        STR  (default "")
//
// min_setting and max_setting share an on_change handler — the callback
// receives a SysVarChange and uses var_name() to identify which variable
// changed, then as_int() to read the typed value.
//
// label uses a separate on_change handler that calls as_str() to read the
// value. Both demonstrate the SysVarChange typed API.
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

#include <villagesql/preview/sys_var.h>
#include <villagesql/vsql.h>

using namespace vsql;
namespace sv = vsql::preview_sys_var;

static long long g_min_setting = 0;
static long long g_max_setting = 100;
static char *g_label = nullptr;
static char g_last_label[256] = "";

// Shared handler for min_setting and max_setting — uses var_name() to
// identify which variable changed, and as_int() to read the typed value.
static void on_range_change(sv::SysVarChange change) {
  if (change.var_name() == "min_setting") {
    if (change.as_int().value() > g_max_setting)
      g_max_setting = change.as_int().value();
  } else if (change.var_name() == "max_setting") {
    if (change.as_int().value() < g_min_setting)
      g_min_setting = change.as_int().value();
  }
}

// Separate handler for label — uses as_str() to read the typed value.
static void on_label_change(sv::SysVarChange change) {
  auto s = change.as_str().value();
  size_t len =
      s.size() < sizeof(g_last_label) - 1 ? s.size() : sizeof(g_last_label) - 1;
  memcpy(g_last_label, s.data(), len);
  g_last_label[len] = '\0';
}

static void last_label_vdf(StringResult out) {
  out.set(std::string_view(g_last_label));
}

static auto SYS_VARS = sv::make_capability({
    sv::make_int("min_setting", "Lower bound of the range", &g_min_setting, 0,
                 0, 1000)
        .on_change<&on_range_change>(),
    sv::make_int("max_setting", "Upper bound of the range", &g_max_setting, 100,
                 0, 1000)
        .on_change<&on_range_change>(),
    sv::make_str("label", "Arbitrary label string", &g_label, "")
        .on_change<&on_label_change>(),
});

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .func(make_func<&last_label_vdf>("last_label").returns(STRING).build())
        .with(SYS_VARS))
