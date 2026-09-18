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
// Declares one INT variable, one STRING variable, and one ENUM variable with
// known defaults.
//
// Demonstrates three patterns for accessing config vars from VDFs:
//   read_max_items()  - reads the global directly (fast path, no locking needed
//                       for INT variables)
//   write_max_items() - writes via SYS_VARS.set() so MySQL applies
//                       locking, validation, and PERSIST support
//   read_label()      - reads via SYS_VARS.get() to round-trip through MySQL
//
// The ENUM variable (mode) exercises make_enum: it is set by name, validated at
// SET time against a fixed list, and stored as a zero-based index. read_mode()
// returns that index (the storage global), and its on_change records the last
// index seen via the typed SysVarChange::as_enum() accessor.

#include <string>

#include <villagesql/preview/sys_var.h>
#include <villagesql/vsql.h>

using namespace vsql;
namespace sv = vsql::preview_sys_var;

static long long g_max_items;
static char *g_label;
static unsigned long g_mode;  // index into MODE_NAMES; default "grant"
static long long g_last_mode_change = -1;  // last index seen by on_change

// The allowed names for the `mode` enum, in index order:
//   0 = activate, 1 = grant, 2 = sync
static const char *const MODE_NAMES[] = {"activate", "grant", "sync"};

// on_change for mode: records the committed index via the typed as_enum()
// accessor, so a test can confirm the callback fires with the right value.
static void on_mode_change(sv::SysVarChange change) {
  if (change.is_enum())
    g_last_mode_change = static_cast<long long>(change.as_enum());
}

static auto SYS_VARS = sv::make_capability({
    sv::make_int("max_items", "Maximum number of items to process",
                 &g_max_items, 100, 0, 1000000),
    sv::make_str("label", "A label string for this extension", &g_label,
                 "default_label"),
    sv::make_enum("mode", "An enumerated mode (activate, grant, sync)", &g_mode,
                  MODE_NAMES, /*def_val=*/1)
        .on_change<&on_mode_change>(),
});

// Returns the current value of max_items by reading the storage global
// directly. Safe for INT variables — no locking required.
void read_max_items_impl(IntResult out) { out.set(g_max_items); }

// Sets max_items via SYS_VARS.set() so MySQL handles locking, range
// validation, and persistence. The storage global is updated by MySQL on
// success.
//
// scope controls persistence:
//   nullptr        - update running value only (GLOBAL), not persisted
//   "PERSIST"      - update running value AND write to mysqld-auto.cnf
//                    (survives restart)
//   "PERSIST_ONLY" - write to mysqld-auto.cnf only, running value unchanged
//                    (takes effect on next restart)
void write_max_items_impl(IntArg value, IntResult out) {
  if (value.is_null()) {
    out.set(1);
    return;
  }
  out.set(
      SYS_VARS.set("vsql_config_vars_test", "max_items", nullptr, value.value())
          ? 1
          : 0);
}

// Reads the label variable via SYS_VARS.get() to exercise the get path.
// Returns NULL if the get fails.
void read_label_impl(StringResult out) {
  std::string val;
  if (SYS_VARS.get("vsql_config_vars_test", "label", val)) {
    out.set_null();
    return;
  }
  out.set(val);
}

// Returns the current mode as its zero-based index, read straight from the
// storage global (an enum is stored as its index).
void read_mode_impl(IntResult out) { out.set(static_cast<long long>(g_mode)); }

// Returns the index the mode on_change last saw, or -1 if it has not fired.
void last_mode_change_impl(IntResult out) { out.set(g_last_mode_change); }

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .with(SYS_VARS)
        .func(make_func<&read_max_items_impl>("read_max_items")
                  .returns(INT)
                  .no_params()
                  .build())
        .func(make_func<&write_max_items_impl>("write_max_items")
                  .returns(INT)
                  .param(INT)
                  .build())
        .func(make_func<&read_label_impl>("read_label")
                  .returns(STRING)
                  .no_params()
                  .build())
        .func(make_func<&read_mode_impl>("read_mode")
                  .returns(INT)
                  .no_params()
                  .build())
        .func(make_func<&last_mode_change_impl>("last_mode_change")
                  .returns(INT)
                  .no_params()
                  .build()))
