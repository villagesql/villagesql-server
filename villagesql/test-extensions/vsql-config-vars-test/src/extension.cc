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
//
// Also demonstrates the two patterns for accessing INT config vars from VDFs:
//   read_max_items()  - reads the global directly (fast path, no locking needed
//                       for INT variables)
//   write_max_items() - writes via sys_var::set() so MySQL applies locking,
//                       validation, and PERSIST support

#include <villagesql/vsql.h>

using namespace vsql;

static long long g_max_items;
static char *g_label;

// Returns the current value of max_items by reading the storage global
// directly. Safe for INT variables — no locking required.
void read_max_items_impl(IntResult out) { out.set(g_max_items); }

// Sets max_items via sys_var::set so MySQL handles locking, range validation,
// and persistence. The storage global is updated by MySQL on success.
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
      sys_var::set("vsql_config_vars_test", "max_items", nullptr, value.value())
          ? 1
          : 0);
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .sys_var(make_sys_var_int("max_items",
                                  "Maximum number of items to process",
                                  &g_max_items, 100, 0, 1000000))
        .sys_var(make_sys_var_str("label", "A label string for this extension",
                                  &g_label, "default_label"))
        .func(make_func<&read_max_items_impl>("read_max_items")
                  .returns(INT)
                  .build())
        .func(make_func<&write_max_items_impl>("write_max_items")
                  .returns(INT)
                  .param(INT)
                  .build()))
