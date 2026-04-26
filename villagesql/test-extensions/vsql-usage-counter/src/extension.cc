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

// VillageSQL test extension demonstrating status variable usage counters.
//
// Provides a single VDF vsql_usage_counter.add(a, b) that returns a + b
// and increments a call counter on each invocation. The counter is exposed
// as a status variable so it is visible in SHOW GLOBAL STATUS and
// performance_schema.global_status.
//
// Status variables:
//   vsql_usage_counter.add_calls  INT  total number of add() invocations

#include <villagesql/vsql.h>

using namespace vsql;

// Counter incremented on every add() call. Concurrent increments from
// multiple query threads may occasionally be lost (non-atomic ++), which is
// acceptable for an approximate call counter exposed via SHOW STATUS.
static long long g_add_calls = 0;

void add_impl(IntArg a, IntArg b, IntResult out) {
  g_add_calls++;
  if (a.is_null() || b.is_null()) {
    out.set_null();
    return;
  }
  out.set(a.value() + b.value());
}

VEF_GENERATE_ENTRY_POINTS(make_extension()
                              .func(make_func<&add_impl>("add")
                                        .returns(INT)
                                        .param(INT)
                                        .param(INT)
                                        .build())
                              .status_var(make_status_var_int("add_calls",
                                                              &g_add_calls)))
