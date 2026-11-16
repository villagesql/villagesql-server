/* Copyright (c) 2026 VillageSQL Contributors
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see <https://www.gnu.org/licenses/>.
 */

// Test-only extension for a VDF with prerun state in a generated column or
// CHECK constraint. Such an expression is resolved once, when the table is
// opened, and reused by every statement that uses the table.
//
// state_probe(x) returns x when its prerun state is there, and -1 when it is
// not. prerun_count() and postrun_count() count the calls of each.

#include <villagesql/vsql.h>

#include <atomic>

using namespace vsql;

static std::atomic<long long> g_prerun_count{0};
static std::atomic<long long> g_postrun_count{0};

struct ProbeState {
  long long rows = 0;
};

void state_probe_prerun(PrerunArgs, PrerunResult out) {
  g_prerun_count.fetch_add(1);
  out.set_user_data(new ProbeState{});
}

void state_probe(void *&user_data, IntArg x, IntResult out) {
  if (user_data == nullptr) {
    out.set(-1);
    return;
  }
  static_cast<ProbeState *>(user_data)->rows++;
  if (x.is_null()) {
    out.set_null();
    return;
  }
  out.set(x.value());
}

void state_probe_postrun(PostrunArgs args) {
  g_postrun_count.fetch_add(1);
  delete static_cast<ProbeState *>(args.user_data());
}

void prerun_count(IntResult out) { out.set(g_prerun_count.load()); }
void postrun_count(IntResult out) { out.set(g_postrun_count.load()); }

VEF_GENERATE_ENTRY_POINTS(make_extension()
                              .func(make_func<&state_probe>("state_probe")
                                        .returns(INT)
                                        .param(INT)
                                        .deterministic()
                                        .prerun<&state_probe_prerun>()
                                        .postrun<&state_probe_postrun>()
                                        .build())
                              .func(make_func<&prerun_count>("prerun_count")
                                        .returns(INT)
                                        .no_params()
                                        .build())
                              .func(make_func<&postrun_count>("postrun_count")
                                        .returns(INT)
                                        .no_params()
                                        .build()))
