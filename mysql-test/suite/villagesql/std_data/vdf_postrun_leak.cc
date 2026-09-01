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

// Test only extension for the VDF prerun/postrun leak regression.
// leak_probe's prerun allocates heap state (tracked by a global balance
// counter) AND requests a result buffer. When fix_fields fails after prerun
// (forced via the vdf_fail_after_prerun DBUG point), postrun must still run to
// free that state. live_state_count() reports the balance so the test can
// assert it returns to zero.

#include <villagesql/vsql.h>

#include <atomic>
#include <cstdio>

using namespace vsql;

static std::atomic<long long> g_live_state_count{0};
static std::atomic<long long> g_prerun_count{0};
static std::atomic<long long> g_postrun_count{0};

struct LeakState {
  long long n = 0;
  LeakState() { g_live_state_count.fetch_add(1); }
  ~LeakState() { g_live_state_count.fetch_sub(1); }
};

void leak_probe_prerun(PrerunArgs, PrerunResult out) {
  g_prerun_count.fetch_add(1);
  out.request_buffer_size(64);
  out.set_user_data(new LeakState{});
}

void leak_probe(LeakState &state, StringResult out) {
  state.n++;
  auto out_buf = out.buffer();
  int len = snprintf(reinterpret_cast<char *>(out_buf.data()), out_buf.size(),
                     "%lld", state.n);
  out.set_length(static_cast<size_t>(len));
}

void leak_probe_postrun(PostrunArgs args) {
  g_postrun_count.fetch_add(1);
  args.delete_state<LeakState>();
}

void live_state_count(IntResult out) { out.set(g_live_state_count.load()); }
void prerun_count(IntResult out) { out.set(g_prerun_count.load()); }
void postrun_count(IntResult out) { out.set(g_postrun_count.load()); }

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .func(make_func<&leak_probe>("leak_probe")
                  .returns(STRING)
                  .no_params()
                  .prerun<&leak_probe_prerun>()
                  .postrun<&leak_probe_postrun>()
                  .build())
        .func(make_func<&live_state_count>("live_state_count")
                  .returns(INT)
                  .no_params()
                  .build())
        .func(make_func<&prerun_count>("prerun_count")
                  .returns(INT)
                  .no_params()
                  .build())
        .func(make_func<&postrun_count>("postrun_count")
                  .returns(INT)
                  .no_params()
                  .build()))
