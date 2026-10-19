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

// VillageSQL test extension whose registration carries an on_init hook with no
// on_deinit. This is the mirror of vsql-deinit-only-test, covering the other
// half of the server's pairing check. Installing it must fail, and neither hook
// may run.
//
// Uses VEF_GENERATE_REGISTRATION to get the registration helper, then clears
// on_deinit after the builder runs, which is the only way past the
// static_assert. Each hook appends to a marker file, so the test can assert
// that no marker file exists.

#include <cstdio>
#include <cstdlib>
#include <string>

#include <villagesql/vsql.h>

using namespace vsql;

static std::string marker_path() {
  const char *dir = getenv("MYSQL_TMP_DIR");
  const std::string base = (dir != nullptr) ? dir : "/tmp";
  return base + "/vsql_init_only_test.marker";
}

static void append_marker(const char *line) {
  FILE *f = fopen(marker_path().c_str(), "a");
  if (f != nullptr) {
    fprintf(f, "%s\n", line);
    fclose(f);
  }
}

static void on_init_hook() { append_marker("on_init"); }

static void on_deinit_hook() { append_marker("on_deinit"); }

VEF_GENERATE_REGISTRATION(
    make_extension().on_init<&on_init_hook>().on_deinit<&on_deinit_hook>())

// Override vef_register to drop on_deinit after the builder runs, bypassing
// the builder's static_assert so the server's check is what rejects this.
extern "C" vef_registration_t *vef_register(vef_register_arg_t *arg) {
  auto *reg = _vef_do_register(arg);
  if (reg != nullptr) reg->on_deinit = nullptr;
  return reg;
}

extern "C" void vef_unregister(vef_unregister_arg_t *arg,
                               vef_registration_t *reg) {
  (void)arg;
  (void)reg;
}
