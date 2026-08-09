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

// VillageSQL test extension exercising the extension-side on_init / on_deinit
// builder hooks. Each hook appends a line to a marker file so a test can
// observe that BOTH ran, including on_deinit.
//
// A file (not an in-memory counter) is used because on_deinit runs as the
// extension is unloaded: the .so is dlclose'd at UNINSTALL, so any process-
// local state is gone before SQL could read it back. A file survives the
// unload, so the test can INSTALL, UNINSTALL, then read the file and assert
// both "on_init" and "on_deinit" are present.
//
// The marker path lives under MYSQL_TMP_DIR (set by mysql-test-run and
// inherited by the server process) so it is per-run and cleaned up with the
// test vardir; it falls back to /tmp when that env var is absent. on_init /
// on_deinit run at extension load/unload -- before/after any SQL -- so a sys
// var cannot supply the path, and getenv() is the natural channel for
// load-time code.

#include <cstdio>
#include <cstdlib>
#include <string>

#include <villagesql/vsql.h>

using namespace vsql;

static std::string marker_path() {
  const char *dir = getenv("MYSQL_TMP_DIR");
  std::string base = (dir != nullptr) ? dir : "/tmp";
  return base + "/vsql_on_init_test.marker";
}

static void append_marker(const char *line) {
  FILE *f = fopen(marker_path().c_str(), "a");
  if (f != nullptr) {
    fprintf(f, "%s\n", line);
    fclose(f);
  }
}

// Runs extension-side, once, after the extension is validated and accepted.
static void on_init_hook() { append_marker("on_init"); }

// Runs extension-side at unload (UNINSTALL / shutdown).
static void on_deinit_hook() { append_marker("on_deinit"); }

VEF_GENERATE_ENTRY_POINTS(
    make_extension().on_init<&on_init_hook>().on_deinit<&on_deinit_hook>())
