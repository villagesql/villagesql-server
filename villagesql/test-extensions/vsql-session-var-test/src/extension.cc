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

// vsql_session_var_test extension: exercises the "vsql::session_var" preview
// capability, which declares per-session (THD-local) system variables — the
// functional equivalent of a plugin's MYSQL_THDVAR_*.
//
// Declares two session-scoped variables:
//   ef_search (INT)     — an ef_search-style search-width knob
//   session_label (STR)
//
// Each connection has its own value (SET SESSION), with the descriptor's
// def_val as the global default. The VDFs read the caller's per-session value
// via get_session_int / get_session_str, which resolve current_thd.

#include <string>

#include <villagesql/preview/session_var.h>
#include <villagesql/vsql.h>

using namespace vsql;
namespace sv = vsql::preview_session_var;

static auto SESSION_VARS = sv::make_capability({
    sv::make_int("ef_search",
                 "Session-scoped search width (larger = slower but more "
                 "accurate)",
                 20, 1, 4096),
    sv::make_str("session_label", "Session-scoped label string",
                 "default_label"),
});

// Reads the caller's per-session ef_search value via get_session_int — the
// equivalent of a plugin's THDVAR(thd, ef_search). Returns NULL on error.
void read_ef_search_impl(IntResult out) {
  long long val = 0;
  if (SESSION_VARS.get_session_int("vsql_session_var_test", "ef_search", val)) {
    out.set_null();
    return;
  }
  out.set(val);
}

// Reads the caller's per-session session_label value via get_session_str.
void read_session_label_impl(StringResult out) {
  std::string val;
  if (SESSION_VARS.get_session_str("vsql_session_var_test", "session_label",
                                   val)) {
    out.set_null();
    return;
  }
  out.set(val);
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .with(SESSION_VARS)
        .func(make_func<&read_ef_search_impl>("read_ef_search")
                  .returns(INT)
                  .no_params()
                  .build())
        .func(make_func<&read_session_label_impl>("read_session_label")
                  .returns(STRING)
                  .no_params()
                  .build()))
