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
//   session_int_var (INT)
//   session_str_var (STR)
//
// Each connection has its own value (SET SESSION), with the descriptor's
// def_val as the global default.
//
// The INT VDF demonstrates the fast path: it resolves a reusable IntHandle
// once and reads the caller's per-session value through it, which is lock-free
// on the hot path (a per-thread base + offset load, no name lookup). The STR
// VDF uses the name-keyed get_session_str. Both resolve current_thd, so they
// must run on the connection thread.

#include <string>

#include <villagesql/preview/session_var.h>
#include <villagesql/vsql.h>

using namespace vsql;
namespace sv = vsql::preview_session_var;

static auto SESSION_VARS = sv::make_capability({
    sv::make_int("session_int_var", "Session-scoped integer value")
        .default_(20)
        .min(1)
        .max(4096),
    sv::make_str("session_str_var", "Session-scoped label string")
        .default_("default_label"),
});

// Bind the INT variable to a lazily-resolved IntVar. It resolves on first
// read() (once the extension has loaded) and caches the handle, so each read is
// lock-free — the equivalent of a plugin's THDVAR(thd, var). The read site is
// name-free.
static auto SESSION_INT = SESSION_VARS.int_var("session_int_var");

// Reads the caller's per-session session_int_var value.
void read_session_int_var_impl(IntResult out) {
  long long val = 0;
  if (SESSION_INT.read(val)) {
    out.set_null();
    return;
  }
  out.set(val);
}

// Reads the caller's per-session session_str_var value via get_session_str.
void read_session_str_var_impl(StringResult out) {
  std::string val;
  if (SESSION_VARS.get_session_str("session_str_var", val)) {
    out.set_null();
    return;
  }
  out.set(val);
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .with(SESSION_VARS)
        .func(make_func<&read_session_int_var_impl>("read_session_int_var")
                  .returns(INT)
                  .no_params()
                  .build())
        .func(make_func<&read_session_str_var_impl>("read_session_str_var")
                  .returns(STRING)
                  .no_params()
                  .build()))
