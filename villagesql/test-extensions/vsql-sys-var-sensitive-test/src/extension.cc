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

// VillageSQL test extension for SENSITIVE system variables.
//
//   api_token  STR, sensitive, the kind of value an administrator configures
//              and SQL users must not see
//   plain      INT, not sensitive
//
// read_token() and write_token() read and write api_token through
// SYS_VARS.get() and SYS_VARS.set(), which the extension can do whoever is
// connected. read_var(component, name) reads any variable through
// SYS_VARS.get(). init_token() returns the api_token that on_init read and
// wrote back, which at server startup happens before any session exists.

#include <string>

#include <villagesql/preview/sys_var.h>
#include <villagesql/vsql.h>

using namespace vsql;
namespace sv = vsql::preview_sys_var;

static char *g_api_token;
static long long g_plain;

static auto SYS_VARS = sv::make_capability({
    sv::make_str("api_token", "Token for a remote API", &g_api_token, "none")
        .sensitive(),
    sv::make_int("plain", "Not sensitive", &g_plain, 1, 0, 1000),
});

// The api_token on_init read, and whether on_init both read it and wrote it
// back unchanged.
static std::string g_init_token;
static bool g_init_token_ok = false;

static void on_init_hook() {
  g_init_token_ok =
      !SYS_VARS.get("vsql_sys_var_sensitive_test", "api_token", g_init_token) &&
      !SYS_VARS.set("vsql_sys_var_sensitive_test", "api_token", nullptr,
                    g_init_token);
}

static void on_deinit_hook() {}

// Returns the api_token on_init read, or NULL if the read or the write back
// failed.
void init_token_impl(StringResult out) {
  if (!g_init_token_ok) {
    out.set_null();
    return;
  }
  out.set(g_init_token);
}

// Returns api_token, or NULL if the read failed.
void read_token_impl(StringResult out) {
  std::string val;
  if (SYS_VARS.get("vsql_sys_var_sensitive_test", "api_token", val)) {
    out.set_null();
    return;
  }
  out.set(val);
}

// Returns the named variable, or NULL if the read failed. component is an
// extension name, or "mysql_server" for a built-in variable.
void read_var_impl(StringArg component, StringArg name, StringResult out) {
  if (component.is_null() || name.is_null()) {
    out.set_null();
    return;
  }
  const std::string component_str(component.value());
  const std::string name_str(name.value());
  std::string val;
  if (SYS_VARS.get(component_str.c_str(), name_str.c_str(), val)) {
    out.set_null();
    return;
  }
  out.set(val);
}

// Sets api_token, as an extension would when it rotates a token. Returns 1 if
// the set reported an error, 0 if it succeeded.
void write_token_impl(StringArg value, IntResult out) {
  if (value.is_null()) {
    out.set(1);
    return;
  }
  const std::string value_str(value.value());
  out.set(SYS_VARS.set("vsql_sys_var_sensitive_test", "api_token", nullptr,
                       value_str)
              ? 1
              : 0);
}

VEF_GENERATE_ENTRY_POINTS(make_extension()
                              .with(SYS_VARS)
                              .on_init<&on_init_hook>()
                              .on_deinit<&on_deinit_hook>()
                              .func(make_func<&read_token_impl>("read_token")
                                        .returns(STRING)
                                        .no_params()
                                        .build())
                              .func(make_func<&write_token_impl>("write_token")
                                        .returns(INT)
                                        .param(STRING)
                                        .build())
                              .func(make_func<&init_token_impl>("init_token")
                                        .returns(STRING)
                                        .no_params()
                                        .build())
                              .func(make_func<&read_var_impl>("read_var")
                                        .returns(STRING)
                                        .param(STRING)
                                        .param(STRING)
                                        .build()))
