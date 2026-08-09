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

// vsql_mysql_services_session_test extension: a minimal "session attribute"
// reader built on the MySQL Services capability — a small stand-in for the
// vsql::preview::session capability, but implemented purely by consuming MySQL
// component services rather than server internals.
//
// It consumes two server-core services (both always registered, so no keyring
// component / server restart / setup is needed to run the test):
//   mysql_current_thread_reader  — hands back the current THD
//   mysql_thd_attributes         — reads a named attribute off that THD
//
// VDF:
//   session_sql_command() -> STRING
//       Returns the SQL command name of the running statement (the THD's
//       "sql_command" attribute), or NULL if the services are unavailable.

#include <cstddef>

#include <mysql/components/services/defs/mysql_string_defs.h>
#include <mysql/components/services/mysql_current_thread_reader.h>
#include <mysql/components/services/mysql_thd_attributes.h>
#include <villagesql/preview/mysql_services.h>
#include <villagesql/vsql.h>

using namespace vsql;

static preview_mysql_services::MysqlServices services;
VSQL_REQUIRE_SERVICE(services, mysql_current_thread_reader, thd_reader);
VSQL_REQUIRE_SERVICE(services, mysql_thd_attributes, attrs);

// session_sql_command()
//
// Reads the current statement's SQL command name via the two consumed
// services. "sql_command" is returned as a mysql_cstring_with_length (a plain
// {str,length}), so no string-handle conversion is needed.
void session_sql_command(StringResult out) {
  if (!thd_reader.valid() || !attrs.valid()) {
    out.error("MySQL session services not wired up");
    return;
  }

  MYSQL_THD thd = nullptr;
  if (thd_reader->get(&thd) || thd == nullptr) {
    out.set_null();
    return;
  }

  mysql_cstring_with_length value{nullptr, 0};
  if (attrs->get(thd, "sql_command", &value) || value.str == nullptr) {
    out.set_null();
    return;
  }

  out.set(std::string_view(value.str, value.length));
}

VEF_GENERATE_ENTRY_POINTS(make_extension().with(services).func(
    make_func<&session_sql_command>("session_sql_command")
        .returns(STRING)
        .no_params()
        .build()))
