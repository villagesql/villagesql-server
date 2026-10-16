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

#include <cstddef>
#include <string>
#include <utility>

#include <mysql/components/services/defs/mysql_string_defs.h>
#include <mysql/components/services/mysql_current_thread_reader.h>
#include <mysql/components/services/mysql_string.h>
#include <mysql/components/services/mysql_thd_attributes.h>
#include <mysql/components/services/security_context.h>
#include <mysql/mysql_lex_string.h>
#include <villagesql/preview/mysql_services.h>
#include <villagesql/vsql.h>

using namespace vsql;

static preview_mysql_services::MysqlServices services;
VSQL_REQUIRE_SERVICE(services, mysql_current_thread_reader, thd_reader);
VSQL_REQUIRE_SERVICE(services, mysql_thd_attributes, attrs);
VSQL_REQUIRE_SERVICE(services, mysql_string_factory, string_factory);
VSQL_REQUIRE_SERVICE(services, mysql_string_get_data_in_charset, string_data);
VSQL_REQUIRE_SERVICE(services, mysql_thd_security_context, thd_security);
VSQL_REQUIRE_SERVICE(services, mysql_security_context_options, security_opts);

// session_sql_command()
//
// Reads the current statement's SQL command name via thd_reader and attrs.
// "sql_command" is returned as a mysql_cstring_with_length (a plain
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

static bool session_services_valid() {
  return thd_reader.valid() && attrs.valid() && string_factory.valid() &&
         string_data.valid() && thd_security.valid() && security_opts.valid();
}

static MYSQL_THD current_session(PrerunResult result) {
  MYSQL_THD thd = nullptr;
  if (!session_services_valid()) {
    result.error("MySQL session services not wired up");
  } else if (thd_reader->get(&thd) || thd == nullptr) {
    result.error("no current session");
    thd = nullptr;
  }
  return thd;
}

void session_schema_prerun(PrerunArgs, PrerunResult result) {
  MYSQL_THD thd = current_session(result);
  if (thd == nullptr) return;

  my_h_string handle = nullptr;
  if (attrs->get(thd, "schema", &handle)) {
    result.error("cannot read the default schema");
    return;
  }
  const char *ptr = nullptr;
  size_t len = 0;
  CHARSET_INFO_h cs = nullptr;
  const bool read_failed = string_data->get_data(handle, &ptr, &len, &cs);
  std::string schema =
      read_failed || ptr == nullptr ? std::string() : std::string(ptr, len);
  string_factory->destroy(handle);
  if (read_failed) {
    result.error("cannot read the default schema");
    return;
  }
  if (schema.empty()) {
    result.error("requires a default schema; run USE <schema> first");
    return;
  }
  result.set_user_data(new std::string(std::move(schema)));
}

void session_priv_user_prerun(PrerunArgs, PrerunResult result) {
  MYSQL_THD thd = current_session(result);
  if (thd == nullptr) return;

  Security_context_handle ctx = nullptr;
  MYSQL_LEX_CSTRING priv_user{nullptr, 0};
  MYSQL_LEX_CSTRING priv_host{nullptr, 0};
  if (thd_security->get(thd, &ctx) ||
      security_opts->get(ctx, "priv_user", &priv_user) ||
      security_opts->get(ctx, "priv_host", &priv_host)) {
    result.error("cannot read the privilege user");
    return;
  }
  std::string user(priv_user.str, priv_user.length);
  user += '@';
  user.append(priv_host.str, priv_host.length);
  result.set_user_data(new std::string(std::move(user)));
}

void session_value_postrun(PostrunArgs args) {
  args.delete_state<std::string>();
}

void session_value(const std::string &value, StringResult out) {
  out.set(value);
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .with(services)
        .func(make_func<&session_sql_command>("session_sql_command")
                  .returns(STRING)
                  .no_params()
                  .build())
        .func(make_func<&session_value>("session_schema")
                  .returns(STRING)
                  .no_params()
                  .prerun<&session_schema_prerun>()
                  .postrun<&session_value_postrun>()
                  .build())
        .func(make_func<&session_value>("session_priv_user")
                  .returns(STRING)
                  .no_params()
                  .prerun<&session_priv_user_prerun>()
                  .postrun<&session_value_postrun>()
                  .build()))
