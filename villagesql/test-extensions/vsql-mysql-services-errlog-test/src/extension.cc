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

// vsql_mysql_services_errlog_test extension: writes to the server error log
// through the MySQL Services capability.
//
// Consumes one server-core service (always registered, so no component /
// restart / setup is needed to run the test):
//   mysql_simple_error_log — emit a line to the server error log
//
// VDF:
//   log_warning(text) -> INT
//       Emits `text` to the error log at WARNING severity. Returns 0 on
//       success, 1 on error. The line is observable via
//       performance_schema.error_log, which the test asserts against.

#include <cstddef>

#include <mysql/components/services/mysql_simple_error_log.h>
#include <mysqld_error.h>  // ER_LOG_PRINTF_MSG
#include <villagesql/preview/mysql_services.h>
#include <villagesql/vsql.h>

using namespace vsql;

static preview_mysql_services::MysqlServices services;
VSQL_REQUIRE_SERVICE(services, mysql_simple_error_log, error_log);

// log_warning(text)
//
// Writes `text` to the server error log via mysql_simple_error_log::emit.
// ER_LOG_PRINTF_MSG is the generic "%s" error-log message code, so the text
// is logged verbatim. WARNING severity is used (not INFORMATION) so the line
// survives the default log_error_verbosity of 2, which drops info/notes.
// Returns 0 on success, 1 on error.
void log_warning(StringArg text, IntResult out) {
  if (text.is_null() || !error_log.valid()) {
    out.set(1);
    return;
  }
  const std::string_view msg = text.value();
  const bool err = error_log->emit("vsql_mysql_services_errlog_test", __FILE__,
                                   __LINE__, MYSQL_ERROR_LOG_SEVERITY_WARNING,
                                   ER_LOG_PRINTF_MSG, std::string(msg).c_str());
  out.set(err ? 1 : 0);
}

VEF_GENERATE_ENTRY_POINTS(make_extension().with(services).func(
    make_func<&log_warning>("log_warning").returns(INT).param(STRING).build()))
