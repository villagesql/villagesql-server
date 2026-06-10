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

// VillageSQL slow query log extension.
//
// Logs queries exceeding vsql_slow_query_log.threshold_ms to
// vsql_slow_query_log.log_file. Disabled by default; enable with
//   SET GLOBAL vsql_slow_query_log.enabled = ON;

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <mutex>

#include <villagesql/preview/statement_event.h>
#include <villagesql/preview/sys_var.h>
#include <villagesql/vsql.h>

using namespace vsql;
namespace sv = vsql::preview_sys_var;
namespace se = vsql::preview_statement_event;

static bool g_enabled;
static long long g_threshold_ms;
static char *g_log_filename;

static std::mutex g_log_mutex;

static void slow_query_hook(const se::StatementEventArgs &args,
                            se::StatementEventResult &result) {
  if (!g_enabled) return;
  if (args.query_time_secs() * 1000.0 < static_cast<double>(g_threshold_ms))
    return;

  time_t now = static_cast<time_t>(args.query_start_utime() / 1000000);
  uint32_t usec = static_cast<uint32_t>(args.query_start_utime() % 1000000);
  struct tm tm_buf;
  gmtime_r(&now, &tm_buf);
  char ts[48];
  strftime(ts, sizeof(ts), "%Y-%m-%dT%H:%M:%S.", &tm_buf);
  snprintf(ts + 20, sizeof(ts) - 20, "%06uZ", usec);

  std::lock_guard<std::mutex> lock(g_log_mutex);
  FILE *f = fopen(g_log_filename, "a");
  if (f == nullptr) {
    result.error_msg("failed to open '%s': %s", g_log_filename,
                     strerror(errno));
    return;
  }

  fprintf(f, "# Time: %s\n", ts);
  fprintf(f, "# User@Host: %s @ %s  Id: %lu\n", args.user() ? args.user() : "",
          args.host() ? args.host() : "", args.connection_id());
  fprintf(f,
          "# Schema: %s  Query_time: %.6f  Lock_time: %.6f"
          "  Rows_sent: %llu  Rows_examined: %llu\n",
          args.schema() ? args.schema() : "", args.query_time_secs(),
          args.lock_time_secs(), (unsigned long long)args.rows_sent(),
          (unsigned long long)args.rows_examined());
  fprintf(f, "SET timestamp=%llu;\n", (unsigned long long)now);
  auto q = args.query();
  fprintf(f, "%.*s;\n", (int)q.size(), q.data());
  fclose(f);
}

static auto SYS_VARS = sv::make_capability({
    sv::make_bool("enabled",
                  "Enable or disable the slow query log (default: OFF)",
                  &g_enabled, false),
    sv::make_int("threshold_ms",
                 "Minimum query execution time in ms to log (default: 1000)",
                 &g_threshold_ms, 1000, 0, 3600000),
    sv::make_str("log_file",
                 "Path to the slow query log file "
                 "(default: /tmp/vsql_slow_query.log)",
                 &g_log_filename, "/tmp/vsql_slow_query.log"),
});

static se::StatementEventCapability<VEF_STATEMENT_EVENT_POSTEXECUTE,
                                    &slow_query_hook>
    QUERY_HOOK;

VEF_GENERATE_ENTRY_POINTS(make_extension().with(SYS_VARS).with(QUERY_HOOK))
