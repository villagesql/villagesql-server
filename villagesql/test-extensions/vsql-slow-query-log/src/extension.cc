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
#include <cinttypes>
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

// Remembers the previous logged statement's digest_hash and read_rnd_next so a
// repeated identical query can be checked: a per-statement delta counter
// reports the same value each run, a cumulative session total keeps growing.
// This is how the test verifies the start-of-statement status_var snapshot is
// taken (without it, stat_delta falls back to cumulative totals). Guarded by
// g_log_mutex.
static char g_prev_digest_hash[65];
static uint64_t g_prev_read_rnd_next;
static bool g_have_prev;

static void slow_query_hook(const se::StatementEventArgs &args,
                            se::StatementEventResult &result) {
  if (!g_enabled) return;
  if (args.query_time_secs() * 1000.0 < static_cast<double>(g_threshold_ms))
    return;

  time_t now = static_cast<time_t>(args.query_start_utime() / 1000000);
  char ts[32];
  struct tm tm_utc;
  gmtime_r(&now,
           &tm_utc);  // TODO(villagesql-windows): use gmtime_s(&tm_utc, &now)
  strftime(ts, sizeof(ts), "%Y-%m-%dT%H:%M:%SZ", &tm_utc);

  std::lock_guard<std::mutex> lock(g_log_mutex);
  FILE *f = fopen(g_log_filename, "a");
  if (f == nullptr) {
    result.error_msg("failed to open '%s': %s", g_log_filename,
                     strerror(errno));
    return;
  }

  fprintf(f, "# Time: %s\n", ts);
  fprintf(f, "# User@Host: %s @ %s  Id: %lu\n", args.user() ? args.user() : "",
          args.client_ip() ? args.client_ip() : "", args.connection_id());
  fprintf(f,
          "# Schema: %s  Query_time: %.6f  Lock_time: %.6f"
          "  Rows_sent: %" PRIu64 "  Rows_examined: %" PRIu64 "\n",
          args.schema() ? args.schema() : "", args.query_time_secs(),
          args.lock_time_secs(), args.rows_sent(), args.rows_examined());
  const char *digest_hash = args.digest_hash();
  fprintf(f, "# Digest_hash: %s  Read_rnd_next: %" PRIu64 "\n",
          digest_hash ? digest_hash : "", args.read_rnd_next());

  // When the same statement (same digest_hash) runs again, compare its
  // read_rnd_next against the previous run. Equal => per-statement delta;
  // greater => the counter is leaking the cumulative session total. Emitting a
  // fixed token keeps the .result stable across plans and machines.
  if (digest_hash != nullptr) {
    if (g_have_prev && strncmp(g_prev_digest_hash, digest_hash,
                               sizeof(g_prev_digest_hash)) == 0) {
      const bool stable = args.read_rnd_next() == g_prev_read_rnd_next;
      fprintf(f, "# Read_rnd_next_delta_stable: %s\n", stable ? "YES" : "NO");
    }
    snprintf(g_prev_digest_hash, sizeof(g_prev_digest_hash), "%s", digest_hash);
    g_prev_read_rnd_next = args.read_rnd_next();
    g_have_prev = true;
  }

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
    STATEMENT_EVENT;

VEF_GENERATE_ENTRY_POINTS(make_extension().with(SYS_VARS).with(STATEMENT_EVENT))
