/**
 * @file prometheus_exporter.cc
 * @brief Embedded Prometheus metrics exporter plugin for VillageSQL/MySQL.
 *
 * This plugin serves Prometheus text exposition format metrics via an
 * embedded HTTP server, eliminating the need for an external
 * @c mysqld_exporter sidecar process.
 *
 * ## Architecture
 *
 * The plugin spawns a single background thread that runs a poll-based
 * HTTP server on the configured port. When Prometheus scrapes @c /metrics,
 * the plugin executes standard SQL queries via @c srv_session (no external
 * network connection) and formats results in Prometheus text format.
 *
 * ## Supported Metric Sources
 *
 * - @c SHOW GLOBAL STATUS (mysql_global_status_*)
 * - @c SHOW GLOBAL VARIABLES (mysql_global_variables_*)
 * - @c INFORMATION_SCHEMA.INNODB_METRICS (mysql_innodb_metrics_*)
 * - @c SHOW REPLICA STATUS (mysql_replica_*) with multi-channel labels
 * - @c SHOW BINARY LOGS (mysql_binlog_*)
 *
 * ## Security Notes
 *
 * The HTTP endpoint has no authentication or TLS. Use a loopback bind address
 * and restrict network access to the port. The plugin executes all queries
 * through the server's internal session service using a configurable security
 * context user.
 *
 * ## Platform Notes
 *
 * Requires Linux due to use of @c eventfd() and @c MSG_NOSIGNAL.
 *
 * @sa plugin/prometheus_exporter/README.md
 */

/* Copyright (c) 2025, Oracle and/or its affiliates.
   Copyright (c) 2026 VillageSQL Contributors

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License, version 2.0,
  as published by the Free Software Foundation.

  This program is designed to work with certain software (including
  but not limited to OpenSSL) that is licensed under separate terms,
  as designated in a particular file or component or in included license
  documentation.  The authors of MySQL hereby grant you an additional
  permission to link the program and your derivative works with the
  separately licensed software that they have either included with
  the program or referenced in the documentation.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License, version 2.0, for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#define LOG_COMPONENT_TAG "prometheus_exporter"

#include <mysql/plugin.h>
#include <mysql/service_command.h>
#include <mysql/service_security_context.h>
#include <mysql/service_srv_session.h>
#include <mysql/service_srv_session_info.h>

#include <mysql/components/my_service.h>
#include <mysql/components/services/log_builtins.h>
#include <mysqld_error.h>

#include "m_string.h"
#include "my_inttypes.h"
#include "my_sys.h"
#include "my_thread.h"
#include "mysql/strings/m_ctype.h"
#include "sql/sql_plugin.h"
#include "template_utils.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <sys/eventfd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstring>
#include <string>
#include <vector>

static SERVICE_TYPE(registry) *reg_srv = nullptr;
SERVICE_TYPE(log_builtins) *log_bi = nullptr;
SERVICE_TYPE(log_builtins_string) *log_bs = nullptr;

/**
 * @name Plugin Configuration Variables
 * @{
 */

/** @brief Enable the Prometheus metrics exporter HTTP endpoint. */
static bool prom_enabled = false;

/**
 * @brief TCP port for the Prometheus exporter HTTP endpoint.
 *
 * Valid range: 1024-65535. Requires server restart to change.
 */
static unsigned int prom_port = 9104;

/**
 * @brief IPv4 address to bind the HTTP endpoint to.
 *
 * Must be a numeric IPv4 address (e.g. "127.0.0.1"). Hostnames are not
 * accepted. Requires server restart to change.
 */
static char *prom_bind_address = nullptr;

/**
 * @brief MySQL account used for internal session-based metric queries.
 *
 * The account must exist on localhost and have sufficient privileges to
 * query INNODB_METRICS, SHOW REPLICA STATUS, and related tables.
 * Requires server restart to change.
 */
static char *prom_security_user = nullptr;

/** @} */

static MYSQL_SYSVAR_BOOL(enabled, prom_enabled,
                         PLUGIN_VAR_READONLY | PLUGIN_VAR_OPCMDARG,
                         "Enable the Prometheus metrics exporter HTTP "
                         "endpoint. Default OFF.",
                         nullptr, nullptr, false);

static MYSQL_SYSVAR_UINT(port, prom_port,
                         PLUGIN_VAR_READONLY | PLUGIN_VAR_OPCMDARG,
                         "TCP port for the Prometheus exporter HTTP "
                         "endpoint. Default 9104.",
                         nullptr, nullptr, 9104, 1024, 65535, 0);

static MYSQL_SYSVAR_STR(bind_address, prom_bind_address,
                        PLUGIN_VAR_READONLY | PLUGIN_VAR_OPCMDARG |
                            PLUGIN_VAR_MEMALLOC,
                        "Bind address for the Prometheus exporter HTTP "
                        "endpoint. Default 127.0.0.1.",
                        nullptr, nullptr, "127.0.0.1");

static MYSQL_SYSVAR_STR(security_user, prom_security_user,
                        PLUGIN_VAR_READONLY | PLUGIN_VAR_OPCMDARG |
                            PLUGIN_VAR_MEMALLOC,
                        "MySQL account used internally to run the metric "
                        "collection queries. The account must exist on "
                        "localhost. Default: root. For reduced privilege, "
                        "use an account granted PROCESS, REPLICATION CLIENT, "
                        "and SELECT on information_schema.",
                        nullptr, nullptr, "root");

static SYS_VAR *prom_system_vars[] = {
    MYSQL_SYSVAR(enabled),
    MYSQL_SYSVAR(port),
    MYSQL_SYSVAR(bind_address),
    MYSQL_SYSVAR(security_user),
    nullptr,
};

/**
 * @name Plugin Operational Metrics
 * @{
 */

/**
 * @brief Total number of /metrics scrapes served.
 *
 * Exposed via SHOW GLOBAL STATUS as Prometheus_exporter_requests_total.
 */
static std::atomic<uint64_t> g_requests_total{0};

/**
 * @brief Total number of scrape errors encountered.
 *
 * Incremented when any collector query fails. Exposed via
 * SHOW GLOBAL STATUS as Prometheus_exporter_errors_total.
 */
static std::atomic<uint64_t> g_errors_total{0};

/**
 * @brief Duration of the last scrape in microseconds.
 *
 * Exposed via SHOW GLOBAL STATUS as
 * Prometheus_exporter_scrape_duration_microseconds.
 */
static std::atomic<uint64_t> g_last_scrape_duration_us{0};

/** @} */

/**
 * @brief Per-plugin-instance context for the HTTP listener thread.
 *
 * Contains all state needed to manage the background listener thread
 * including file descriptors and shutdown coordination.
 */
struct PrometheusContext {
  my_thread_handle listener_thread;
  int listen_fd;
  int wakeup_fd;
  std::atomic<bool> shutdown_requested;
  void *plugin_ref;

  PrometheusContext()
      : listen_fd(-1),
        wakeup_fd(-1),
        shutdown_requested(false),
        plugin_ref(nullptr) {}
};

static const char *gauge_variables[] = {
    "Threads_connected",
    "Threads_running",
    "Threads_cached",
    "Threads_created",
    "Open_tables",
    "Open_files",
    "Open_streams",
    "Open_table_definitions",
    "Opened_tables",
    "Innodb_buffer_pool_pages_data",
    "Innodb_buffer_pool_pages_dirty",
    "Innodb_buffer_pool_pages_free",
    "Innodb_buffer_pool_pages_misc",
    "Innodb_buffer_pool_pages_total",
    "Innodb_buffer_pool_bytes_data",
    "Innodb_buffer_pool_bytes_dirty",
    "Innodb_page_size",
    "Innodb_data_pending_reads",
    "Innodb_data_pending_writes",
    "Innodb_data_pending_fsyncs",
    "Innodb_os_log_pending_writes",
    "Innodb_os_log_pending_fsyncs",
    "Innodb_row_lock_current_waits",
    "Key_blocks_unused",
    "Key_blocks_used",
    "Key_blocks_not_flushed",
    "Max_used_connections",
    "Uptime",
    "Uptime_since_flush_status",
    nullptr,
};

static bool is_gauge(const char *name) {
  for (const char **p = gauge_variables; *p != nullptr; ++p) {
    if (strcasecmp(name, *p) == 0) return true;
  }
  return false;
}

typedef const char *(*type_fn_t)(const char *name);

struct MetricsCollectorCtx {
  std::string *output;
  std::string prefix;
  type_fn_t type_fn;
  std::string current_name;
  std::string current_value;
  int col_index;
  bool error;
};

static const char *global_status_type(const char *name) {
  return is_gauge(name) ? "gauge" : "untyped";
}

static int prom_start_result_metadata(void *, uint, uint,
                                      const CHARSET_INFO *) {
  return 0;
}

static int prom_field_metadata(void *, struct st_send_field *,
                               const CHARSET_INFO *) {
  return 0;
}

static int prom_end_result_metadata(void *, uint, uint) { return 0; }

static int prom_start_row(void *ctx) {
  auto *mc = static_cast<MetricsCollectorCtx *>(ctx);
  mc->col_index = 0;
  mc->current_name.clear();
  mc->current_value.clear();
  return 0;
}

static int prom_end_row(void *ctx) {
  auto *mc = static_cast<MetricsCollectorCtx *>(ctx);

  if (mc->current_name.empty() || mc->current_value.empty()) return 0;

  // Try to parse as a number; skip non-numeric values (ON/OFF etc.)
  char *end = nullptr;
  strtod(mc->current_value.c_str(), &end);
  if (end == mc->current_value.c_str() || *end != '\0') return 0;

  // Build the Prometheus metric name: prefix + lowercase(mysql_name)
  std::string prom_name = mc->prefix;
  for (const char *p = mc->current_name.c_str(); *p != '\0'; ++p) {
    prom_name += static_cast<char>(tolower(static_cast<unsigned char>(*p)));
  }
  const char *type_str = mc->type_fn(mc->current_name.c_str());

  *mc->output += "# TYPE ";
  *mc->output += prom_name;
  *mc->output += ' ';
  *mc->output += type_str;
  *mc->output += '\n';
  *mc->output += prom_name;
  *mc->output += ' ';
  *mc->output += mc->current_value;
  *mc->output += '\n';

  return 0;
}

static void prom_abort_row(void *) {}

static ulong prom_get_client_capabilities(void *) { return 0; }

static int prom_get_null(void *) { return 0; }
static int prom_get_integer(void *, longlong) { return 0; }
static int prom_get_longlong(void *, longlong, uint) { return 0; }
static int prom_get_decimal(void *, const decimal_t *) { return 0; }
static int prom_get_double(void *, double, uint32) { return 0; }
static int prom_get_date(void *, const MYSQL_TIME *) { return 0; }
static int prom_get_time(void *, const MYSQL_TIME *, uint) { return 0; }
static int prom_get_datetime(void *, const MYSQL_TIME *, uint) { return 0; }

static int prom_get_string(void *ctx, const char *value, size_t length,
                           const CHARSET_INFO *) {
  auto *mc = static_cast<MetricsCollectorCtx *>(ctx);
  if (mc->col_index == 0) {
    mc->current_name.assign(value, length);
  } else if (mc->col_index == 1) {
    mc->current_value.assign(value, length);
  }
  mc->col_index++;
  return 0;
}

static void prom_handle_ok(void *, uint, uint, ulonglong, ulonglong,
                           const char *) {}

static void prom_handle_error(void *ctx, uint, const char *, const char *) {
  auto *mc = static_cast<MetricsCollectorCtx *>(ctx);
  mc->error = true;
  g_errors_total.fetch_add(1, std::memory_order_relaxed);
}

static void prom_shutdown(void *, int) {}

static const struct st_command_service_cbs prom_cbs = {
    prom_start_result_metadata,
    prom_field_metadata,
    prom_end_result_metadata,
    prom_start_row,
    prom_end_row,
    prom_abort_row,
    prom_get_client_capabilities,
    prom_get_null,
    prom_get_integer,
    prom_get_longlong,
    prom_get_decimal,
    prom_get_double,
    prom_get_date,
    prom_get_time,
    prom_get_datetime,
    prom_get_string,
    prom_handle_ok,
    prom_handle_error,
    prom_shutdown,
    nullptr,  // connection_alive
};

struct InnodbMetricsCtx {
  std::string *output;
  std::string current_name;
  std::string current_type;
  std::string current_count;
  int col_index;
  bool error;
};

static int innodb_start_row(void *ctx) {
  auto *mc = static_cast<InnodbMetricsCtx *>(ctx);
  mc->col_index = 0;
  mc->current_name.clear();
  mc->current_type.clear();
  mc->current_count.clear();
  return 0;
}

static int innodb_end_row(void *ctx) {
  auto *mc = static_cast<InnodbMetricsCtx *>(ctx);

  if (mc->current_name.empty() || mc->current_count.empty()) return 0;

  // Map InnoDB TYPE to Prometheus type: "counter" -> counter, else gauge
  const char *prom_type = (mc->current_type == "counter") ? "counter" : "gauge";

  // Build metric name: mysql_innodb_metrics_ + lowercase(name)
  std::string prom_name = "mysql_innodb_metrics_";
  for (const char *p = mc->current_name.c_str(); *p != '\0'; ++p) {
    prom_name += static_cast<char>(tolower(static_cast<unsigned char>(*p)));
  }

  *mc->output += "# TYPE ";
  *mc->output += prom_name;
  *mc->output += ' ';
  *mc->output += prom_type;
  *mc->output += '\n';
  *mc->output += prom_name;
  *mc->output += ' ';
  *mc->output += mc->current_count;
  *mc->output += '\n';

  return 0;
}

static int innodb_get_string(void *ctx, const char *value, size_t length,
                             const CHARSET_INFO *) {
  auto *mc = static_cast<InnodbMetricsCtx *>(ctx);
  if (mc->col_index == 0) {
    mc->current_name.assign(value, length);
  } else if (mc->col_index == 2) {
    mc->current_type.assign(value, length);
  } else if (mc->col_index == 3) {
    mc->current_count.assign(value, length);
  }
  mc->col_index++;
  return 0;
}

static void innodb_handle_error(void *ctx, uint, const char *, const char *) {
  auto *mc = static_cast<InnodbMetricsCtx *>(ctx);
  mc->error = true;
  g_errors_total.fetch_add(1, std::memory_order_relaxed);
}

static const struct st_command_service_cbs innodb_cbs = {
    prom_start_result_metadata,
    prom_field_metadata,
    prom_end_result_metadata,
    innodb_start_row,
    innodb_end_row,
    prom_abort_row,
    prom_get_client_capabilities,
    prom_get_null,
    prom_get_integer,
    prom_get_longlong,
    prom_get_decimal,
    prom_get_double,
    prom_get_date,
    prom_get_time,
    prom_get_datetime,
    innodb_get_string,
    prom_handle_ok,
    innodb_handle_error,
    prom_shutdown,
    nullptr,  // connection_alive
};

static bool command_failed(int command_fail, bool callback_error) {
  if (!command_fail && !callback_error) return false;

  if (command_fail && !callback_error) {
    g_errors_total.fetch_add(1, std::memory_order_relaxed);
  }
  return true;
}

static bool collect_innodb_metrics(MYSQL_SESSION session, std::string &output) {
  InnodbMetricsCtx mc;
  mc.output = &output;
  mc.col_index = 0;
  mc.error = false;

  COM_DATA cmd;
  memset(&cmd, 0, sizeof(cmd));
  cmd.com_query.query =
      "SELECT NAME, SUBSYSTEM, TYPE, COUNT "
      "FROM information_schema.INNODB_METRICS "
      "WHERE STATUS='enabled'";
  cmd.com_query.length = strlen(cmd.com_query.query);

  const int fail = command_service_run_command(
      session, COM_QUERY, &cmd, &my_charset_utf8mb3_general_ci, &innodb_cbs,
      CS_TEXT_REPRESENTATION, &mc);
  return !command_failed(fail, mc.error);
}

struct ReplicaStatusCtx {
  std::string *output;
  std::vector<std::string> col_names;
  std::vector<bool> col_is_null;
  std::vector<std::string> col_values;
  std::vector<bool> type_emitted;
  int col_index;
  bool has_row;
  bool error;
};

static int replica_start_result_metadata(void *ctx, uint num_cols, uint,
                                         const CHARSET_INFO *) {
  auto *rc = static_cast<ReplicaStatusCtx *>(ctx);
  rc->col_names.clear();
  rc->col_names.reserve(num_cols);
  return 0;
}

static int replica_field_metadata(void *ctx, struct st_send_field *field,
                                  const CHARSET_INFO *) {
  auto *rc = static_cast<ReplicaStatusCtx *>(ctx);
  rc->col_names.push_back(field->col_name);
  return 0;
}

static int replica_start_row(void *ctx) {
  auto *rc = static_cast<ReplicaStatusCtx *>(ctx);
  rc->col_is_null.clear();
  rc->col_is_null.resize(rc->col_names.size(), false);
  rc->col_values.clear();
  rc->col_values.resize(rc->col_names.size());
  rc->col_index = 0;
  rc->has_row = true;
  return 0;
}

static int replica_get_string(void *ctx, const char *value, size_t length,
                              const CHARSET_INFO *) {
  auto *rc = static_cast<ReplicaStatusCtx *>(ctx);
  if (rc->col_index < static_cast<int>(rc->col_values.size())) {
    rc->col_is_null[rc->col_index] = false;
    rc->col_values[rc->col_index].assign(value, length);
  }
  rc->col_index++;
  return 0;
}

static int replica_get_integer(void *ctx, longlong value) {
  auto *rc = static_cast<ReplicaStatusCtx *>(ctx);
  if (rc->col_index < static_cast<int>(rc->col_values.size())) {
    rc->col_is_null[rc->col_index] = false;
    rc->col_values[rc->col_index] = std::to_string(value);
  }
  rc->col_index++;
  return 0;
}

static int replica_get_longlong(void *ctx, longlong value, uint is_unsigned) {
  auto *rc = static_cast<ReplicaStatusCtx *>(ctx);
  if (rc->col_index < static_cast<int>(rc->col_values.size())) {
    rc->col_is_null[rc->col_index] = false;
    rc->col_values[rc->col_index] =
        is_unsigned ? std::to_string(static_cast<ulonglong>(value))
                    : std::to_string(value);
  }
  rc->col_index++;
  return 0;
}

static int replica_get_double(void *ctx, double value, uint32) {
  auto *rc = static_cast<ReplicaStatusCtx *>(ctx);
  if (rc->col_index < static_cast<int>(rc->col_values.size())) {
    rc->col_is_null[rc->col_index] = false;
    rc->col_values[rc->col_index] = std::to_string(value);
  }
  rc->col_index++;
  return 0;
}

static int replica_get_null(void *ctx) {
  auto *rc = static_cast<ReplicaStatusCtx *>(ctx);
  if (rc->col_index < static_cast<int>(rc->col_is_null.size())) {
    rc->col_is_null[rc->col_index] = true;
  }
  rc->col_index++;
  return 0;
}

struct ReplicaWantedField {
  const char *col_name;
  const char *metric_name;
  bool is_bool;
};

static const ReplicaWantedField replica_wanted_fields[] = {
    {"Seconds_Behind_Source", "mysql_replica_seconds_behind_source", false},
    {"Replica_IO_Running", "mysql_replica_io_running", true},
    {"Replica_SQL_Running", "mysql_replica_sql_running", true},
    {"Relay_Log_Space", "mysql_replica_relay_log_space", false},
    {"Exec_Source_Log_Pos", "mysql_replica_exec_source_log_pos", false},
    {"Read_Source_Log_Pos", "mysql_replica_read_source_log_pos", false},
};

static int find_replica_column_index(const ReplicaStatusCtx &ctx,
                                     const char *col_name) {
  for (int i = 0; i < static_cast<int>(ctx.col_names.size()); ++i) {
    if (ctx.col_names[i] == col_name) return i;
  }
  return -1;
}

static void append_prometheus_label_value(std::string &output,
                                          const std::string &value) {
  for (char ch : value) {
    switch (ch) {
      case '\\':
        output += "\\\\";
        break;
      case '"':
        output += "\\\"";
        break;
      case '\n':
        output += "\\n";
        break;
      default:
        output += ch;
        break;
    }
  }
}

static void append_replica_channel_label(std::string &output,
                                         const std::string &channel_name) {
  if (channel_name.empty()) return;

  output += "{channel=\"";
  append_prometheus_label_value(output, channel_name);
  output += "\"}";
}

static int replica_end_row(void *ctx) {
  auto *rc = static_cast<ReplicaStatusCtx *>(ctx);
  const int channel_idx = find_replica_column_index(*rc, "Channel_Name");
  const std::string channel_name =
      (channel_idx >= 0 &&
       channel_idx < static_cast<int>(rc->col_values.size()))
          ? rc->col_values[channel_idx]
          : "";

  for (size_t wanted_idx = 0;
       wanted_idx < array_elements(replica_wanted_fields); ++wanted_idx) {
    const auto &wanted = replica_wanted_fields[wanted_idx];
    const int idx = find_replica_column_index(*rc, wanted.col_name);
    if (idx < 0 || idx >= static_cast<int>(rc->col_values.size())) continue;

    const bool is_null =
        idx < static_cast<int>(rc->col_is_null.size()) && rc->col_is_null[idx];
    const std::string &val = rc->col_values[idx];

    std::string value_str;
    if (wanted.is_bool) {
      if (is_null || val.empty()) continue;
      value_str = (val == "Yes") ? "1" : "0";
    } else {
      if (is_null) {
        if (strcmp(wanted.col_name, "Seconds_Behind_Source") != 0) continue;
        value_str = "NaN";
      } else {
        if (val.empty()) continue;
        // Check if numeric -- require full-string consumption
        const char *start = val.c_str();
        char *end = nullptr;
        strtod(start, &end);
        if (end == start || *end != '\0') continue;  // not numeric, skip
        value_str = val;
      }
    }

    if (!rc->type_emitted[wanted_idx]) {
      *rc->output += "# TYPE ";
      *rc->output += wanted.metric_name;
      *rc->output += " gauge\n";
      rc->type_emitted[wanted_idx] = true;
    }
    *rc->output += wanted.metric_name;
    append_replica_channel_label(*rc->output, channel_name);
    *rc->output += ' ';
    *rc->output += value_str;
    *rc->output += '\n';
  }

  return 0;
}

static void replica_handle_error(void *ctx, uint, const char *, const char *) {
  auto *rc = static_cast<ReplicaStatusCtx *>(ctx);
  rc->error = true;
  g_errors_total.fetch_add(1, std::memory_order_relaxed);
}

static const struct st_command_service_cbs replica_cbs = {
    replica_start_result_metadata,
    replica_field_metadata,
    prom_end_result_metadata,
    replica_start_row,
    replica_end_row,
    prom_abort_row,
    prom_get_client_capabilities,
    replica_get_null,
    replica_get_integer,
    replica_get_longlong,
    prom_get_decimal,
    replica_get_double,
    prom_get_date,
    prom_get_time,
    prom_get_datetime,
    replica_get_string,
    prom_handle_ok,
    replica_handle_error,
    prom_shutdown,
    nullptr,  // connection_alive
};

static bool collect_replica_status(MYSQL_SESSION session, std::string &output) {
  ReplicaStatusCtx rc;
  rc.output = &output;
  rc.type_emitted.assign(array_elements(replica_wanted_fields), false);
  rc.col_index = 0;
  rc.has_row = false;
  rc.error = false;

  COM_DATA cmd;
  memset(&cmd, 0, sizeof(cmd));
  cmd.com_query.query = "SHOW REPLICA STATUS";
  cmd.com_query.length = strlen(cmd.com_query.query);

  const int fail = command_service_run_command(
      session, COM_QUERY, &cmd, &my_charset_utf8mb3_general_ci, &replica_cbs,
      CS_TEXT_REPRESENTATION, &rc);
  return !command_failed(fail, rc.error);
}

struct BinlogCtx {
  std::string *output;
  int col_index;
  int file_count;
  long long total_size;
  std::string current_size;
  bool error;
};

static int binlog_start_row(void *ctx) {
  auto *bc = static_cast<BinlogCtx *>(ctx);
  bc->col_index = 0;
  bc->current_size.clear();
  return 0;
}

static int binlog_end_row(void *ctx) {
  auto *bc = static_cast<BinlogCtx *>(ctx);
  bc->file_count++;
  if (!bc->current_size.empty()) {
    const char *start = bc->current_size.c_str();
    char *end = nullptr;
    long long sz = strtoll(start, &end, 10);
    if (end != start && *end == '\0' && sz >= 0 &&
        bc->total_size <= LLONG_MAX - sz) {
      bc->total_size += sz;
    }
  }
  return 0;
}

static int binlog_get_string(void *ctx, const char *value, size_t length,
                             const CHARSET_INFO *) {
  auto *bc = static_cast<BinlogCtx *>(ctx);
  if (bc->col_index == 1) {
    bc->current_size.assign(value, length);
  }
  bc->col_index++;
  return 0;
}

static void binlog_handle_ok(void *ctx, uint, uint, ulonglong, ulonglong,
                             const char *) {
  auto *bc = static_cast<BinlogCtx *>(ctx);
  if (bc->file_count > 0) {
    *bc->output += "# TYPE mysql_binlog_file_count gauge\n";
    *bc->output += "mysql_binlog_file_count ";
    *bc->output += std::to_string(bc->file_count);
    *bc->output += '\n';

    *bc->output += "# TYPE mysql_binlog_size_bytes_total gauge\n";
    *bc->output += "mysql_binlog_size_bytes_total ";
    *bc->output += std::to_string(bc->total_size);
    *bc->output += '\n';
  }
}

static void binlog_handle_error(void *ctx, uint, const char *, const char *) {
  auto *bc = static_cast<BinlogCtx *>(ctx);
  bc->error = true;
  g_errors_total.fetch_add(1, std::memory_order_relaxed);
}

static const struct st_command_service_cbs binlog_cbs = {
    prom_start_result_metadata,
    prom_field_metadata,
    prom_end_result_metadata,
    binlog_start_row,
    binlog_end_row,
    prom_abort_row,
    prom_get_client_capabilities,
    prom_get_null,
    prom_get_integer,
    prom_get_longlong,
    prom_get_decimal,
    prom_get_double,
    prom_get_date,
    prom_get_time,
    prom_get_datetime,
    binlog_get_string,
    binlog_handle_ok,
    binlog_handle_error,
    prom_shutdown,
    nullptr,  // connection_alive
};

static bool collect_binlog(MYSQL_SESSION session, std::string &output) {
  BinlogCtx bc;
  bc.output = &output;
  bc.col_index = 0;
  bc.file_count = 0;
  bc.total_size = 0;
  bc.error = false;

  COM_DATA cmd;
  memset(&cmd, 0, sizeof(cmd));
  cmd.com_query.query = "SHOW BINARY LOGS";
  cmd.com_query.length = strlen(cmd.com_query.query);

  const int fail = command_service_run_command(
      session, COM_QUERY, &cmd, &my_charset_utf8mb3_general_ci, &binlog_cbs,
      CS_TEXT_REPRESENTATION, &bc);
  return !command_failed(fail, bc.error);
}

static bool collect_name_value_query(MYSQL_SESSION session, std::string &output,
                                     const char *query, const char *prefix,
                                     type_fn_t type_fn) {
  MetricsCollectorCtx mc;
  mc.output = &output;
  mc.prefix = prefix;
  mc.type_fn = type_fn;
  mc.col_index = 0;
  mc.error = false;

  COM_DATA cmd;
  memset(&cmd, 0, sizeof(cmd));
  cmd.com_query.query = query;
  cmd.com_query.length = strlen(query);

  const int fail = command_service_run_command(
      session, COM_QUERY, &cmd, &my_charset_utf8mb3_general_ci, &prom_cbs,
      CS_TEXT_REPRESENTATION, &mc);
  return !command_failed(fail, mc.error);
}

static bool collect_global_status(MYSQL_SESSION session, std::string &output) {
  return collect_name_value_query(session, output, "SHOW GLOBAL STATUS",
                                  "mysql_global_status_", global_status_type);
}

static const char *global_variables_type([[maybe_unused]] const char *name) {
  return "gauge";
}

static bool collect_global_variables(MYSQL_SESSION session,
                                     std::string &output) {
  return collect_name_value_query(session, output, "SHOW GLOBAL VARIABLES",
                                  "mysql_global_variables_",
                                  global_variables_type);
}

struct ScrapeResult {
  int http_status;
  const char *reason_phrase;
  std::string body;
};

static ScrapeResult make_scrape_error(int http_status,
                                      const char *reason_phrase,
                                      const char *body) {
  return {http_status, reason_phrase, body};
}

static ScrapeResult collect_metrics() {
  if (!srv_session_server_is_available()) {
    g_errors_total.fetch_add(1, std::memory_order_relaxed);
    return make_scrape_error(503, "Service Unavailable",
                             "# Server not available\n");
  }

  MYSQL_SESSION session = srv_session_open(nullptr, nullptr);
  if (session == nullptr) {
    g_errors_total.fetch_add(1, std::memory_order_relaxed);
    return make_scrape_error(500, "Internal Server Error",
                             "# Failed to open session\n");
  }

  const char *user = prom_security_user ? prom_security_user : "root";
  MYSQL_SECURITY_CONTEXT sc;
  if (thd_get_security_context(srv_session_info_get_thd(session), &sc) ||
      security_context_lookup(sc, user, "localhost", "127.0.0.1", "")) {
    srv_session_close(session);
    g_errors_total.fetch_add(1, std::memory_order_relaxed);
    return make_scrape_error(
        500, "Internal Server Error",
        "# Failed to set security context (user missing or lacks "
        "privileges?)\n");
  }

  std::string output;
  if (!collect_global_status(session, output)) {
    srv_session_close(session);
    return make_scrape_error(500, "Internal Server Error",
                             "# Failed to collect global status metrics\n");
  }
  if (!collect_global_variables(session, output)) {
    srv_session_close(session);
    return make_scrape_error(500, "Internal Server Error",
                             "# Failed to collect global variable metrics\n");
  }
  if (!collect_innodb_metrics(session, output)) {
    srv_session_close(session);
    return make_scrape_error(500, "Internal Server Error",
                             "# Failed to collect InnoDB metrics\n");
  }
  if (!collect_replica_status(session, output)) {
    srv_session_close(session);
    return make_scrape_error(500, "Internal Server Error",
                             "# Failed to collect replica metrics\n");
  }
  if (!collect_binlog(session, output)) {
  }

  srv_session_close(session);

  return {200, "OK", output};
}

static int setup_listen_socket(const char *bind_addr, unsigned int port) {
  if (bind_addr == nullptr || *bind_addr == '\0') {
    return -1;
  }

  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return -1;

  int reuse = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(port));
  if (inet_pton(AF_INET, bind_addr, &addr.sin_addr) != 1) {
    close(fd);
    return -1;
  }

  if (bind(fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) < 0) {
    close(fd);
    return -1;
  }

  if (listen(fd, 5) < 0) {
    close(fd);
    return -1;
  }

  return fd;
}

static void write_full(int fd, const char *buf, size_t len) {
  size_t written = 0;
  while (written < len) {
    ssize_t n = send(fd, buf + written, len - written, MSG_NOSIGNAL);
    if (n < 0) {
      if (errno == EINTR) continue;
      break;  // EAGAIN (timeout), EPIPE, ECONNRESET, etc. -- give up
    }
    if (n == 0) break;
    written += static_cast<size_t>(n);
  }
}

static ssize_t read_http_request(int fd, char *buf, size_t max_len) {
  size_t total = 0;
  while (total < max_len - 1) {
    ssize_t n = recv(fd, buf + total, max_len - 1 - total, 0);
    if (n < 0) {
      if (errno == EINTR) continue;
      return -1;  // timeout or error
    }
    if (n == 0) break;  // client closed
    total += static_cast<size_t>(n);
    buf[total] = '\0';
    // Check if we have the full request headers
    if (strstr(buf, "\r\n\r\n") != nullptr) break;
    // Or at least the request line for simple requests
    if (strstr(buf, "\r\n") != nullptr && total >= 13) break;
  }
  buf[total] = '\0';
  return static_cast<ssize_t>(total);
}

static void *prometheus_listener_thread(void *arg) {
  auto *ctx = static_cast<PrometheusContext *>(arg);

  // Initialize srv_session thread-local state once for this physical thread.
  // Per the MySQL session service contract, this must be called once per
  // thread that will use the session service, not once per request.
  if (srv_session_init_thread(ctx->plugin_ref) != 0) {
    LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                    "Prometheus exporter: failed to init session thread");
    return nullptr;
  }

  while (!ctx->shutdown_requested.load(std::memory_order_acquire)) {
    struct pollfd pfds[2];
    pfds[0].fd = ctx->listen_fd;
    pfds[0].events = POLLIN;
    pfds[0].revents = 0;
    pfds[1].fd = ctx->wakeup_fd;
    pfds[1].events = POLLIN;
    pfds[1].revents = 0;

    int ret = poll(pfds, 2, -1);  // block until wakeup or new connection
    if (ret < 0) {
      if (errno == EINTR) continue;
      break;  // fatal poll error
    }

    // Check wakeup fd first
    if (pfds[1].revents & POLLIN) break;
    if (!(pfds[0].revents & POLLIN)) continue;

    int client_fd = accept(ctx->listen_fd, nullptr, nullptr);
    if (client_fd < 0) {
      if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK ||
          errno == ECONNABORTED)
        continue;
      break;  // fatal accept error
    }

    // Set receive timeout to avoid blocking indefinitely on slow clients
    struct timeval tv;
    tv.tv_sec = 5;
    tv.tv_usec = 0;
    setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    // Read HTTP request
    char buf[4096];
    ssize_t n = read_http_request(client_fd, buf, sizeof(buf));
    if (n <= 0) {
      close(client_fd);
      continue;
    }

    if (n >= 12 && strncmp(buf, "GET /metrics", 12) == 0 &&
        (buf[12] == ' ' || buf[12] == '?' || buf[12] == '\r' ||
         buf[12] == '\0')) {
      g_requests_total.fetch_add(1, std::memory_order_relaxed);

      auto start = std::chrono::steady_clock::now();
      ScrapeResult scrape = collect_metrics();
      auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - start);
      g_last_scrape_duration_us.store(static_cast<uint64_t>(elapsed.count()),
                                      std::memory_order_relaxed);

      std::string response =
          "HTTP/1.1 " + std::to_string(scrape.http_status) + " " +
          scrape.reason_phrase +
          "\r\n"
          "Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"
          "Content-Length: " +
          std::to_string(scrape.body.size()) +
          "\r\n"
          "Connection: close\r\n"
          "\r\n" +
          scrape.body;

      write_full(client_fd, response.c_str(), response.size());
    } else {
      const char *resp_404 =
          "HTTP/1.1 404 Not Found\r\n"
          "Connection: close\r\n"
          "\r\n";
      write_full(client_fd, resp_404, strlen(resp_404));
    }
    close(client_fd);
  }

  srv_session_deinit_thread();
  return nullptr;
}

static int prometheus_exporter_init(void *p) {
  auto *plugin = static_cast<struct st_plugin_int *>(p);

  if (init_logging_service_for_plugin(&reg_srv, &log_bi, &log_bs)) return 1;

  if (!prom_enabled) {
    LogPluginErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
                 "Prometheus exporter plugin installed but not enabled. "
                 "Set --prometheus-exporter-enabled=ON to activate.");
    plugin->data = nullptr;
    return 0;
  }

  auto *ctx = new (std::nothrow) PrometheusContext();
  if (ctx == nullptr) {
    deinit_logging_service_for_plugin(&reg_srv, &log_bi, &log_bs);
    return 1;
  }
  ctx->plugin_ref = p;

  ctx->listen_fd = setup_listen_socket(prom_bind_address, prom_port);
  if (ctx->listen_fd < 0) {
    LogPluginErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                 "Prometheus exporter: failed to bind to %s:%u",
                 prom_bind_address ? prom_bind_address : "(null)", prom_port);
    delete ctx;
    deinit_logging_service_for_plugin(&reg_srv, &log_bi, &log_bs);
    return 1;
  }

  ctx->wakeup_fd = eventfd(0, EFD_CLOEXEC);
  if (ctx->wakeup_fd < 0) {
    close(ctx->listen_fd);
    delete ctx;
    deinit_logging_service_for_plugin(&reg_srv, &log_bi, &log_bs);
    return 1;
  }

  my_thread_attr_t attr;
  my_thread_attr_init(&attr);
  my_thread_attr_setdetachstate(&attr, MY_THREAD_CREATE_JOINABLE);

  if (my_thread_create(&ctx->listener_thread, &attr, prometheus_listener_thread,
                       ctx) != 0) {
    LogPluginErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                 "Prometheus exporter: failed to create listener thread");
    close(ctx->listen_fd);
    close(ctx->wakeup_fd);
    delete ctx;
    deinit_logging_service_for_plugin(&reg_srv, &log_bi, &log_bs);
    return 1;
  }

  LogPluginErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
               "Prometheus exporter listening on %s:%u", prom_bind_address,
               prom_port);

  if (prom_bind_address != nullptr &&
      strcmp(prom_bind_address, "127.0.0.1") != 0) {
    LogPluginErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG,
                 "Prometheus exporter is bound to %s which is not a "
                 "loopback address. The /metrics endpoint has no "
                 "authentication or TLS -- ensure network access to "
                 "port %u is restricted.",
                 prom_bind_address, prom_port);
  }

  plugin->data = ctx;
  return 0;
}

static int prometheus_exporter_deinit(void *p) {
  auto *plugin = static_cast<struct st_plugin_int *>(p);
  auto *ctx = static_cast<PrometheusContext *>(plugin->data);

  if (ctx != nullptr) {
    ctx->shutdown_requested.store(true, std::memory_order_release);
    if (ctx->wakeup_fd >= 0) {
      uint64_t val = 1;
      ssize_t r = write(ctx->wakeup_fd, &val, sizeof(val));
      (void)r;  // ignore errors; at worst the listener wakes via EINTR
    }

    void *dummy;
    my_thread_join(&ctx->listener_thread, &dummy);

    // Now it's safe to close the fds
    if (ctx->listen_fd >= 0) close(ctx->listen_fd);
    if (ctx->wakeup_fd >= 0) close(ctx->wakeup_fd);

    delete ctx;
    plugin->data = nullptr;
  }

  deinit_logging_service_for_plugin(&reg_srv, &log_bi, &log_bs);
  return 0;
}

static int show_requests_total(MYSQL_THD, SHOW_VAR *var, char *buff) {
  var->type = SHOW_LONGLONG;
  var->value = buff;
  longlong v =
      static_cast<longlong>(g_requests_total.load(std::memory_order_relaxed));
  memcpy(buff, &v, sizeof(v));
  return 0;
}

static int show_errors_total(MYSQL_THD, SHOW_VAR *var, char *buff) {
  var->type = SHOW_LONGLONG;
  var->value = buff;
  longlong v =
      static_cast<longlong>(g_errors_total.load(std::memory_order_relaxed));
  memcpy(buff, &v, sizeof(v));
  return 0;
}

static int show_scrape_duration(MYSQL_THD, SHOW_VAR *var, char *buff) {
  var->type = SHOW_LONGLONG;
  var->value = buff;
  longlong v = static_cast<longlong>(
      g_last_scrape_duration_us.load(std::memory_order_relaxed));
  memcpy(buff, &v, sizeof(v));
  return 0;
}

static SHOW_VAR prom_status_vars[] = {
    {"Prometheus_exporter_requests_total",
     reinterpret_cast<char *>(&show_requests_total), SHOW_FUNC,
     SHOW_SCOPE_GLOBAL},
    {"Prometheus_exporter_errors_total",
     reinterpret_cast<char *>(&show_errors_total), SHOW_FUNC,
     SHOW_SCOPE_GLOBAL},
    {"Prometheus_exporter_scrape_duration_microseconds",
     reinterpret_cast<char *>(&show_scrape_duration), SHOW_FUNC,
     SHOW_SCOPE_GLOBAL},
    {nullptr, nullptr, SHOW_UNDEF, SHOW_SCOPE_UNDEF},
};

static struct st_mysql_daemon prometheus_exporter_descriptor = {
    MYSQL_DAEMON_INTERFACE_VERSION};

mysql_declare_plugin(prometheus_exporter){
    MYSQL_DAEMON_PLUGIN,
    &prometheus_exporter_descriptor,
    "prometheus_exporter",
    "VillageSQL Authors",
    "Embedded Prometheus metrics exporter for MySQL/VillageSQL",
    PLUGIN_LICENSE_GPL,
    prometheus_exporter_init,
    nullptr,
    prometheus_exporter_deinit,
    0x0100,
    prom_status_vars,
    prom_system_vars,
    nullptr,
    0,
} mysql_declare_plugin_end;
