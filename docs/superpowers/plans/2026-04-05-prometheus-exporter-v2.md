# Prometheus Exporter v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand the prometheus_exporter plugin from 1 data source to 5, add 7 MTR tests with format validation, and create full documentation with architecture diagrams.

**Architecture:** The plugin uses MySQL's `srv_session` + `command_service_run_command` API to execute SQL queries internally (no network). Each data source gets its own collector function that takes an open session and appends Prometheus-formatted text to an output string. A single `collect_metrics()` orchestrator opens one session, calls all collectors, then closes the session.

**Tech Stack:** C++20, MySQL daemon plugin API, `srv_session` service, `command_service` callbacks, POSIX sockets, MTR test framework.

**Spec:** `docs/superpowers/specs/2026-04-05-prometheus-exporter-v2-design.md`

**Build commands:**
```bash
# Build just the plugin (fast, ~10s):
cd /data/rene/build && make -j32 prometheus_exporter

# Run all prometheus_exporter tests:
cd /data/rene/build && ./mysql-test/mysql-test-run.pl --suite=prometheus_exporter --nounit-tests --parallel=auto

# Run a specific test:
cd /data/rene/build && ./mysql-test/mysql-test-run.pl --suite=prometheus_exporter --nounit-tests <test_name>

# Record a test result:
cd /data/rene/build && ./mysql-test/mysql-test-run.pl --record --suite=prometheus_exporter --nounit-tests <test_name>
```

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `plugin/prometheus_exporter/prometheus_exporter.cc` | Modify | Add 4 new collectors, refactor existing code into collector pattern, expand gauge list |
| `plugin/prometheus_exporter/README.md` | Create | Full documentation: architecture diagram, config reference, metric namespaces, usage |
| `Docs/prometheus_exporter.md` | Create | Pointer to full docs in plugin dir |
| `mysql-test/suite/prometheus_exporter/t/basic.test` | Modify | Add inline comments |
| `mysql-test/suite/prometheus_exporter/t/metrics_endpoint.test` | Modify | Verify all 5 data source prefixes |
| `mysql-test/suite/prometheus_exporter/t/global_variables.test` | Create | Test SHOW GLOBAL VARIABLES collector |
| `mysql-test/suite/prometheus_exporter/t/global_variables-master.opt` | Create | Server opts for test |
| `mysql-test/suite/prometheus_exporter/r/global_variables.result` | Create | Expected output |
| `mysql-test/suite/prometheus_exporter/t/innodb_metrics.test` | Create | Test INNODB_METRICS collector |
| `mysql-test/suite/prometheus_exporter/t/innodb_metrics-master.opt` | Create | Server opts for test |
| `mysql-test/suite/prometheus_exporter/r/innodb_metrics.result` | Create | Expected output |
| `mysql-test/suite/prometheus_exporter/t/replica_status.test` | Create | Test graceful absence on non-replica |
| `mysql-test/suite/prometheus_exporter/t/replica_status-master.opt` | Create | Server opts for test |
| `mysql-test/suite/prometheus_exporter/r/replica_status.result` | Create | Expected output |
| `mysql-test/suite/prometheus_exporter/t/binlog.test` | Create | Test SHOW BINARY LOGS collector |
| `mysql-test/suite/prometheus_exporter/t/binlog-master.opt` | Create | Server opts for test |
| `mysql-test/suite/prometheus_exporter/r/binlog.result` | Create | Expected output |
| `mysql-test/suite/prometheus_exporter/t/format_validation.test` | Create | Perl-based format validation |
| `mysql-test/suite/prometheus_exporter/t/format_validation-master.opt` | Create | Server opts for test |
| `mysql-test/suite/prometheus_exporter/r/format_validation.result` | Create | Expected output |
| `mysql-test/suite/prometheus_exporter/r/metrics_endpoint.result` | Modify | Updated expected output |

---

## Task 1: Refactor existing code into collector pattern

Refactor `collect_metrics()` so the existing SHOW GLOBAL STATUS logic is extracted into its own `collect_global_status()` function. This doesn't change behavior -- it's a structural refactor to enable adding more collectors cleanly.

**Files:**
- Modify: `plugin/prometheus_exporter/prometheus_exporter.cc`

- [ ] **Step 1: Refactor MetricsCollectorCtx to support configurable prefix and type function**

In `prometheus_exporter.cc`, modify the `MetricsCollectorCtx` struct and `prom_end_row` to accept a prefix and a type-determination function pointer. Replace the current hardcoded `"mysql_global_status_"` prefix.

Change the struct (around line 174):

```cpp
// Function pointer type for determining Prometheus metric type
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
```

Change `prom_end_row` (around line 202) to use the context's prefix and type_fn:

```cpp
static int prom_end_row(void *ctx) {
  auto *mc = static_cast<MetricsCollectorCtx *>(ctx);

  if (mc->current_name.empty() || mc->current_value.empty()) return 0;

  char *end = nullptr;
  strtod(mc->current_value.c_str(), &end);
  if (end == mc->current_value.c_str()) return 0;

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
```

Remove the standalone `to_prometheus_name()` function (line 162-168) since the prefix logic is now in `prom_end_row`.

- [ ] **Step 2: Add type function for global status**

Add the type function that wraps the existing `is_gauge()`:

```cpp
static const char *global_status_type(const char *name) {
  return is_gauge(name) ? "gauge" : "untyped";
}
```

- [ ] **Step 3: Create a helper to run a 2-column query**

This helper opens a query, uses the existing `prom_cbs` callbacks, and returns. It will be reused by both Global Status and Global Variables collectors:

```cpp
static void collect_name_value_query(MYSQL_SESSION session,
                                     std::string &output, const char *query,
                                     const char *prefix, type_fn_t type_fn) {
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

  command_service_run_command(session, COM_QUERY, &cmd,
                             &my_charset_utf8mb3_general_ci, &prom_cbs,
                             CS_TEXT_REPRESENTATION, &mc);
}
```

- [ ] **Step 4: Extract collect_global_status()**

```cpp
static void collect_global_status(MYSQL_SESSION session, std::string &output) {
  collect_name_value_query(session, output, "SHOW GLOBAL STATUS",
                           "mysql_global_status_", global_status_type);
}
```

- [ ] **Step 5: Refactor collect_metrics() to use the new collector**

Replace the body of `collect_metrics()` to separate session management from collection:

```cpp
static std::string collect_metrics() {
  if (!srv_session_server_is_available()) {
    return "# Server not available\n";
  }

  if (srv_session_init_thread(g_ctx->plugin_ref) != 0) {
    return "# Failed to init session thread\n";
  }

  MYSQL_SESSION session = srv_session_open(nullptr, nullptr);
  if (session == nullptr) {
    srv_session_deinit_thread();
    return "# Failed to open session\n";
  }

  MYSQL_SECURITY_CONTEXT sc;
  thd_get_security_context(srv_session_info_get_thd(session), &sc);
  security_context_lookup(sc, "root", "localhost", "127.0.0.1", "");

  std::string output;
  collect_global_status(session, output);

  srv_session_close(session);
  srv_session_deinit_thread();

  return output;
}
```

- [ ] **Step 6: Build and verify no behavior change**

```bash
cd /data/rene/build && make -j32 prometheus_exporter
```

Expected: builds with no errors.

- [ ] **Step 7: Run existing tests**

```bash
cd /data/rene/build && ./mysql-test/mysql-test-run.pl --suite=prometheus_exporter --nounit-tests --parallel=auto
```

Expected: All 3 tests pass (basic, metrics_endpoint, shutdown_report).

- [ ] **Step 8: Commit**

```bash
git add plugin/prometheus_exporter/prometheus_exporter.cc
git commit -m "refactor: extract collector pattern from prometheus_exporter

Refactor collect_metrics() to use a collector function pattern with
configurable prefix and type-determination function. Extracts
collect_global_status() and collect_name_value_query() helper.
No behavior change -- prepares for adding more collectors."
```

---

## Task 2: Add SHOW GLOBAL VARIABLES collector

**Files:**
- Modify: `plugin/prometheus_exporter/prometheus_exporter.cc`
- Create: `mysql-test/suite/prometheus_exporter/t/global_variables.test`
- Create: `mysql-test/suite/prometheus_exporter/t/global_variables-master.opt`
- Create: `mysql-test/suite/prometheus_exporter/r/global_variables.result`

- [ ] **Step 1: Write the test**

Create `mysql-test/suite/prometheus_exporter/t/global_variables-master.opt`:
```
$PROMETHEUS_EXPORTER_PLUGIN_OPT $PROMETHEUS_EXPORTER_PLUGIN_LOAD --prometheus-exporter-enabled=ON --prometheus-exporter-port=19105 --prometheus-exporter-bind-address=127.0.0.1
```

Create `mysql-test/suite/prometheus_exporter/t/global_variables.test`:
```sql
--source include/not_windows.inc

--echo # Verify mysql_global_variables_ metrics are present
--exec curl -s http://127.0.0.1:19105/metrics | grep "^# TYPE mysql_global_variables_max_connections gauge" | head -1

--echo # Verify a buffer pool size variable is exported
--exec curl -s http://127.0.0.1:19105/metrics | grep "^# TYPE mysql_global_variables_innodb_buffer_pool_size gauge" | head -1

--echo # Verify the value is numeric
--replace_regex /[0-9]+/NUM/
--exec curl -s http://127.0.0.1:19105/metrics | grep "^mysql_global_variables_max_connections " | head -1
```

- [ ] **Step 2: Add the type function and collector**

In `prometheus_exporter.cc`, add after `collect_global_status()`:

```cpp
static const char *global_variables_type(const char *) { return "gauge"; }

static void collect_global_variables(MYSQL_SESSION session,
                                     std::string &output) {
  collect_name_value_query(session, output, "SHOW GLOBAL VARIABLES",
                           "mysql_global_variables_", global_variables_type);
}
```

- [ ] **Step 3: Wire into collect_metrics()**

In `collect_metrics()`, add after the `collect_global_status(session, output);` line:

```cpp
  collect_global_variables(session, output);
```

- [ ] **Step 4: Build**

```bash
cd /data/rene/build && make -j32 prometheus_exporter
```

Expected: builds with no errors.

- [ ] **Step 5: Record and run test**

```bash
cd /data/rene/build && ./mysql-test/mysql-test-run.pl --record --suite=prometheus_exporter --nounit-tests global_variables
```

Expected: PASS, result file generated.

- [ ] **Step 6: Run all tests to verify no regressions**

```bash
cd /data/rene/build && ./mysql-test/mysql-test-run.pl --suite=prometheus_exporter --nounit-tests --parallel=auto
```

Expected: All tests pass.

- [ ] **Step 7: Commit**

```bash
git add plugin/prometheus_exporter/prometheus_exporter.cc \
        mysql-test/suite/prometheus_exporter/t/global_variables.test \
        mysql-test/suite/prometheus_exporter/t/global_variables-master.opt \
        mysql-test/suite/prometheus_exporter/r/global_variables.result
git commit -m "feat(prometheus): add SHOW GLOBAL VARIABLES collector

Exports server configuration values (max_connections,
innodb_buffer_pool_size, etc.) as mysql_global_variables_* gauge metrics.
Non-numeric variables are silently skipped."
```

---

## Task 3: Add INNODB_METRICS collector

**Files:**
- Modify: `plugin/prometheus_exporter/prometheus_exporter.cc`
- Create: `mysql-test/suite/prometheus_exporter/t/innodb_metrics.test`
- Create: `mysql-test/suite/prometheus_exporter/t/innodb_metrics-master.opt`
- Create: `mysql-test/suite/prometheus_exporter/r/innodb_metrics.result`

- [ ] **Step 1: Write the test**

Create `mysql-test/suite/prometheus_exporter/t/innodb_metrics-master.opt`:
```
$PROMETHEUS_EXPORTER_PLUGIN_OPT $PROMETHEUS_EXPORTER_PLUGIN_LOAD --prometheus-exporter-enabled=ON --prometheus-exporter-port=19106 --prometheus-exporter-bind-address=127.0.0.1
```

Create `mysql-test/suite/prometheus_exporter/t/innodb_metrics.test`:
```sql
--source include/not_windows.inc

--echo # Verify InnoDB metrics are present
--exec curl -s http://127.0.0.1:19106/metrics | grep "^# TYPE mysql_innodb_metrics_" | head -3

--echo # Verify a known counter type metric
--exec curl -s http://127.0.0.1:19106/metrics | grep "^# TYPE mysql_innodb_metrics_buffer_pool_reads " | head -1

--echo # Verify a known gauge type metric (value type in InnoDB)
--exec curl -s http://127.0.0.1:19106/metrics | grep "^# TYPE mysql_innodb_metrics_buffer_pool_size " | head -1
```

- [ ] **Step 2: Add InnodbMetricsCtx and callbacks**

This collector uses a 4-column result (NAME, SUBSYSTEM, TYPE, COUNT), so it needs its own context and callbacks. Add in `prometheus_exporter.cc`:

```cpp
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

  // Map InnoDB TYPE to Prometheus type
  const char *prom_type = "gauge";
  if (mc->current_type == "counter") {
    prom_type = "counter";
  }
  // value, status_counter, set_owner, set_member -> gauge

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
  switch (mc->col_index) {
    case 0:
      mc->current_name.assign(value, length);
      break;
    case 1:
      // SUBSYSTEM -- skip, not used
      break;
    case 2:
      mc->current_type.assign(value, length);
      break;
    case 3:
      mc->current_count.assign(value, length);
      break;
  }
  mc->col_index++;
  return 0;
}

static void innodb_handle_error(void *ctx, uint, const char *, const char *) {
  static_cast<InnodbMetricsCtx *>(ctx)->error = true;
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
    nullptr,
};
```

- [ ] **Step 3: Add the collector function**

```cpp
static void collect_innodb_metrics(MYSQL_SESSION session,
                                   std::string &output) {
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

  command_service_run_command(session, COM_QUERY, &cmd,
                             &my_charset_utf8mb3_general_ci, &innodb_cbs,
                             CS_TEXT_REPRESENTATION, &mc);
}
```

- [ ] **Step 4: Wire into collect_metrics()**

Add after `collect_global_variables(session, output);`:

```cpp
  collect_innodb_metrics(session, output);
```

- [ ] **Step 5: Build**

```bash
cd /data/rene/build && make -j32 prometheus_exporter
```

Expected: builds with no errors.

- [ ] **Step 6: Record and run test**

```bash
cd /data/rene/build && ./mysql-test/mysql-test-run.pl --record --suite=prometheus_exporter --nounit-tests innodb_metrics
```

Expected: PASS. Note: the exact metric names depend on which InnoDB metrics are enabled by default. The test greps for known ones. If a specific metric name doesn't exist, adjust the grep to match an existing metric from the output.

- [ ] **Step 7: Run all tests**

```bash
cd /data/rene/build && ./mysql-test/mysql-test-run.pl --suite=prometheus_exporter --nounit-tests --parallel=auto
```

Expected: All tests pass.

- [ ] **Step 8: Commit**

```bash
git add plugin/prometheus_exporter/prometheus_exporter.cc \
        mysql-test/suite/prometheus_exporter/t/innodb_metrics.test \
        mysql-test/suite/prometheus_exporter/t/innodb_metrics-master.opt \
        mysql-test/suite/prometheus_exporter/r/innodb_metrics.result
git commit -m "feat(prometheus): add INNODB_METRICS collector

Exports ~200 detailed InnoDB metrics from information_schema.INNODB_METRICS
as mysql_innodb_metrics_* with type mapping from InnoDB's own TYPE column
(counter -> counter, value/status_counter/set_owner/set_member -> gauge)."
```

---

## Task 4: Add SHOW REPLICA STATUS collector

**Files:**
- Modify: `plugin/prometheus_exporter/prometheus_exporter.cc`
- Create: `mysql-test/suite/prometheus_exporter/t/replica_status.test`
- Create: `mysql-test/suite/prometheus_exporter/t/replica_status-master.opt`
- Create: `mysql-test/suite/prometheus_exporter/r/replica_status.result`

- [ ] **Step 1: Write the test**

This test verifies graceful absence -- on a non-replica server, no `mysql_replica_` metrics should appear.

Create `mysql-test/suite/prometheus_exporter/t/replica_status-master.opt`:
```
$PROMETHEUS_EXPORTER_PLUGIN_OPT $PROMETHEUS_EXPORTER_PLUGIN_LOAD --prometheus-exporter-enabled=ON --prometheus-exporter-port=19107 --prometheus-exporter-bind-address=127.0.0.1
```

Create `mysql-test/suite/prometheus_exporter/t/replica_status.test`:
```sql
--source include/not_windows.inc

--echo # On a non-replica server, no mysql_replica_ metrics should appear
--exec curl -s http://127.0.0.1:19107/metrics | grep -c "mysql_replica_" || echo "0"

--echo # But other metrics should still be present
--exec curl -s http://127.0.0.1:19107/metrics | grep "^# TYPE mysql_global_status_uptime" | head -1
```

- [ ] **Step 2: Add ReplicaStatusCtx and callbacks**

This collector needs column-name-aware parsing. During `field_metadata`, build a column name list. During `get_string`, map column index to field name and collect wanted fields.

```cpp
struct ReplicaStatusCtx {
  std::string *output;
  std::vector<std::string> col_names;
  std::vector<std::string> col_values;
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
  rc->col_names.emplace_back(field->col_name);
  return 0;
}

static int replica_start_row(void *ctx) {
  auto *rc = static_cast<ReplicaStatusCtx *>(ctx);
  rc->col_index = 0;
  rc->col_values.clear();
  rc->col_values.resize(rc->col_names.size());
  rc->has_row = true;
  return 0;
}

static int replica_get_string(void *ctx, const char *value, size_t length,
                              const CHARSET_INFO *) {
  auto *rc = static_cast<ReplicaStatusCtx *>(ctx);
  if (rc->col_index < static_cast<int>(rc->col_values.size())) {
    rc->col_values[rc->col_index].assign(value, length);
  }
  rc->col_index++;
  return 0;
}

static int replica_end_row(void *ctx) {
  auto *rc = static_cast<ReplicaStatusCtx *>(ctx);

  struct ReplicaField {
    const char *mysql_name;
    const char *prom_name;
    bool is_bool;  // Yes/No -> 1/0
  };

  static const ReplicaField wanted_fields[] = {
      {"Seconds_Behind_Source", "mysql_replica_seconds_behind_source", false},
      {"Replica_IO_Running", "mysql_replica_io_running", true},
      {"Replica_SQL_Running", "mysql_replica_sql_running", true},
      {"Relay_Log_Space", "mysql_replica_relay_log_space", false},
      {"Exec_Source_Log_Pos", "mysql_replica_exec_source_log_pos", false},
      {"Read_Source_Log_Pos", "mysql_replica_read_source_log_pos", false},
      {nullptr, nullptr, false},
  };

  for (size_t i = 0; i < rc->col_names.size(); i++) {
    for (const ReplicaField *f = wanted_fields; f->mysql_name != nullptr; f++) {
      if (strcasecmp(rc->col_names[i].c_str(), f->mysql_name) != 0) continue;
      if (rc->col_values[i].empty()) continue;

      std::string val;
      if (f->is_bool) {
        val = (rc->col_values[i] == "Yes") ? "1" : "0";
      } else {
        char *end = nullptr;
        strtod(rc->col_values[i].c_str(), &end);
        if (end == rc->col_values[i].c_str()) continue;  // skip non-numeric
        val = rc->col_values[i];
      }

      *rc->output += "# TYPE ";
      *rc->output += f->prom_name;
      *rc->output += " gauge\n";
      *rc->output += f->prom_name;
      *rc->output += ' ';
      *rc->output += val;
      *rc->output += '\n';
    }
  }

  return 0;
}

static void replica_handle_error(void *ctx, uint, const char *, const char *) {
  static_cast<ReplicaStatusCtx *>(ctx)->error = true;
}

static const struct st_command_service_cbs replica_cbs = {
    replica_start_result_metadata,
    replica_field_metadata,
    prom_end_result_metadata,
    replica_start_row,
    replica_end_row,
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
    replica_get_string,
    prom_handle_ok,
    replica_handle_error,
    prom_shutdown,
    nullptr,
};
```

Note: add `#include <vector>` to the includes at the top of the file.

- [ ] **Step 3: Add the collector function**

```cpp
static void collect_replica_status(MYSQL_SESSION session,
                                   std::string &output) {
  ReplicaStatusCtx rc;
  rc.output = &output;
  rc.col_index = 0;
  rc.has_row = false;
  rc.error = false;

  COM_DATA cmd;
  memset(&cmd, 0, sizeof(cmd));
  cmd.com_query.query = "SHOW REPLICA STATUS";
  cmd.com_query.length = strlen(cmd.com_query.query);

  command_service_run_command(session, COM_QUERY, &cmd,
                             &my_charset_utf8mb3_general_ci, &replica_cbs,
                             CS_TEXT_REPRESENTATION, &rc);
}
```

- [ ] **Step 4: Wire into collect_metrics()**

Add after `collect_innodb_metrics(session, output);`:

```cpp
  collect_replica_status(session, output);
```

- [ ] **Step 5: Build**

```bash
cd /data/rene/build && make -j32 prometheus_exporter
```

Expected: builds with no errors.

- [ ] **Step 6: Record and run test**

```bash
cd /data/rene/build && ./mysql-test/mysql-test-run.pl --record --suite=prometheus_exporter --nounit-tests replica_status
```

Expected: PASS. The test verifies that on a non-replica, `mysql_replica_` metrics are absent (grep returns count 0).

- [ ] **Step 7: Run all tests**

```bash
cd /data/rene/build && ./mysql-test/mysql-test-run.pl --suite=prometheus_exporter --nounit-tests --parallel=auto
```

Expected: All tests pass.

- [ ] **Step 8: Commit**

```bash
git add plugin/prometheus_exporter/prometheus_exporter.cc \
        mysql-test/suite/prometheus_exporter/t/replica_status.test \
        mysql-test/suite/prometheus_exporter/t/replica_status-master.opt \
        mysql-test/suite/prometheus_exporter/r/replica_status.result
git commit -m "feat(prometheus): add SHOW REPLICA STATUS collector

Exports replication metrics (seconds_behind_source, io_running,
sql_running, relay_log_space, log positions) as mysql_replica_* gauges.
Gracefully skipped when server is not a replica (no rows returned)."
```

---

## Task 5: Add SHOW BINARY LOGS collector

**Files:**
- Modify: `plugin/prometheus_exporter/prometheus_exporter.cc`
- Create: `mysql-test/suite/prometheus_exporter/t/binlog.test`
- Create: `mysql-test/suite/prometheus_exporter/t/binlog-master.opt`
- Create: `mysql-test/suite/prometheus_exporter/r/binlog.result`

- [ ] **Step 1: Write the test**

Create `mysql-test/suite/prometheus_exporter/t/binlog-master.opt`:
```
$PROMETHEUS_EXPORTER_PLUGIN_OPT $PROMETHEUS_EXPORTER_PLUGIN_LOAD --prometheus-exporter-enabled=ON --prometheus-exporter-port=19108 --prometheus-exporter-bind-address=127.0.0.1
```

Create `mysql-test/suite/prometheus_exporter/t/binlog.test`:
```sql
--source include/not_windows.inc

--echo # Verify binlog metrics are present
--exec curl -s http://127.0.0.1:19108/metrics | grep "^# TYPE mysql_binlog_file_count gauge" | head -1

--echo # Verify binlog size metric
--exec curl -s http://127.0.0.1:19108/metrics | grep "^# TYPE mysql_binlog_size_bytes_total gauge" | head -1

--echo # Verify values are numeric
--replace_regex /[0-9]+/NUM/
--exec curl -s http://127.0.0.1:19108/metrics | grep "^mysql_binlog_file_count " | head -1
```

- [ ] **Step 2: Add BinlogCtx and callbacks**

```cpp
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
    char *end = nullptr;
    long long sz = strtoll(bc->current_size.c_str(), &end, 10);
    if (end != bc->current_size.c_str()) {
      bc->total_size += sz;
    }
  }
  return 0;
}

static int binlog_get_string(void *ctx, const char *value, size_t length,
                             const CHARSET_INFO *) {
  auto *bc = static_cast<BinlogCtx *>(ctx);
  if (bc->col_index == 1) {  // File_size column
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
  static_cast<BinlogCtx *>(ctx)->error = true;
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
    nullptr,
};
```

- [ ] **Step 3: Add the collector function**

```cpp
static void collect_binlog(MYSQL_SESSION session, std::string &output) {
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

  command_service_run_command(session, COM_QUERY, &cmd,
                             &my_charset_utf8mb3_general_ci, &binlog_cbs,
                             CS_TEXT_REPRESENTATION, &bc);
}
```

- [ ] **Step 4: Wire into collect_metrics()**

Add after `collect_replica_status(session, output);`:

```cpp
  collect_binlog(session, output);
```

- [ ] **Step 5: Build**

```bash
cd /data/rene/build && make -j32 prometheus_exporter
```

Expected: builds with no errors.

- [ ] **Step 6: Record and run test**

```bash
cd /data/rene/build && ./mysql-test/mysql-test-run.pl --record --suite=prometheus_exporter --nounit-tests binlog
```

Expected: PASS. Binary logging is on by default in MTR, so binlog metrics should appear.

- [ ] **Step 7: Run all tests**

```bash
cd /data/rene/build && ./mysql-test/mysql-test-run.pl --suite=prometheus_exporter --nounit-tests --parallel=auto
```

Expected: All tests pass.

- [ ] **Step 8: Commit**

```bash
git add plugin/prometheus_exporter/prometheus_exporter.cc \
        mysql-test/suite/prometheus_exporter/t/binlog.test \
        mysql-test/suite/prometheus_exporter/t/binlog-master.opt \
        mysql-test/suite/prometheus_exporter/r/binlog.result
git commit -m "feat(prometheus): add SHOW BINARY LOGS collector

Exports mysql_binlog_file_count and mysql_binlog_size_bytes_total
as gauge metrics. Silently skipped when binary logging is disabled."
```

---

## Task 6: Update existing tests and add format validation

**Files:**
- Modify: `mysql-test/suite/prometheus_exporter/t/basic.test`
- Modify: `mysql-test/suite/prometheus_exporter/t/metrics_endpoint.test`
- Create: `mysql-test/suite/prometheus_exporter/t/format_validation.test`
- Create: `mysql-test/suite/prometheus_exporter/t/format_validation-master.opt`
- Create: `mysql-test/suite/prometheus_exporter/r/format_validation.result`
- Modify/record: `mysql-test/suite/prometheus_exporter/r/basic.result`
- Modify/record: `mysql-test/suite/prometheus_exporter/r/metrics_endpoint.result`

- [ ] **Step 1: Add inline comments to basic.test**

Replace the content of `mysql-test/suite/prometheus_exporter/t/basic.test`:

```sql
# =============================================================================
# basic.test -- Prometheus Exporter Plugin: Install/Uninstall Lifecycle
#
# Verifies:
# - Plugin can be dynamically installed and uninstalled
# - System variables (enabled, port, bind_address) are registered
# - Status variables (requests_total, errors_total, scrape_duration) exist
# - Default values are correct (enabled=OFF, port=9104, bind=0.0.0.0)
# =============================================================================

--source include/not_windows.inc

# Check that plugin is available
disable_query_log;
if (`SELECT @@have_dynamic_loading != 'YES'`) {
  --skip prometheus_exporter plugin requires dynamic loading
}
if (!$PROMETHEUS_EXPORTER_PLUGIN) {
  --skip prometheus_exporter plugin requires the environment variable \$PROMETHEUS_EXPORTER_PLUGIN to be set (normally done by mtr)
}
enable_query_log;

--echo # Install prometheus_exporter plugin
--replace_result $PROMETHEUS_EXPORTER_PLUGIN PROMETHEUS_EXPORTER_PLUGIN
eval INSTALL PLUGIN prometheus_exporter SONAME '$PROMETHEUS_EXPORTER_PLUGIN';

--echo # Verify system variables exist with correct defaults
SHOW VARIABLES LIKE 'prometheus_exporter%';

--echo # Verify status variables exist (all zero when disabled)
SHOW STATUS LIKE 'Prometheus_exporter%';

--echo # Uninstall plugin
UNINSTALL PLUGIN prometheus_exporter;
```

- [ ] **Step 2: Update metrics_endpoint.test to verify all 5 prefixes**

Replace the content of `mysql-test/suite/prometheus_exporter/t/metrics_endpoint.test`:

```sql
# =============================================================================
# metrics_endpoint.test -- Prometheus Exporter: HTTP Endpoint & All Collectors
#
# Verifies:
# - HTTP endpoint serves Prometheus text format
# - All 5 collector prefixes appear in output
# - 404 returned for unknown paths
# - Scrape counter increments
# =============================================================================

--source include/not_windows.inc

# Verify plugin is loaded and enabled
SHOW VARIABLES LIKE 'prometheus_exporter_enabled';

--echo # Verify SHOW GLOBAL STATUS metrics
--exec curl -s http://127.0.0.1:19104/metrics | grep "^# TYPE mysql_global_status_threads_connected" | head -1

--echo # Verify SHOW GLOBAL VARIABLES metrics
--exec curl -s http://127.0.0.1:19104/metrics | grep "^# TYPE mysql_global_variables_max_connections" | head -1

--echo # Verify INNODB_METRICS metrics
--exec curl -s http://127.0.0.1:19104/metrics | grep "^# TYPE mysql_innodb_metrics_" | head -1

--echo # Verify binlog metrics (binary logging is on by default)
--exec curl -s http://127.0.0.1:19104/metrics | grep "^# TYPE mysql_binlog_file_count" | head -1

--echo # Verify metric value line is present and numeric
--replace_regex /[0-9]+/NUM/
--exec curl -s http://127.0.0.1:19104/metrics | grep "^mysql_global_status_threads_connected " | head -1

--echo # Test 404 for unknown paths
--exec curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:19104/notfound

--echo # Verify scrape counter incremented
--replace_regex /[0-9]+/NUM/
SHOW STATUS LIKE 'Prometheus_exporter_requests_total';
```

- [ ] **Step 3: Create format_validation test**

Create `mysql-test/suite/prometheus_exporter/t/format_validation-master.opt`:
```
$PROMETHEUS_EXPORTER_PLUGIN_OPT $PROMETHEUS_EXPORTER_PLUGIN_LOAD --prometheus-exporter-enabled=ON --prometheus-exporter-port=19109 --prometheus-exporter-bind-address=127.0.0.1
```

Create `mysql-test/suite/prometheus_exporter/t/format_validation.test`:
```sql
# =============================================================================
# format_validation.test -- Validates Prometheus exposition format correctness
#
# Uses a perl block to fetch /metrics and validate:
# - Every # TYPE line has a valid type (counter, gauge, untyped)
# - Every # TYPE line is followed by a metric line with matching name
# - Metric names match [a-z_][a-z0-9_]*
# - Values are numeric
# =============================================================================

--source include/not_windows.inc

--echo # Fetching and validating /metrics output format

--perl
use strict;
use warnings;

my $output = `curl -s http://127.0.0.1:19109/metrics`;
my @lines = split /\n/, $output;
my $errors = 0;
my $metrics_count = 0;
my $expect_metric_name = undef;

for (my $i = 0; $i < scalar @lines; $i++) {
  my $line = $lines[$i];

  # Skip empty lines
  next if $line =~ /^\s*$/;

  # Check # TYPE lines
  if ($line =~ /^# TYPE /) {
    if ($line =~ /^# TYPE ([a-z_][a-z0-9_]*) (counter|gauge|untyped)$/) {
      $expect_metric_name = $1;
    } else {
      print "FORMAT ERROR: invalid TYPE line: $line\n";
      $errors++;
      $expect_metric_name = undef;
    }
    next;
  }

  # Skip other comment lines
  next if $line =~ /^#/;

  # Metric value line
  if ($line =~ /^([a-z_][a-z0-9_]*) (.+)$/) {
    my ($name, $value) = ($1, $2);
    $metrics_count++;

    # Check name matches expected from TYPE line
    if (defined $expect_metric_name && $name ne $expect_metric_name) {
      print "FORMAT ERROR: expected metric '$expect_metric_name' but got '$name'\n";
      $errors++;
    }
    $expect_metric_name = undef;

    # Check value is numeric
    unless ($value =~ /^-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$/) {
      print "FORMAT ERROR: non-numeric value '$value' for metric '$name'\n";
      $errors++;
    }
  } else {
    print "FORMAT ERROR: unrecognized line: $line\n";
    $errors++;
  }
}

if ($errors == 0 && $metrics_count > 0) {
  print "OK: $metrics_count metrics validated, 0 format errors\n";
} elsif ($metrics_count == 0) {
  print "ERROR: no metrics found in output\n";
} else {
  print "FAIL: $errors format errors found in $metrics_count metrics\n";
}
EOF
```

- [ ] **Step 4: Record all test results**

```bash
cd /data/rene/build && ./mysql-test/mysql-test-run.pl --record --suite=prometheus_exporter --nounit-tests basic
cd /data/rene/build && ./mysql-test/mysql-test-run.pl --record --suite=prometheus_exporter --nounit-tests metrics_endpoint
cd /data/rene/build && ./mysql-test/mysql-test-run.pl --record --suite=prometheus_exporter --nounit-tests format_validation
```

Expected: All three PASS and result files are generated/updated.

- [ ] **Step 5: Run all tests together**

```bash
cd /data/rene/build && ./mysql-test/mysql-test-run.pl --suite=prometheus_exporter --nounit-tests --parallel=auto
```

Expected: All 8 tests pass (basic, metrics_endpoint, global_variables, innodb_metrics, replica_status, binlog, format_validation, shutdown_report).

- [ ] **Step 6: Commit**

```bash
git add mysql-test/suite/prometheus_exporter/
git commit -m "test(prometheus): expand test suite with format validation

- Add inline documentation to basic.test
- Update metrics_endpoint.test to verify all 5 collector prefixes
- Add format_validation.test: perl-based structural validation of
  Prometheus exposition format (valid TYPE lines, matching metric names,
  numeric values)
Total: 7 tests covering all collectors and output format correctness."
```

---

## Task 7: Create documentation

**Files:**
- Create: `plugin/prometheus_exporter/README.md`
- Create: `Docs/prometheus_exporter.md`

- [ ] **Step 1: Create the full README**

Create `plugin/prometheus_exporter/README.md` with the following content:

```markdown
# Prometheus Exporter Plugin

An embedded Prometheus metrics exporter for VillageSQL/MySQL. Eliminates the
need for an external `mysqld_exporter` sidecar by serving metrics directly
from within the server process.

## Architecture

The plugin is a MySQL daemon plugin that spawns a single background thread
to serve HTTP. When Prometheus scrapes `/metrics`, the plugin opens an
internal `srv_session` (no network connection -- pure in-process function
calls), executes SQL queries against the server, formats the results in
Prometheus text exposition format, and returns them over HTTP.

```text
┌─────────────────────────────────────────────────────┐
│                  VillageSQL Server                   │
│                                                     │
│  ┌──────────────────────────────────────────────┐   │
│  │         prometheus_exporter plugin            │   │
│  │                                              │   │
│  │  ┌──────────────┐    ┌────────────────────┐  │   │
│  │  │ HTTP Listener │    │  collect_metrics()  │  │   │
│  │  │  (poll loop) │───>│                    │  │   │
│  │  │:9104/metrics │    │  srv_session_open()│  │   │
│  │  └──────────────┘    │        │           │  │   │
│  │                      │  ┌─────▼─────────┐ │  │   │
│  │                      │  │ SHOW GLOBAL   │ │  │   │
│  │                      │  │ STATUS        │ │  │   │
│  │                      │  ├───────────────┤ │  │   │
│  │                      │  │ SHOW GLOBAL   │ │  │   │
│  │                      │  │ VARIABLES     │ │  │   │
│  │                      │  ├───────────────┤ │  │   │
│  │                      │  │ INNODB_METRICS│ │  │   │
│  │                      │  ├───────────────┤ │  │   │
│  │                      │  │ SHOW REPLICA  │ │  │   │
│  │                      │  │ STATUS        │ │  │   │
│  │                      │  ├───────────────┤ │  │   │
│  │                      │  │ SHOW BINARY   │ │  │   │
│  │                      │  │ LOGS          │ │  │   │
│  │                      │  └───────────────┘ │  │   │
│  │                      └────────────────────┘  │   │
│  └──────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
         ▲
         │ HTTP GET /metrics (every 15-60s)
         │
    ┌────┴─────┐
    │Prometheus│
    │  Server  │
    └──────────┘
```

Key design choice: the plugin executes standard SQL queries via the
`srv_session` service API rather than accessing internal server structs
directly. This makes it resilient to MySQL version changes during rebases.

## Configuration

The plugin is disabled by default. All variables are read-only (require
server restart to change).

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `prometheus_exporter_enabled` | BOOL | OFF | Enable the HTTP metrics endpoint |
| `prometheus_exporter_port` | UINT | 9104 | TCP port to listen on (1024-65535) |
| `prometheus_exporter_bind_address` | STRING | 0.0.0.0 | IP address to bind to |

## Usage

### Load at server startup (recommended)

```ini
# my.cnf
[mysqld]
plugin-load=prometheus_exporter=prometheus_exporter.so
prometheus-exporter-enabled=ON
prometheus-exporter-port=9104
prometheus-exporter-bind-address=127.0.0.1
```

### Load at runtime

```sql
INSTALL PLUGIN prometheus_exporter SONAME 'prometheus_exporter.so';
-- Note: enabled defaults to OFF, so the HTTP server won't start
-- unless --prometheus-exporter-enabled=ON was set at startup
```

### Scrape

```bash
curl http://127.0.0.1:9104/metrics
```

### Prometheus configuration

```yaml
scrape_configs:
  - job_name: 'villagesql'
    static_configs:
      - targets: ['your-db-host:9104']
```

## Metric Namespaces

| Prefix | Source | Type Logic | Metrics |
|--------|--------|-----------|---------|
| `mysql_global_status_` | `SHOW GLOBAL STATUS` | Known gauge list; rest `untyped` | ~400 server status counters |
| `mysql_global_variables_` | `SHOW GLOBAL VARIABLES` | All `gauge` | Numeric config values (max_connections, buffer sizes, etc.) |
| `mysql_innodb_metrics_` | `information_schema.INNODB_METRICS` | InnoDB TYPE column: `counter`->counter, others->gauge | ~200 detailed InnoDB internals |
| `mysql_replica_` | `SHOW REPLICA STATUS` | Per-field mapping, all `gauge` | Replication lag, IO/SQL thread state, log positions |
| `mysql_binlog_` | `SHOW BINARY LOGS` | All `gauge` | File count and total size |

## Metric Type Classification

**Global Status**: A static list of known gauge variables (Threads_connected,
Open_tables, Uptime, buffer pool pages, etc.) are typed as `gauge`. All
others are typed as `untyped` since without additional context it's
ambiguous whether they're monotonic counters or point-in-time values.

**Global Variables**: All typed as `gauge` -- configuration values are
point-in-time snapshots.

**InnoDB Metrics**: Uses the `TYPE` column from `INNODB_METRICS`:
- `counter` -> Prometheus `counter`
- `value`, `status_counter`, `set_owner`, `set_member` -> Prometheus `gauge`

**Replica Status**: All fields typed as `gauge`. Boolean fields
(Replica_IO_Running, Replica_SQL_Running) are converted to 1/0.

**Binary Logs**: Both metrics (file_count, size_bytes_total) are `gauge`.

## Plugin Status Variables

The plugin exposes its own operational metrics via `SHOW GLOBAL STATUS`:

| Variable | Description |
|----------|-------------|
| `Prometheus_exporter_requests_total` | Total number of /metrics scrapes served |
| `Prometheus_exporter_errors_total` | Total number of errors during scrapes |
| `Prometheus_exporter_scrape_duration_microseconds` | Duration of the last scrape in microseconds |

## Limitations

- **No TLS**: The HTTP endpoint is plain HTTP. Use `bind_address=127.0.0.1`
  and a reverse proxy if TLS is needed.
- **No authentication**: Rely on bind address restriction and network-level
  controls. Prometheus typically scrapes over a private network.
- **Single-threaded**: One scrape at a time. Concurrent requests queue on
  the TCP backlog. This is fine for Prometheus's 15-60s scrape interval.
- **Linux only**: Uses POSIX sockets (socket/bind/listen/accept/poll).
  Windows support would require Winsock adaptation.
- **Read-only variables**: Port and bind address require a server restart
  to change.
```

- [ ] **Step 2: Create the Docs pointer**

Create `Docs/prometheus_exporter.md`:

```markdown
# Prometheus Exporter Plugin

Embedded Prometheus metrics exporter for VillageSQL.

For full documentation, architecture, and configuration reference, see
[plugin/prometheus_exporter/README.md](../plugin/prometheus_exporter/README.md).
```

- [ ] **Step 3: Commit**

```bash
git add plugin/prometheus_exporter/README.md Docs/prometheus_exporter.md
git commit -m "docs(prometheus): add architecture docs and configuration reference

- Full README with ASCII architecture diagram, configuration table,
  metric namespace reference, usage examples, type classification,
  and limitations
- Pointer doc in Docs/ directory"
```

---

## Self-Review Checklist

**Spec coverage:**
- [x] SHOW GLOBAL STATUS (existing, refactored in Task 1)
- [x] SHOW GLOBAL VARIABLES (Task 2)
- [x] INNODB_METRICS (Task 3)
- [x] SHOW REPLICA STATUS (Task 4)
- [x] SHOW BINARY LOGS (Task 5)
- [x] Test expansion to 7 tests (Task 6)
- [x] Format validation test (Task 6)
- [x] Documentation with architecture diagram (Task 7)
- [x] Docs/ pointer (Task 7)
- [x] Port allocation for parallel safety (each .opt file uses different port)

**Placeholder scan:** No TBD/TODO/placeholder text found.

**Type consistency:**
- `MetricsCollectorCtx` used consistently across Tasks 1-2
- `InnodbMetricsCtx` defined and used only in Task 3
- `ReplicaStatusCtx` defined and used only in Task 4
- `BinlogCtx` defined and used only in Task 5
- `collect_*` function signatures consistent: `(MYSQL_SESSION, std::string &)`
- All callback structs follow same pattern: reuse no-op callbacks, specialize only what differs
