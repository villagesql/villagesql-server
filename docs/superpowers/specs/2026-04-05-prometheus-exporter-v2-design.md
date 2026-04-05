# Prometheus Exporter Plugin v2 -- Design Spec

## Summary

Expand the embedded Prometheus exporter plugin from a single data source (`SHOW GLOBAL STATUS`) to five data sources, add comprehensive test coverage with format validation, and create full documentation with architecture diagrams.

## Current State (v1)

- Single-file daemon plugin at `plugin/prometheus_exporter/prometheus_exporter.cc` (~573 lines)
- Exports `SHOW GLOBAL STATUS` variables as `mysql_global_status_*` metrics
- Bare-bones HTTP server on configurable port (default 9104), disabled by default
- Uses `srv_session` + `command_service_run_command` to execute SQL internally (no network, no auth -- pure in-process function calls)
- 2 MTR tests: `basic` (install/uninstall) and `metrics_endpoint` (HTTP format validation)
- System variables: `enabled`, `port`, `bind_address` (all READONLY)
- Plugin status variables: `requests_total`, `errors_total`, `scrape_duration_microseconds`

## Design Decisions

### SQL queries over direct internal access

The plugin executes SQL queries via the `srv_session` service rather than accessing internal server structs directly. Rationale: VillageSQL rebases onto new MySQL versions; internal struct layouts change across versions, but SQL interfaces (`SHOW GLOBAL STATUS`, `information_schema` tables) are stable. The per-scrape overhead of SQL execution (~milliseconds) is negligible since Prometheus scrapes every 15-60 seconds.

### Single file

The plugin stays in a single `.cc` file. With the v2 additions it will be ~800-900 lines. This is manageable for a self-contained plugin and avoids header management overhead. Can be split later if it grows further.

### Session reuse within a scrape

All collectors run on the same `srv_session` -- opened once per scrape, closed after all collectors finish. This avoids per-query session creation overhead.

### Graceful failure per collector

Each collector silently skips on error. For example, `SHOW REPLICA STATUS` returns no rows on a non-replica server; `SHOW BINARY LOGS` fails if binary logging is disabled. The scrape still succeeds with whatever metrics were collected.

## Expanded Metrics: 5 Data Sources

### 1. SHOW GLOBAL STATUS (existing)

- **Query**: `SHOW GLOBAL STATUS`
- **Prefix**: `mysql_global_status_`
- **Type logic**: Known gauge list (Threads_connected, Open_tables, buffer pool pages, Uptime, etc.); everything else `untyped`
- **Callback**: 2-column (Variable_name, Value), skip non-numeric values

### 2. SHOW GLOBAL VARIABLES (new)

- **Query**: `SHOW GLOBAL VARIABLES`
- **Prefix**: `mysql_global_variables_`
- **Type logic**: All `gauge` (configuration values are point-in-time snapshots)
- **Callback**: Same 2-column pattern as Global Status, skip non-numeric values (strings like `datadir` are discarded)
- **Key metrics exposed**: `max_connections`, `innodb_buffer_pool_size`, `innodb_log_file_size`, `table_open_cache`, `thread_cache_size`, etc.

### 3. information_schema.INNODB_METRICS (new)

- **Query**: `SELECT NAME, SUBSYSTEM, TYPE, COUNT FROM information_schema.INNODB_METRICS WHERE STATUS='enabled'`
- **Prefix**: `mysql_innodb_metrics_`
- **Type logic**: Uses InnoDB's own `TYPE` column:
  - `counter` -> Prometheus `counter`
  - `value`, `status_counter`, `set_owner`, `set_member` -> Prometheus `gauge`
- **Callback**: 4-column (NAME, SUBSYSTEM, TYPE, COUNT). Subsystem is not emitted as a label (to avoid cardinality); it's implicit in metric names.
- **Key metrics exposed**: ~200 detailed InnoDB internals covering buffer pool, transactions, locks, redo log, purge, DML operations, adaptive hash index, etc. This covers the quantitative data from `SHOW ENGINE INNODB STATUS` without text parsing.

### 4. SHOW REPLICA STATUS (new)

- **Query**: `SHOW REPLICA STATUS`
- **Prefix**: `mysql_replica_`
- **Type logic**: Per-field mapping
- **Callback**: Column-name-aware. During `field_metadata`, capture column names into a vector. During `get_string`, match against wanted fields.
- **Fields exported**:

| MySQL Column | Prometheus Metric | Type |
|---|---|---|
| `Seconds_Behind_Source` | `mysql_replica_seconds_behind_source` | gauge |
| `Replica_IO_Running` | `mysql_replica_io_running` | gauge (1=Yes, 0=No) |
| `Replica_SQL_Running` | `mysql_replica_sql_running` | gauge (1=Yes, 0=No) |
| `Relay_Log_Space` | `mysql_replica_relay_log_space` | gauge |
| `Exec_Source_Log_Pos` | `mysql_replica_exec_source_log_pos` | gauge |
| `Read_Source_Log_Pos` | `mysql_replica_read_source_log_pos` | gauge |

- If the query returns no rows (server is not a replica), nothing is emitted.

### 5. SHOW BINARY LOGS (new)

- **Query**: `SHOW BINARY LOGS`
- **Prefix**: `mysql_binlog_`
- **Type logic**: All `gauge`
- **Callback**: Accumulates across all result rows. Counts rows and sums `File_size` column.
- **Metrics exported**:
  - `mysql_binlog_file_count` -- number of binary log files
  - `mysql_binlog_size_bytes_total` -- total size of all binary log files
- If binary logging is disabled, query fails silently -- no metrics emitted.

## Code Organization

All changes within `plugin/prometheus_exporter/prometheus_exporter.cc`. Internal structure:

```
1. Includes, logging refs, system vars, context struct        (existing)
2. Prometheus formatting helpers + gauge classification        (existing, expanded)
3. Command service callbacks (reusable)                        (existing, refactored)
4. Collector: collect_global_status()                          (refactored from existing)
5. Collector: collect_global_variables()                       (new)
6. Collector: collect_innodb_metrics()                         (new)
7. Collector: collect_replica_status()                         (new)
8. Collector: collect_binlog()                                 (new)
9. collect_metrics() orchestrator                              (refactored)
10. HTTP server                                                (existing)
11. Plugin init/deinit, status vars, declaration               (existing)
```

### Collector function signature

```cpp
static void collect_<source>(MYSQL_SESSION session, std::string &output);
```

Each collector takes the already-open session, runs its query, appends Prometheus-formatted lines to `output`. Returns silently on any error.

### Callback reuse

- **Global Status, Global Variables**: Reuse existing `MetricsCollectorCtx` and `prom_cbs` callbacks with configurable prefix and type-determination function passed via context.
- **InnoDB Metrics**: New context struct for 4-column results (NAME, SUBSYSTEM, TYPE, COUNT).
- **Replica Status**: New context struct with column-name-to-index mapping built during `field_metadata`.
- **Binary Logs**: New context struct that accumulates file count and total size.

## Tests

### Test inventory (7 tests)

| Test | Purpose | .opt file needed |
|------|---------|:---:|
| `basic` | Install/uninstall, system vars, status vars. Add inline comments. | No |
| `metrics_endpoint` | HTTP endpoint, expanded to verify all 5 data source prefixes appear | Yes |
| `global_variables` | Verify `mysql_global_variables_max_connections` appears with type `gauge` | Yes |
| `innodb_metrics` | Verify `mysql_innodb_metrics_` lines appear with correct counter/gauge types | Yes |
| `replica_status` | Verify graceful absence: no `mysql_replica_` metrics on non-replica server | Yes |
| `binlog` | Verify `mysql_binlog_file_count` and `mysql_binlog_size_bytes_total` appear | Yes |
| `format_validation` | Perl block validates entire `/metrics` output structure (see below) | Yes |

### Format validation test

A perl block fetches the full `/metrics` output via curl and validates:
- Every `# TYPE <name> <type>` line has `<type>` in {counter, gauge, untyped}
- Every `# TYPE` line is immediately followed by a metric line starting with the same `<name>`
- Every metric value line has format `<name> <numeric_value>`
- Metric names match `[a-z_][a-z0-9_]*`
- No blank values, no trailing whitespace on metric lines

### Port allocation for parallel test safety

Each test with a `.opt` file uses a different port to avoid conflicts when MTR runs tests in parallel:
- `metrics_endpoint`: 19104
- `global_variables`: 19105
- `innodb_metrics`: 19106
- `replica_status`: 19107
- `binlog`: 19108
- `format_validation`: 19109

## Documentation

### `plugin/prometheus_exporter/README.md` (full docs)

1. **Overview** -- what the plugin is, philosophy (embedded, no sidecar)
2. **Architecture diagram** -- ASCII art:

```
┌─────────────────────────────────────────────────────┐
│                  VillageSQL Server                   │
│                                                     │
│  ┌──────────────────────────────────────────────┐   │
│  │         prometheus_exporter plugin            │   │
│  │                                              │   │
│  │  ┌──────────────┐    ┌────────────────────┐  │   │
│  │  │ HTTP Listener │    │  collect_metrics()  │  │   │
│  │  │ (poll loop)  │───>│                    │  │   │
│  │  │ :9104/metrics│    │  srv_session_open()│  │   │
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
         │ HTTP GET /metrics
         │
    ┌────┴─────┐
    │Prometheus│
    │  Server  │
    └──────────┘
```

3. **Configuration** -- table of system variables (enabled, port, bind_address) with defaults and descriptions
4. **Metric namespaces** -- table of 5 prefixes, their source queries, and type classification logic
5. **Usage** -- `INSTALL PLUGIN` vs `--plugin-load`, example curl output snippet
6. **Metric type classification** -- how gauge/counter/untyped is determined per source
7. **Plugin status variables** -- the 3 self-monitoring metrics
8. **Limitations** -- no TLS, no auth (rely on bind_address), single-threaded scrape handling, Linux-only (POSIX sockets)

### `Docs/prometheus_exporter.md` (pointer)

Brief file pointing to the full docs:

```markdown
# Prometheus Exporter Plugin

Embedded Prometheus metrics exporter for VillageSQL.

For full documentation, architecture, and configuration reference, see
[plugin/prometheus_exporter/README.md](../plugin/prometheus_exporter/README.md).
```

## Files Modified/Created

| File | Action |
|------|--------|
| `plugin/prometheus_exporter/prometheus_exporter.cc` | Modified -- add 4 collectors, refactor collect_metrics(), expand gauge list |
| `plugin/prometheus_exporter/README.md` | Created -- full documentation with architecture diagram |
| `Docs/prometheus_exporter.md` | Created -- pointer to plugin docs |
| `mysql-test/suite/prometheus_exporter/t/basic.test` | Modified -- add inline comments |
| `mysql-test/suite/prometheus_exporter/t/metrics_endpoint.test` | Modified -- verify all 5 prefixes |
| `mysql-test/suite/prometheus_exporter/t/global_variables.test` | Created |
| `mysql-test/suite/prometheus_exporter/t/innodb_metrics.test` | Created |
| `mysql-test/suite/prometheus_exporter/t/replica_status.test` | Created |
| `mysql-test/suite/prometheus_exporter/t/binlog.test` | Created |
| `mysql-test/suite/prometheus_exporter/t/format_validation.test` | Created |
| `mysql-test/suite/prometheus_exporter/r/*.result` | Created/updated for all tests |
| `mysql-test/suite/prometheus_exporter/t/*-master.opt` | Created for new tests |

No changes to any files outside `plugin/prometheus_exporter/`, `Docs/`, and `mysql-test/suite/prometheus_exporter/`.
