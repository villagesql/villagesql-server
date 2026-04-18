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

```
┌─────────────────────────────────────────────────────┐
│                  VillageSQL Server                   │
│                                                     │
│  ┌──────────────────────────────────────────────┐   │
│  │         prometheus_exporter plugin            │   │
│  │                                               │   │
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
server restart to change). Future versions may support dynamic changes to
port and bind address without restart if there is sufficient interest.

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `prometheus_exporter_enabled` | BOOL | OFF | Enable the HTTP metrics endpoint |
| `prometheus_exporter_port` | UINT | 9104 | TCP port to listen on (1024-65535) |
| `prometheus_exporter_bind_address` | STRING | 127.0.0.1 | Numeric IPv4 address to bind to |
| `prometheus_exporter_security_user` | STRING | root | Internal user name used for collector sessions |

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

Use a numeric IPv4 address for `prometheus_exporter_bind_address`. Hostnames
such as `localhost` are not accepted by the current listener implementation.

### Load at runtime

```sql
INSTALL PLUGIN prometheus_exporter SONAME 'prometheus_exporter.so';
```

Runtime installation is useful as a loadability smoke test. The HTTP endpoint
still requires the read-only startup variables to be set when the server boots,
so the recommended production path is to load the plugin from `my.cnf`.

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
(Replica_IO_Running, Replica_SQL_Running) are converted to 1/0. When
`SHOW REPLICA STATUS` returns multiple rows for named channels, samples are
labeled with `channel="..."` so multi-source replication remains representable.
The default unnamed channel remains unlabeled. `Seconds_Behind_Source=NULL`
is exported as `NaN`.

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
- **Linux only**: The plugin build is gated to Linux. The current
  implementation uses Linux-specific APIs such as `eventfd()` and
  `MSG_NOSIGNAL`.
- **Bind address format**: The current listener accepts numeric IPv4
  addresses only. Use `127.0.0.1`, not `localhost`.
- **Read-only variables**: Port and bind address require a server restart
  to change (may become dynamic in future versions if demand warrants)

## Verification Notes

The Linux verification for this branch covers:

- runtime `INSTALL PLUGIN` / `UNINSTALL PLUGIN`
- endpoint scrapes from a single server
- multi-channel replication with one replica connected to two sources
- Prometheus format validation with labeled replica metrics
