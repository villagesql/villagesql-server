# Prometheus Exporter Plugin

Embedded Prometheus metrics exporter for VillageSQL.

## Platform Support

The plugin is built and supported on Linux only. The current implementation
uses Linux-specific networking/thread wakeup primitives, so non-Linux builds do
not compile the module.

## Quick Start

### 1. Verify the plugin can be loaded

```sql
INSTALL PLUGIN prometheus_exporter SONAME 'prometheus_exporter.so';
SELECT PLUGIN_NAME, PLUGIN_STATUS
FROM INFORMATION_SCHEMA.PLUGINS
WHERE PLUGIN_NAME = 'prometheus_exporter';
UNINSTALL PLUGIN prometheus_exporter;
```

This confirms the shared object is present and loadable, but it does not start
the HTTP endpoint by itself.

### 2. Enable the HTTP endpoint at server startup

```ini
[mysqld]
plugin-load=prometheus_exporter=prometheus_exporter.so
prometheus-exporter-enabled=ON
prometheus-exporter-port=9104
prometheus-exporter-bind-address=127.0.0.1
```

Use a numeric IPv4 bind address such as `127.0.0.1`. The current listener does
not accept hostnames like `localhost`.

Restart the server, then verify:

```bash
curl http://127.0.0.1:9104/metrics
```

## Replica Metrics

Replica metrics are emitted from `SHOW REPLICA STATUS`.

- Single-channel replication exposes one sample per metric family.
- Multi-channel replication exposes one sample per named channel using a
  `channel="..."` label.
- The default unnamed channel remains unlabeled.

Example:

```text
# TYPE mysql_replica_io_running gauge
mysql_replica_io_running{channel="channel_1"} 1
mysql_replica_io_running{channel="channel_3"} 1
```

## Reference

For full configuration, architecture, and collector details, see
[plugin/prometheus_exporter/README.md](../plugin/prometheus_exporter/README.md).
