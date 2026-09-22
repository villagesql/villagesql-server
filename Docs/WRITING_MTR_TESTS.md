# Writing VillageSQL MTR Tests

Conventions for tests under `mysql-test/suite/villagesql/`. See AGENTS.md for
how to run the suites and read their logs; this document covers how to write
the tests themselves. Add a section whenever a convention is worth stating once
instead of rediscovering per PR.

## 1. Shared includes under `include/villagesql/`

Recurring setup, teardown, and inspection steps have a canonical implementation
in `mysql-test/include/villagesql/`. Source those rather than copying the SQL
into a new test, so that a change in behavior is a one-file edit.

| Include | Purpose |
| --- | --- |
| `install_complex_extension.inc` / `uninstall_complex_extension.inc` | Install or remove the `vsql_complex` test extension |
| `install_tvector_extension.inc` / `uninstall_tvector_extension.inc` | Same for `vsql_tvector` |
| `create_extension.inc`, `create_extension_sdk.inc` | Build a `.veb` from source within a test |
| `internal_schema_test.inc` | Debug-only; enables `skip_dd_table_access_check` so a test can read internal schema tables |
| `query_custom_columns.inc`, `query_custom_indexes.inc`, `query_extensions.inc`, `query_hidden_tables.inc` | Dump victionary / extension state in a stable form |
| `binlog_check_begin.inc` / `binlog_check_end.inc` | Assert what a statement wrote to the binlog |
| `decode_failure_log_init.inc` / `decode_failure_log_print.inc` | Show only the error-log lines a decode failure produced |

Several accept parameters — `install_complex_extension.inc` takes
`$rpl_server_number` for replication tests, for example. Read the header
comment before use.

## 2. Marking a test that cannot pass yet

Write the test that pins the behavior you want, then skip it with a shared
marker. Do not delete the test, leave it failing, or comment out its body — the
value is that the intended behavior is recorded in executable form.

| Include | Use when |
| --- | --- |
| `not_implemented.inc` | A VillageSQL feature or fix the test needs does not exist yet |
| `not_implemented_complex_type.inc` | Specifically a missing custom-type capability |
| `mysql_failure_skip.inc` | The test fails in mainline MySQL too, not because of VillageSQL |
| `too_old_upgrade.inc` | The test depends on upgrading from pre-8.x, which MySQL dropped |

The pattern is a comment saying what the test pins and why it cannot pass,
followed by the marker near the top of the file:

```
# UNINSTALL EXTENSION must be blocked when a CHECK constraint references one
# of the extension's VDFs, even after the table cache is flushed.
#
# Today the dependency check only finds VDFs via custom_columns ...

--source include/villagesql/not_implemented.inc

--source include/villagesql/install_complex_extension.inc
...
```

The test never runs past the marker, so its `.result` file is committed empty.

### Why the shared marker, and not your own `--skip`

Each include carries a fixed reason string. MTR writes that string verbatim
into `mysql-test-report.xml`, which CI uploads as an artifact:

```xml
<testcase name="uninstall_blocked_by_check_constraint_on_vdf" status="skipped"
          comment="VillageSQL feature not yet implemented - to be enabled later" ... />
```

That reason string is the grouping key. Because it is identical across every
test in a category, the category is countable, and those counts are tracked on
dashboards. A bespoke `--skip "some message"` creates a bucket nobody is
watching, and the test quietly stops being work anyone is tracking.

These counts are meant to go to zero. Burning one down means deleting the
`--source` line, running the test, and recording its real `.result` — not
changing the skip reason. If a test needs to stay skipped for a new reason,
add an include for that reason rather than inlining a message.

## 3. Stopping and restarting the server

Never write a bare `--shutdown_server`. With no argument it uses mysqltest's
default timeout of **60 seconds**, which is not enough for a debug build under
Valgrind on a loaded CI runner, where a clean InnoDB shutdown can take minutes.

Use the upstream framework includes, which live in `include/` rather than
`include/villagesql/`. `shutdown_mysqld.inc` and `restart_mysqld.inc` wrap the
same steps but multiply the timeout by 6 when `$VALGRIND_TEST` is set:

| Include | Use for |
| --- | --- |
| `include/shutdown_mysqld.inc` | Stop the server and leave it stopped |
| `include/restart_mysqld.inc` | Stop and start again in one step |
| `include/start_mysqld.inc` | Start a server stopped by one of the above |
| `include/kill_mysqld.inc` | Deliberately kill the server (`--shutdown_server 0`, no timeout involved) |

```
# Wrong - 60s default, flaky under Valgrind.
--exec echo "wait" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--shutdown_server
--source include/wait_until_disconnected.inc

# Right - identical steps, 360s under Valgrind.
--source include/shutdown_mysqld.inc
```

A larger timeout is a ceiling, not a delay: `do_shutdown_server()` polls once a
second and returns as soon as the server is gone, so it costs nothing on a
normal run. Set `$shutdown_server_timeout` before sourcing if a test needs a
different base value; the Valgrind multiplier applies on top of it.

`shutdown_mysqld.inc` emits no output, so adopting it does not change `.result`
files. `kill_mysqld.inc` echoes `# Kill the server` and `start_mysqld.inc`
echoes the restart parameters, so those do.

Let the includes own the expect file. `include/shutdown_mysqld.inc`,
`include/kill_mysqld.inc`, and `include/expect_crash.inc` set
`$_expect_file_name`, and `include/start_mysqld.inc` requires it. That is preferable to writing
`$MYSQLTEST_VARDIR/tmp/mysqld.1.expect` by hand, which hardcodes a server id:

```
--source include/shutdown_mysqld.inc
--let $restart_parameters = restart:--datadir=$DDIR
--source include/start_mysqld.inc
```

### Recognizing a shutdown timeout in CI

The symptom does not name the test that caused it, so the signature is worth
knowing.

When `--shutdown_server` times out, mysqltest sends the server `SIGABRT` on
purpose to leave a core behind (`abort_process()` in `client/mysqltest.cc`).
Under Valgrind the aborted server never frees anything, so Valgrind dumps a
full leak report into `var/<worker>/log/mysqld.N.err`. MTR's synthetic
`valgrind_report` test fails on any report with a nonzero `possibly lost`,
`still reachable`, or `ERROR SUMMARY` (`valgrind_exit_reports()` in
`mysql-test/mysql-test-run.pl`).

The test itself is usually retried and passes, so it is reported as unstable
rather than failing, and `valgrind_report` is the only name in the failure
list:

```
Failing test(s): valgrind_report
Unstable test(s)(failures/attempts): villagesql/startup.upgrade_code_base_mismatch(1/3)
```

To confirm, look in the worker's error log for a shutdown that was cut off:

```
mysqltest: At line NN: Command "shutdown_server" failed with error 2.
...
mysqld got signal 6 ;
```

A `SIGABRT` arriving exactly 60 seconds after `Received SHUTDOWN from user
root` is this problem, not a product bug. Fix the test to use the includes
above rather than suppressing the Valgrind report.
