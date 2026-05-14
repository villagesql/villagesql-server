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

// vsql-thread-worker-test: exercises dynamic wakeup control.
//
// A minimal TCP listener on an OS-assigned port.  On each accepted connection
// the worker reads a one-line command and stores it in last_command.
//
// Tests verify:
//   1. ENABLE opens the socket (port 0 → OS picks); active_port() returns it.
//   2. POLL_FD fires; worker reads command and stores it in last_command.
//   3. last_command() VDF returns the received string.
//   4. DISABLE closes the socket cleanly; active_port() returns 0.
//
// SQL queries are issued from the worker thread rather than from a VDF because
// the sql_query capability requires a vef_thread_handle_t, which is only
// available inside the worker callback.  While it is technically possible to
// call SQL from a VDF, doing so creates a nested statement context (a statement
// executing inside another statement), which is not recommended.  The worker
// thread is the intended place for extension-initiated SQL.
//
// Commands accepted over TCP:
//   SQL_FOR_EACH <sql>   Run query via for_each; store first column of first
//   row
//                        in last_monitor_result.
//   SQL_EXECUTE <sql>    Same, using execute+next instead of for_each.
//   (any other string)   Stored in last_command only; no query executed.
//
// VDFs provided:
//   active_port()         -> INT     Port currently being listened on (0 if
//   closed). last_command()        -> STRING  Last command string received
//   (empty if none). last_monitor_result() -> STRING  Result of last
//   SQL_FOR_EACH or SQL_EXECUTE
//                                    command (empty if none).

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cstring>
#include <string_view>

#include <villagesql/preview/sql_query.h>
#include <villagesql/preview/thread_worker.h>
#include <villagesql/vsql.h>

using namespace vsql;

struct WorkerState {
  std::atomic<int> active_port{0};
  int listen_fd{-1};
  char last_command[256]{};
  char last_monitor_result[256]{};
};

static WorkerState g_state;

static vsql::preview_sql_query::SqlQueryCapability g_sql_query_cap;

static int open_socket() {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return -1;

  int reuse = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

  struct sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = 0;  // let OS pick
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

  if (bind(fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) < 0 ||
      listen(fd, 5) < 0) {
    close(fd);
    return -1;
  }

  // Read back the port the OS assigned.
  struct sockaddr_in bound{};
  socklen_t len = sizeof(bound);
  if (getsockname(fd, reinterpret_cast<struct sockaddr *>(&bound), &len) == 0)
    g_state.active_port.store(ntohs(bound.sin_port));

  return fd;
}

static const char *get_cmd(const char *buf, const char *prefix) {
  size_t plen = std::strlen(prefix);
  if (std::strncmp(buf, prefix, plen) != 0) return nullptr;
  return buf + plen;
}

static void update_last_monitor_result(std::string_view val) {
  auto len = val.size() < sizeof(g_state.last_monitor_result) - 1
                 ? val.size()
                 : sizeof(g_state.last_monitor_result) - 1;
  std::memcpy(g_state.last_monitor_result, val.data(), len);
  g_state.last_monitor_result[len] = '\0';
}

static void run_monitor_sql_for_each(struct vef_thread_handle_t *handle,
                                     const char *sql) {
  g_state.last_monitor_result[0] = '\0';
  auto session = g_sql_query_cap.open(handle);
  if (!session) return;
  bool got_row = false;
  session.sql(sql).for_each(
      [&got_row](const vsql::preview_sql_query::Result &r) {
        if (got_row) return;
        got_row = true;
        update_last_monitor_result(r.column_str(0));
      });
}

static void run_monitor_sql_execute(struct vef_thread_handle_t *handle,
                                    const char *sql) {
  g_state.last_monitor_result[0] = '\0';
  auto session = g_sql_query_cap.open(handle);
  if (!session) return;
  auto result = session.sql(sql).execute();
  if (!result.next()) return;
  update_last_monitor_result(result.column_str(0));
}

static vef_next_wakeup_t worker(vef_wakeup_reason_t reason,
                                struct vef_thread_handle_t *handle, void *) {
  if (reason == VEF_WAKEUP_ENABLE) {
    g_state.last_command[0] = '\0';
    g_state.listen_fd = open_socket();
    return {0, g_state.listen_fd};
  }

  if (reason == VEF_WAKEUP_DISABLE) {
    if (g_state.listen_fd >= 0) {
      close(g_state.listen_fd);
      g_state.listen_fd = -1;
    }
    g_state.active_port.store(0);
    return {};
  }

  // VEF_WAKEUP_POLL_FD: accept, read command, close.
  if (g_state.listen_fd >= 0) {
    int client = accept(g_state.listen_fd, nullptr, nullptr);
    if (client >= 0) {
      char rbuf[256]{};
      ssize_t n = recv(client, rbuf, sizeof(rbuf) - 1, 0);
      if (n > 0) {
        while (n > 0 && (rbuf[n - 1] == '\n' || rbuf[n - 1] == '\r')) --n;
        rbuf[n] = '\0';
        std::strncpy(g_state.last_command, rbuf,
                     sizeof(g_state.last_command) - 1);

        const char *sql;
        if ((sql = get_cmd(rbuf, "SQL_FOR_EACH ")) != nullptr)
          run_monitor_sql_for_each(handle, sql);
        else if ((sql = get_cmd(rbuf, "SQL_EXECUTE ")) != nullptr)
          run_monitor_sql_execute(handle, sql);
      }
      close(client);
    }
  }
  return {};
}

static vsql::preview_thread_worker::ThreadWorkerCapability<&worker>
    g_worker_cap{"listener"};

static void active_port_vdf(IntResult out) {
  out.set(g_state.active_port.load(std::memory_order_relaxed));
}

static void last_command_vdf(StringResult out) {
  out.set(g_state.last_command);
}

static void last_monitor_result_vdf(StringResult out) {
  out.set(g_state.last_monitor_result);
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .func(make_func<&active_port_vdf>("active_port").returns(INT).build())
        .func(make_func<&last_command_vdf>("last_command")
                  .returns(STRING)
                  .build())
        .func(make_func<&last_monitor_result_vdf>("last_monitor_result")
                  .returns(STRING)
                  .build())
        .with(g_sql_query_cap)
        .with(g_worker_cap))
