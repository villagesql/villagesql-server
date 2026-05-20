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
// the worker reads a one-line command and returns a one-line-per-field
// response.
//
// Tests verify:
//   1. ENABLE opens the socket (port 0 → OS picks); active_port() returns it.
//   2. POLL_FD fires; worker reads command and writes a response.
//   3. DISABLE closes the socket cleanly; active_port() returns 0.
//
// SQL queries are issued from the worker thread rather than from a VDF because
// the sql_query capability requires a vef_thread_handle_t, which is only
// available inside the worker callback.  While it is technically possible to
// call SQL from a VDF, doing so creates a nested statement context (a statement
// executing inside another statement), which is not recommended.  The worker
// thread is the intended place for extension-initiated SQL.
//
// Commands accepted over TCP:
//   SQL_FOR_EACH <sql>   Run query via for_each; return first column of first
//                        row as result. On error, return "<errno>:<message>"
//                        as error. Any warnings are joined as
//                        "<errno>:<message>; ..." (empty if none).
//   SQL_EXECUTE <sql>    Same, using execute+next instead of for_each.
//   (any other string)   No query executed; empty result/warning/error.
//
// TCP responses are:
//   RESULT: <value>
//   WARNING: <value>
//   ERROR: <value>
//
// VDFs provided:
//   active_port()           -> INT     Port currently being listened on
//                                      (0 if closed).

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstring>
#include <string>
#include <string_view>

#include <villagesql/preview/sql_query.h>
#include <villagesql/preview/thread_worker.h>
#include <villagesql/vsql.h>

using namespace vsql;

struct WorkerState {
  std::atomic<int> active_port{0};
  int listen_fd{-1};
};

struct ReturnCtx {
  std::string sql_res;
  std::string sql_warn;
  std::string sql_err;
};

static WorkerState g_state;

// Populated during extension registration before the worker can run. The worker
// callback calls open() with its vef_thread_handle_t; each call creates an
// independent session.
static vsql::preview_sql_query::SqlQueryCapability SQL_QUERY;

static constexpr int kSendFlags =
#ifdef MSG_NOSIGNAL
    MSG_NOSIGNAL;
#else
    0;
#endif

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

static void append_value(std::string_view value, std::string &out) {
  if (!value.empty()) out.append(value.data(), value.size());
}

static void set_value(std::string_view value, std::string &out) {
  out.clear();
  append_value(value, out);
}

// If the result has an error, write "<errno>:<message>" into the response
// context and return true. Errors take precedence over any rows that may have
// streamed before the failure.
static bool capture_error(const vsql::preview_sql_query::Result &r,
                          ReturnCtx &return_ctx) {
  if (!r.has_error()) return false;
  auto e = r.error();
  return_ctx.sql_err = std::to_string(e.errno_);
  return_ctx.sql_err += ":";
  append_value(e.message, return_ctx.sql_err);
  return true;
}

// Join all warnings on the result into a "<errno>:<message>; ..." string.
static void capture_warnings(const vsql::preview_sql_query::Result &r,
                             ReturnCtx &return_ctx) {
  return_ctx.sql_warn.clear();
  unsigned int n = r.warning_count();
  for (unsigned int i = 0; i < n; ++i) {
    auto w = r.warning(i);
    if (!return_ctx.sql_warn.empty()) return_ctx.sql_warn += "; ";
    return_ctx.sql_warn += std::to_string(w.errno_);
    return_ctx.sql_warn += ":";
    append_value(w.message, return_ctx.sql_warn);
  }
}

static void run_monitor_sql_for_each(struct vef_thread_handle_t *handle,
                                     const char *sql, ReturnCtx &return_ctx) {
  auto session = SQL_QUERY.open(handle);
  if (!session) {
    return_ctx.sql_err = "Session open failed";
    return;
  }
  bool got_row = false;
  // This test extension returns one scalar value in its TCP response, so use
  // the first column from the first row.
  auto status =
      session.sql(sql).for_each([&got_row, &return_ctx](const auto &row) {
        if (got_row) return;
        got_row = true;
        set_value(row.column_str(0), return_ctx.sql_res);
      });
  if (capture_error(status, return_ctx)) return_ctx.sql_res.clear();
  capture_warnings(status, return_ctx);
}

static void run_monitor_sql_execute(struct vef_thread_handle_t *handle,
                                    const char *sql, ReturnCtx &return_ctx) {
  auto session = SQL_QUERY.open(handle);
  if (!session) {
    return_ctx.sql_err = "Session open failed";
    return;
  }
  auto result = session.sql(sql).execute();
  if (capture_error(result, return_ctx)) {
    return_ctx.sql_res.clear();
  } else {
    if (result.next()) set_value(result.column_str(0), return_ctx.sql_res);
  }
  capture_warnings(result, return_ctx);
}

static void write_response(int fd, const ReturnCtx &return_ctx) {
  const std::string response = "RESULT: " + return_ctx.sql_res +
                               "\nWARNING: " + return_ctx.sql_warn +
                               "\nERROR: " + return_ctx.sql_err + "\n";
  const char *buf = response.data();
  size_t len = response.size();

  size_t written = 0;
  while (written < len) {
    ssize_t n = send(fd, buf + written, len - written, kSendFlags);
    if (n < 0) {
      if (errno == EINTR) continue;
      break;
    }
    if (n == 0) break;
    written += static_cast<size_t>(n);
  }
}

static vef_next_wakeup_t worker(vef_wakeup_reason_t reason,
                                struct vef_thread_handle_t *handle, void *) {
  if (reason == VEF_WAKEUP_ENABLE) {
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

  if (reason != VEF_WAKEUP_POLL_FD) return {};

  // VEF_WAKEUP_POLL_FD: accept, read command, close.
  if (g_state.listen_fd >= 0) {
    int client = accept(g_state.listen_fd, nullptr, nullptr);
    if (client >= 0) {
      char rbuf[256]{};
      ssize_t n = recv(client, rbuf, sizeof(rbuf) - 1, 0);
      if (n > 0) {
        while (n > 0 && (rbuf[n - 1] == '\n' || rbuf[n - 1] == '\r')) --n;
        rbuf[n] = '\0';

        const char *sql;
        ReturnCtx return_ctx;

        if ((sql = get_cmd(rbuf, "SQL_FOR_EACH ")) != nullptr)
          run_monitor_sql_for_each(handle, sql, return_ctx);
        else if ((sql = get_cmd(rbuf, "SQL_EXECUTE ")) != nullptr)
          run_monitor_sql_execute(handle, sql, return_ctx);

        write_response(client, return_ctx);
      }
      close(client);
    }
  }
  return {};
}

static vsql::preview_thread_worker::ThreadWorkerCapability<&worker>
    THREAD_WORKER{"listener"};

static void active_port_vdf(IntResult out) {
  out.set(g_state.active_port.load(std::memory_order_relaxed));
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .func(make_func<&active_port_vdf>("active_port").returns(INT).build())
        .with(SQL_QUERY)
        .with(THREAD_WORKER))
