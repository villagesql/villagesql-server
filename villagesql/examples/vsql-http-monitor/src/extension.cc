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

// vsql-http-monitor: minimal HTTP monitoring endpoint example.
//
// Demonstrates:
//   - .thread(...).poll_fd(&g_listen_fd) for instant wakeup on new connections
//   - sys_var for configuring port, bind address, and enabled toggle
//   - Socket lifecycle managed in on_enabled_change() via .on_change()
//   - Simple HTTP response (placeholder body; extend in on_serve())
//
// Usage:
//   INSTALL EXTENSION vsql_http_monitor SONAME 'vsql_http_monitor.veb';
//   SET GLOBAL vsql_http_monitor_port = 9200;
//   SET GLOBAL vsql_http_monitor_bind_address = '0.0.0.0';
//   SET GLOBAL vsql_http_monitor_enabled = ON;
//
// Then: curl http://127.0.0.1:9200/

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cstring>
#include <string>

#include <villagesql/vsql.h>

using namespace vsql;

// ---- Configuration sys vars ------------------------------------------------

static bool g_enabled = false;
static long long g_port = 9200;
static char *g_bind_address = nullptr;

// ---- Listener state --------------------------------------------------------

// The listening socket fd. -1 when the listener is not running.
// Exposed to the framework via .poll_fd(&g_listen_fd) so the worker thread
// wakes immediately when a new connection arrives rather than waiting for its
// full sleep interval.
static int g_listen_fd = -1;

static std::atomic<long long> g_requests_total{0};

// ---- Socket helpers --------------------------------------------------------

static int setup_listen_socket(const char *bind_addr, int port) {
  if (bind_addr == nullptr || *bind_addr == '\0') return -1;

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
      break;
    }
    if (n == 0) break;
    written += static_cast<size_t>(n);
  }
}

// Read until we see the end of the HTTP request headers (\r\n\r\n).
static ssize_t read_http_request(int fd, char *buf, size_t max_len) {
  size_t total = 0;
  while (total < max_len - 1) {
    ssize_t n = recv(fd, buf + total, max_len - 1 - total, 0);
    if (n < 0) {
      if (errno == EINTR) continue;
      return -1;
    }
    if (n == 0) break;
    total += static_cast<size_t>(n);
    buf[total] = '\0';
    if (strstr(buf, "\r\n\r\n") != nullptr) break;
  }
  buf[total] = '\0';
  return static_cast<ssize_t>(total);
}

// Build the HTTP response body. Extend this function to serve real content
// (e.g. metrics, status JSON) once run_query is available.
static std::string on_serve() {
  return "requests_total " + std::to_string(g_requests_total.load()) + "\n";
}

// Handle one accepted client connection.
static void handle_client(int client_fd) {
  // Short timeout so a slow/broken client does not block the worker thread.
  struct timeval tv;
  tv.tv_sec = 5;
  tv.tv_usec = 0;
  setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

  char req_buf[4096];
  read_http_request(client_fd, req_buf, sizeof(req_buf));

  std::string body = on_serve();
  std::string response =
      "HTTP/1.1 200 OK\r\n"
      "Content-Type: text/plain\r\n"
      "Content-Length: " +
      std::to_string(body.size()) +
      "\r\n"
      "Connection: close\r\n"
      "\r\n" +
      body;

  write_full(client_fd, response.c_str(), response.size());
  close(client_fd);

  g_requests_total.fetch_add(1, std::memory_order_relaxed);
}

// ---- Sys var change callbacks ----------------------------------------------

// Called when vsql_http_monitor_enabled changes. Starts or stops the listener
// socket so the worker thread wakes immediately on new connections.
static void on_enabled_change(SysVarChange change) {
  if (change.as_int().value() != 0 && g_listen_fd < 0) {
    const char *bind_addr =
        (g_bind_address != nullptr && *g_bind_address != '\0') ? g_bind_address
                                                               : "127.0.0.1";
    g_listen_fd = setup_listen_socket(bind_addr, static_cast<int>(g_port));
  } else if (!change.as_int().value() != 0 && g_listen_fd >= 0) {
    close(g_listen_fd);
    g_listen_fd = -1;
  }
}

// ---- Worker thread ---------------------------------------------------------

// Called on each wakeup tick (either a new connection arrived on g_listen_fd,
// the periodic timer fired, or shutdown was signalled — in the last case the
// thread loop exits without calling this function again).
static void serve_tick() {
  if (g_listen_fd < 0) return;

  // Non-blocking accept: poll_fd already told us a connection is ready, but
  // accept may return EAGAIN on a spurious wakeup.
  struct sockaddr_in client_addr;
  socklen_t addr_len = sizeof(client_addr);
  int client_fd =
      accept(g_listen_fd, reinterpret_cast<struct sockaddr *>(&client_addr),
             &addr_len);
  if (client_fd < 0) return;

  handle_client(client_fd);
}

// ---- Lifecycle callbacks ---------------------------------------------------

static void on_unload() {
  int fd = g_listen_fd;
  if (fd >= 0) {
    g_listen_fd = -1;
    close(fd);
  }
}

// ---- VDF: request count ----------------------------------------------------

void requests_total(IntResult out) {
  out.set(g_requests_total.load(std::memory_order_relaxed));
}

// ---- Extension registration ------------------------------------------------

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .sys_var(make_sys_var_bool("enabled",
                                   "Enable the HTTP monitoring endpoint",
                                   &g_enabled, false)
                     .on_change<&on_enabled_change>())
        .sys_var(make_sys_var_int("port", "TCP port for the HTTP endpoint",
                                  &g_port, 9200, 1024, 65535))
        .sys_var(make_sys_var_str("bind_address",
                                  "IP address to bind the HTTP endpoint",
                                  &g_bind_address, "127.0.0.1"))
        .status_var(make_status_var_int("requests_total", &g_requests_total))
        .on_unload<&on_unload>()
        .func(make_func<&requests_total>("requests_total").returns(INT).build())
        .thread(make_thread<&serve_tick>("http").periodic(1000).poll_fd(
            &g_listen_fd)))
