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

#include "villagesql/services/preview/auth.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include "my_sys.h"
#include "mysql/plugin_auth_common.h"
#include "mysqld_error.h"
#include "sql/auth/sql_authentication.h"
#include "sql/hostname_cache.h"
#include "strmake.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/systable/helpers.h"
#include "villagesql/sdk/include/villagesql/abi/preview/auth.h"

// The VEF auth ctx is just the connection's MPVIO_EXT. The extension handler
// only ever sees vef_auth_ctx_t (opaque) + the ops table, so it never touches
// MySQL internals directly.
struct vef_auth_ctx_t {
  MPVIO_EXT *mpvio;
};

namespace villagesql::services {

namespace {

struct RegisteredMethod {
  std::string method_name;     // normalized (for uniqueness + lookup)
  std::string extension_name;  // for the "already registered by" message
  const vef_auth_cc_t *cc;
};

// The auth-method registry: a small set (names are globally unique across
// extensions, so a handful of entries) mutated only on INSTALL/UNINSTALL
// EXTENSION and read once per login. A plain vector under g_mu suffices; the
// lookup copies out the cc pointer under the lock and releases it before the
// handler runs (which does client I/O and must not hold a global lock).
std::mutex g_mu;
std::vector<RegisteredMethod> g_methods;

// In-flight drain counter. AuthMethodRef bumps it (seq_cst) for the duration of
// an authenticate() call; on_depopulate removes the method from g_methods then
// spins until this reaches zero, so no extension code runs after on_depopulate
// returns and the .so becomes safe to dlclose.
std::atomic<size_t> g_inflight{0};

vef_preview_auth_t g_auth_vtable{VEF_PREVIEW_AUTH_ABI_VERSION};

// --- MySQL-side handshake adapter (the ops table over MPVIO_EXT) ---
// Read/write go through the VIO function pointers already installed on the
// MPVIO_EXT (server_mpvio_read_packet / server_mpvio_write_packet), so this
// needs no access to those file-static functions.
int64_t vef_auth_read_packet(vef_auth_ctx_t *ctx, const unsigned char **data) {
  unsigned char *buf = nullptr;
  const int n = ctx->mpvio->read_packet(ctx->mpvio, &buf);
  if (data != nullptr) *data = buf;
  return n;
}

bool vef_auth_write_packet(vef_auth_ctx_t *ctx, const unsigned char *data,
                           uint64_t len) {
  // mpvio->write_packet returns 0 on success, non-zero on failure.
  return ctx->mpvio->write_packet(ctx->mpvio, data, static_cast<int>(len)) != 0;
}

const char *vef_auth_user_name(vef_auth_ctx_t *ctx) {
  const char *u = ctx->mpvio->auth_info.user_name;
  return u != nullptr ? u : "";
}

const char *vef_auth_auth_string(vef_auth_ctx_t *ctx) {
  const char *s = ctx->mpvio->auth_info.auth_string;
  return s != nullptr ? s : "";
}

const char *vef_auth_host_or_ip(vef_auth_ctx_t *ctx) {
  const char *h = ctx->mpvio->auth_info.host_or_ip;
  return h != nullptr ? h : "";
}

void vef_auth_set_authenticated_as(vef_auth_ctx_t *ctx, const char *account) {
  if (account == nullptr) return;
  strmake(ctx->mpvio->auth_info.authenticated_as, account,
          sizeof(ctx->mpvio->auth_info.authenticated_as) - 1);
}

void vef_auth_set_external_user(vef_auth_ctx_t *ctx, const char *identity) {
  if (identity == nullptr) return;
  strmake(ctx->mpvio->auth_info.external_user, identity,
          sizeof(ctx->mpvio->auth_info.external_user) - 1);
}

const vef_auth_ops_t g_vef_auth_ops = {
    VEF_PREVIEW_AUTH_ABI_VERSION,  vef_auth_read_packet,
    vef_auth_write_packet,         vef_auth_user_name,
    vef_auth_auth_string,          vef_auth_host_or_ip,
    vef_auth_set_authenticated_as, vef_auth_set_external_user};

}  // namespace

vef_preview_auth_t *preview_auth_vtable() { return &g_auth_vtable; }

AuthMethodRef::AuthMethodRef() {
  g_inflight.fetch_add(1, std::memory_order_seq_cst);
}
AuthMethodRef::~AuthMethodRef() {
  g_inflight.fetch_sub(1, std::memory_order_release);
}

bool on_populate_auth(const PopulateContext &ctx, std::string &error_message) {
  if (ctx.capability_config == nullptr) return false;
  const auto *cc = static_cast<const vef_auth_cc_t *>(ctx.capability_config);

  // Validate the method config. (Moved verbatim from the former validate.cc
  // auth branch -- INSTALL-time misconfiguration fails here.)
  if (cc->name == nullptr || cc->name[0] == '\0') {
    error_message = "auth capability has no method name";
    return true;
  }
  if (strlen(cc->name) > VEF_AUTH_MAX_NAME_LEN) {
    error_message = std::string("auth method '") + cc->name +
                    "': name exceeds the maximum length";
    return true;
  }
  if (cc->handler == nullptr) {
    error_message = std::string("auth method '") + cc->name +
                    "': handler function pointer is not set";
    return true;
  }
  // A VEF auth method must pin a client-side auth plugin: the handshake's
  // change-plugin request needs a name to send the client, and a VEF method has
  // no MySQL plugin to fall back on. Require it here so the misconfiguration
  // fails at INSTALL rather than sending an empty plugin name at login.
  if (cc->client_auth_plugin == nullptr || cc->client_auth_plugin[0] == '\0') {
    error_message = std::string("auth method '") + cc->name +
                    "': client_auth_plugin is not set";
    return true;
  }

  const std::string normalized = normalize_extension_name(cc->name);
  const std::string ext(ctx.extension_name);

  std::lock_guard<std::mutex> lock(g_mu);
  // Auth-method names must be unique across ALL extensions: an account binds to
  // a bare name (IDENTIFIED WITH <name>) and login resolves it by name, so a
  // collision would be ambiguous. Compare on the normalized name so a case-only
  // difference is caught.
  for (const auto &m : g_methods) {
    if (m.method_name == normalized) {
      error_message = std::string("auth method '") + cc->name +
                      "' already registered by extension '" + m.extension_name +
                      "'";
      return true;
    }
  }

  g_methods.push_back({normalized, ext, cc});

  LogVSQL(INFORMATION_LEVEL, "Registered auth method '%s' from extension '%s'",
          cc->name, ext.c_str());
  return false;
}

void on_depopulate_auth(const DepopulateContext &ctx) {
  if (ctx.capability_config == nullptr) return;
  const auto *cc = static_cast<const vef_auth_cc_t *>(ctx.capability_config);

  {
    std::lock_guard<std::mutex> lock(g_mu);
    g_methods.erase(
        std::remove_if(g_methods.begin(), g_methods.end(),
                       [&](const RegisteredMethod &m) { return m.cc == cc; }),
        g_methods.end());
  }

  // Drain: wait for any in-flight authenticate() using this (or any) method to
  // finish before returning, so the extension .so is safe to dlclose.
  while (g_inflight.load(std::memory_order_acquire) > 0)
    std::this_thread::yield();
}

const vef_auth_cc_t *find_auth_method(std::string_view method_name) {
  if (method_name.empty()) return nullptr;
  const std::string normalized =
      normalize_extension_name(std::string(method_name));
  std::lock_guard<std::mutex> lock(g_mu);
  for (const auto &m : g_methods) {
    if (m.method_name == normalized) return m.cc;
  }
  return nullptr;
}

bool auth_method_exists(std::string_view method_name) {
  return find_auth_method(method_name) != nullptr;
}

std::optional<bool> handle_vef_user_bind(std::string_view method_name,
                                         bool uses_identified_by_clause) {
  if (!auth_method_exists(method_name)) return std::nullopt;
  if (uses_identified_by_clause) {
    // IDENTIFIED BY '...' asks the (non-existent) plugin to hash a password;
    // meaningless for a VEF method. Reject rather than silently ignore.
    my_error(ER_PASSWORD_FORMAT, MYF(0));
    return true;
  }
  return false;
}

// Drive an already-resolved VEF auth method's handler for this login. The
// caller passes a `cc` obtained from find_auth_method() while holding an
// AuthMethodRef, so the config/.so stays alive for the whole call. Returns true
// on failure (no handler, or the handler declined) -- fail closed: only an
// explicit VEF_AUTH_OK succeeds. Internal to this file.
static bool try_vef_authenticate(const vef_auth_cc_t *cc, MPVIO_EXT *mpvio) {
  // A VEF method has no MySQL plugin.
  mpvio->plugin = nullptr;

  // on_populate_auth rejects a null handler at INSTALL, so a registered method
  // always has one. Assert the invariant (and fail closed in release).
  if (should_assert_if_null(cc->handler)) return true;

  // Stash the method's pinned client-plugin name on the connection before
  // driving the handler: the handler's first read_packet triggers the handshake
  // change-plugin request that reads it back via mpvio_client_plugin_name().
  mpvio->vef_client_auth_plugin = cc->client_auth_plugin;

  // Run the extension's authenticator over an ops table on this MPVIO_EXT.
  vef_auth_ctx_t ctx{mpvio};
  return cc->handler(&ctx, &g_vef_auth_ops) != VEF_AUTH_OK;
}

int vsql_do_auth_once(THD *thd [[maybe_unused]],
                      const MYSQL_LEX_CSTRING &auth_plugin_name,
                      MPVIO_EXT *mpvio) {
  int res = CR_OK;
  const int old_status = mpvio->status;

  // Hold an in-flight reference across BOTH the lookup and the handler call, so
  // a racing UNINSTALL EXTENSION drains behind us rather than freeing the
  // config / dlclose'ing the .so mid-login.
  AuthMethodRef ref;
  const std::string_view method_name(auth_plugin_name.str,
                                     auth_plugin_name.length);
  const vef_auth_cc_t *cc = find_auth_method(method_name);

  if (cc == nullptr) {
    // The method is neither a loaded MySQL plugin (the caller already ruled
    // that out) nor a registered VEF extension auth method -- e.g. the
    // extension was uninstalled. "Plugin ... is not loaded" is misleading for
    // the extension case and the two are indistinguishable here, so report a
    // neutral VillageSQL error covering both.
    Host_errors errors;
    errors.m_no_auth_plugin = 1;
    inc_host_errors(mpvio->ip, &errors);
    villagesql_error(
        "authentication method '%s' is not available "
        "(no such plugin or extension auth method)",
        MYF(0), auth_plugin_name.str);
    res = CR_ERROR;
  } else {
    res = try_vef_authenticate(cc, mpvio) ? CR_ERROR : CR_OK;
  }

  // Mirror do_auth_once()'s tail: a handler that never called read/write leaves
  // the status at RESTART; reset it so the caller sees a terminal state.
  if (old_status == MPVIO_EXT::RESTART && mpvio->status == MPVIO_EXT::RESTART)
    mpvio->status = MPVIO_EXT::FAILURE;

  return res;
}

}  // namespace villagesql::services
