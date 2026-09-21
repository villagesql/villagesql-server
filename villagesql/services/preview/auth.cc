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
#include "sql/auth/auth_common.h"
#include "sql/auth/sql_authentication.h"
#include "sql/auth/sql_security_ctx.h"
#include "sql/auto_thd.h"
#include "sql/current_thd.h"
#include "sql/hostname_cache.h"
#include "sql/mem_root_array.h"
#include "sql/statement/ed_connection.h"
#include "sql/strfunc.h"
#include "sql_string.h"
#include "strmake.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/identifier_names.h"
#include "villagesql/sdk/include/villagesql/abi/preview/auth.h"

// The VEF auth ctx is just the connection's MPVIO_EXT. The extension handler
// only ever sees vef_auth_ctx_t (opaque) + the ops table, so it never touches
// MySQL internals directly.
struct vef_auth_ctx_t {
  MPVIO_EXT *mpvio;
};

namespace villagesql::services {

// The roles a VEF handler staged via set_active_roles(), as the raw name
// strings the handler passed. The array and its strings are allocated on the
// connection MEM_ROOT (freed with the connection). The element type is
// trivially destructible, so the array needs no destructor run -- it is safe to
// leave on the MEM_ROOT and never destruct (only the constructor, which takes
// the root, must run). Parsing of each "role"/"role@host" name is deferred to
// apply time so it can reuse the server's own quote-aware splitter (see
// maybe_apply_vef_auth_state). An empty `roles` (with a non-null VefAuthState)
// means "activate no roles" (SET ROLE NONE), distinct from a null VefAuthState
// pointer ("handler staged nothing; use the account's default roles").
struct VefAuthState {
  explicit VefAuthState(MEM_ROOT *mem_root) : roles(mem_root) {}
  Mem_root_array<const char *> roles;
};

// A provisioning request staged by request_provision(), to be CREATEd by
// run_vef_provision() only after the handler returns OK -- so a login the
// handler ultimately denies creates nothing. Strings/array live on the
// connection MEM_ROOT (trivially-destructible, no destructor needed). `account`
// is the handler-chosen name to create (kept verbatim -- it need not be the
// connecting user); `method` is the VEF auth method to bind it to
// (IDENTIFIED WITH); `roles` are granted after create.
struct VefProvisionRequest {
  explicit VefProvisionRequest(MEM_ROOT *mem_root) : roles(mem_root) {}
  const char *account;
  const char *method;
  Mem_root_array<const char *> roles;
};

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

void vef_auth_set_active_roles(vef_auth_ctx_t *ctx, const char *const *roles,
                               uint32_t n_roles) {
  MPVIO_EXT *mpvio = ctx->mpvio;
  MEM_ROOT *mem_root = mpvio->mem_root;

  // Allocate/replace the state on the connection MEM_ROOT (freed with the
  // connection; no manual cleanup). Re-staging replaces the prior set. Copy
  // each name so nothing points into the extension's transient buffer; the
  // "role"/"role@host" parse is deferred to apply time.
  auto *state = new (mem_root) VefAuthState(mem_root);
  state->roles.reserve(n_roles);

  for (uint32_t i = 0; i < n_roles; ++i) {
    const char *r = roles[i];
    if (r == nullptr || r[0] == '\0') continue;  // skip empties defensively
    state->roles.push_back(strdup_root(mem_root, r));
  }

  mpvio->vef_auth_info.vef_auth_state = state;
}

bool vef_auth_account_unknown(vef_auth_ctx_t *ctx) {
  // True when the account being authenticated does not exist -- i.e. this login
  // was routed here as a decoy by the unknown-account opt-in (see
  // find_mpvio_user).
  return ctx->mpvio->acl_user_vef_provision_candidate;
}

// Run `sql` as one privileged statement on its own fresh internal THD. Returns
// true on failure (logged). One Auto_THD per statement: reusing one trips a
// binlog-XID assert on the 2nd DDL. Auto_THD's ctor repoints current_thd at its
// THD via store_globals(), so capture the connection's THD first and restore it
// after -- otherwise, once ~Auto_THD frees its THD, the rest of
// acl_authenticate runs with current_thd still pointing at that freed THD.
static bool provision_run(const std::string &sql) {
  THD *const conn_thd = current_thd;
  bool failed = false;
  {
    Auto_THD provisioner;
    provisioner.thd->security_context()->skip_grants();
    Ed_connection conn(provisioner.thd);
    MYSQL_LEX_STRING s;
    lex_string_strmake(provisioner.thd->mem_root, &s, sql.c_str(),
                       sql.length());
    if (conn.execute_direct(s)) {
      failed = true;
      LogVSQL(WARNING_LEVEL, "auto-create: statement failed (errno=%u): %s",
              conn.get_last_errno(),
              conn.get_last_error() ? conn.get_last_error() : "");
    }
  }  // ~Auto_THD frees its THD; current_thd now points at freed memory
  conn_thd->store_globals();  // restore the connection thread's globals
  return failed;
}

// GRANT one staged "role"/"role@host" to `account_id` (a quoted `user`@`host`).
// The role name is quoted as an identifier so a crafted name cannot break out
// of the DDL; an ungrantable role (e.g. no such DB role) is logged and skipped,
// not fatal.
static void grant_staged_role(const char *staged,
                              const std::string &account_id) {
  if (staged == nullptr || staged[0] == '\0') return;
  const auto [role_name, role_host] = get_authid_from_quoted_string(staged);
  const std::string role_id = Auth_id(role_name.c_str(), role_name.length(),
                                      role_host.c_str(), role_host.length())
                                  .auth_str();
  std::string grant = "GRANT ";
  grant.append(role_id);
  grant.append(" TO ");
  grant.append(account_id);
  (void)provision_run(grant);
}

void vef_auth_request_provision(vef_auth_ctx_t *ctx, const char *account,
                                const char *const *roles, uint32_t n_roles) {
  if (account == nullptr || account[0] == '\0') return;
  MPVIO_EXT *mpvio = ctx->mpvio;
  MEM_ROOT *mem_root = mpvio->mem_root;

  // Record the intent; the DDL runs later, in run_vef_provision(), only
  // if the handler returns OK -- so a login the handler then denies creates
  // nothing (no orphan). Copy every string onto the connection MEM_ROOT so
  // nothing points into the extension's transient buffers. `method` is the VEF
  // method this login authenticated against (its account binds to it,
  // IDENTIFIED WITH).
  auto *req = new (mem_root) VefProvisionRequest(mem_root);
  req->account = strdup_root(mem_root, account);
  const char *const method = mpvio->acl_user_plugin.str;
  req->method = (method != nullptr) ? strdup_root(mem_root, method) : nullptr;
  req->roles.reserve(n_roles);
  for (uint32_t i = 0; i < n_roles; ++i) {
    const char *r = roles[i];
    if (r == nullptr || r[0] == '\0') continue;  // skip empties defensively
    req->roles.push_back(strdup_root(mem_root, r));
  }
  mpvio->vef_auth_info.vef_provision_request = req;
}

const char *vef_auth_client_plugin(vef_auth_ctx_t *ctx) {
  // The client-plugin name the client advertised for this connection, cached on
  // the handshake context. Set before the handler's first read, so it is
  // available throughout the handler call.
  const char *p = ctx->mpvio->cached_client_reply.plugin;
  return p != nullptr ? p : "";
}

const vef_auth_ops_t g_vef_auth_ops = {
    VEF_PREVIEW_AUTH_ABI_VERSION,  vef_auth_read_packet,
    vef_auth_write_packet,         vef_auth_user_name,
    vef_auth_auth_string,          vef_auth_host_or_ip,
    vef_auth_set_authenticated_as, vef_auth_set_external_user,
    vef_auth_set_active_roles,     vef_auth_account_unknown,
    vef_auth_request_provision,    vef_auth_client_plugin};

}  // namespace

vef_preview_auth_t *preview_auth_vtable() { return &g_auth_vtable; }

bool maybe_apply_vef_auth_state(MPVIO_EXT *mpvio, Security_context *sctx,
                                const char *acl_user_authid,
                                const char *acl_user_host) {
  const VefAuthState *state = mpvio->vef_auth_info.vef_auth_state;
  if (state == nullptr) return false;  // handler staged nothing; use defaults

  // The caller holds the ACL cache lock (this replaces the account's
  // default-role activation, which runs under the same lock) and calls
  // checkout_access_maps() afterward. Activate each staged role grant-checked:
  // activate_role(..., validate_access=true) refuses a role not granted to the
  // authenticated account, so a token can only activate what the DBA granted --
  // never grant or escalate. An empty set activates nothing (SET ROLE NONE).
  for (const char *staged : state->roles) {
    // Parse "role"/"role@host" the same way SET ROLE does: split on an unquoted
    // '@' and default the host to '%', so a role name containing a quoted '@'
    // is not mis-split. activate_role() copies the strings, so the temporaries
    // outlive the call.
    const auto [name, host] = get_authid_from_quoted_string(staged);
    const LEX_CSTRING role{name.c_str(), name.length()};
    const LEX_CSTRING role_host{host.c_str(), host.length()};
    if (sctx->activate_role(role, role_host, /*validate_access=*/true)) {
      LogVSQL(WARNING_LEVEL,
              "VEF auth: role '%s'@'%s' requested for account '%s'@'%s' is not "
              "granted; skipping",
              role.str, role_host.str,
              acl_user_authid != nullptr ? acl_user_authid : "",
              acl_user_host != nullptr ? acl_user_host : "");
    }
  }
  // A non-null staged set replaces the account's default roles, even if it
  // resolves to no active roles -- because it was empty (n_roles == 0, i.e. SET
  // ROLE NONE) or because every requested role was ungranted and skipped above.
  // Only a null state (handler staged nothing) keeps the defaults.
  //
  // TODO(villagesql-beta): make the all-skipped/empty outcome configurable --
  // some deployments may prefer falling back to the account's default roles
  // when a staged set activates nothing, rather than ending with no roles.
  return true;  // staged state applied; caller must NOT also activate defaults
}

bool run_vef_provision(MPVIO_EXT *mpvio) {
  const VefProvisionRequest *req = mpvio->vef_auth_info.vef_provision_request;
  if (req == nullptr)
    return false;  // handler staged no provision; nothing to do
  if (req->method == nullptr || req->method[0] == '\0') return true;

  // Provisioning runs under skip_grants (SUPER), which the DBA opted into. So
  // it follows the same read_only policy as any superuser CREATE USER:
  // super_read_only forbids it, plain read_only permits it (SUPER exempt, by
  // design) -- as does a correctly super_read_only replica.
  //
  // TODO(villagesql-beta): concurrent logins for the same unknown account race
  // with no single-flight guard -- CREATE USER IF NOT EXISTS is idempotent, but
  // the GRANTs are not coordinated. A server-internal provisioning queue with
  // single-flight dedup would close this before high-concurrency use.

  // `account` is handler-supplied (ultimately client-influenced, pre-auth) and
  // this DDL runs under skip_grants (SUPER), so quote it and the method/role
  // names as identifiers -- never concatenate. auth_str / append_identifier
  // backtick-escape, so a crafted name cannot break out.
  const std::string account_id =
      Auth_id(req->account, strlen(req->account), "%", 1).auth_str();
  String method_id;
  append_identifier(&method_id, req->method, strlen(req->method));

  std::string create = "CREATE USER IF NOT EXISTS ";
  create.append(account_id);
  create.append(" IDENTIFIED WITH ");
  create.append(method_id.ptr(), method_id.length());
  if (provision_run(create)) return true;  // create failed -> fail the login

  for (const char *staged : req->roles) grant_staged_role(staged, account_id);
  return false;
}

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

  const std::string normalized = canonical_extension_name(cc->name);
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
      canonical_extension_name(std::string(method_name));
  std::lock_guard<std::mutex> lock(g_mu);
  for (const auto &m : g_methods) {
    if (m.method_name == normalized) return m.cc;
  }
  return nullptr;
}

std::string auth_method_for_unknown_accounts() {
  std::lock_guard<std::mutex> lock(g_mu);
  const vef_auth_cc_t *chosen = nullptr;
  for (const auto &m : g_methods) {
    // Query the opt-in LIVE (so it reflects the extension's runtime sysvar),
    // not a static registration flag. NULL callback == never opts in.
    if (m.cc != nullptr && m.cc->auto_create_unknown_accounts != nullptr &&
        m.cc->auto_create_unknown_accounts()) {
      if (chosen != nullptr) {
        // More than one method opted in: ambiguous. Decline to guess and fall
        // back to normal unknown-account handling.
        LogVSQL(WARNING_LEVEL,
                "Multiple auth methods opted in to handle unknown accounts; "
                "routing unknown accounts is disabled until only one does");
        return std::string();
      }
      chosen = m.cc;
    }
  }
  return (chosen != nullptr && chosen->name != nullptr)
             ? std::string(chosen->name)
             : std::string();
}

bool method_wants_auto_grant(std::string_view method_name) {
  const std::string normalized =
      canonical_extension_name(std::string(method_name));
  std::lock_guard<std::mutex> lock(g_mu);
  // Queried live so it reflects the extension's runtime sysvar.
  for (const auto &m : g_methods) {
    if (m.method_name == normalized) {
      return m.cc != nullptr && m.cc->auto_grant_roles != nullptr &&
             m.cc->auto_grant_roles();
    }
  }
  return false;
}

void maybe_apply_vef_role_grants(MPVIO_EXT *mpvio, const char *acl_user_authid,
                                 const char *acl_user_host) {
  const VefAuthState *state = mpvio->vef_auth_info.vef_auth_state;
  if (state == nullptr || state->roles.empty()) return;  // nothing staged

  // Only grant when this login's method has opted into auto-grant; off by
  // default, in which case the staged roles are used solely to activate roles
  // the account already holds, never to grant new ones. The method name is the
  // account's plugin.
  const char *const method = mpvio->acl_user_plugin.str;
  if (method == nullptr || !method_wants_auto_grant(method)) return;

  if (acl_user_authid == nullptr || acl_user_authid[0] == '\0') return;
  const std::string account_id =
      Auth_id(acl_user_authid, strlen(acl_user_authid),
              acl_user_host != nullptr ? acl_user_host : "%",
              acl_user_host != nullptr ? strlen(acl_user_host) : 1)
          .auth_str();

  // GRANT each staged role additively -- never revoke. Roles must pre-exist as
  // DB roles; an ungrantable one is skipped.
  //
  // TODO(villagesql-general): authoritative reconcile (revoke roles no longer
  // claimed) is a separate, deferred task.
  for (const char *staged : state->roles) grant_staged_role(staged, account_id);
}

std::optional<bool> handle_vef_user_bind(std::string_view method_name,
                                         bool uses_identified_by_clause) {
  // Existence check only -- the returned pointer is compared, not dereferenced,
  // so no AuthMethodRef is needed (g_mu inside find_auth_method makes the
  // search race-free; nothing here uses the method after the lock is dropped).
  if (find_auth_method(method_name) == nullptr) return std::nullopt;
  if (uses_identified_by_clause) {
    // IDENTIFIED BY '...' supplies a password for the method to turn into a
    // stored credential -- the job a MySQL plugin does via
    // generate_authentication_string(). No VEF method today declares such
    // credential handling, so reject rather than silently store the string as
    // the auth string.
    //
    // TODO(villagesql-beta): make this a per-method capability, not a blanket
    // reject. Whether BY is valid depends on the method's own content (does it
    // declare a credential hook, mirroring st_mysql_auth's
    // AUTH_FLAG_USES_INTERNAL_STORAGE + generate_authentication_string?), so
    // the decision belongs where the method's cc is actually inspected --
    // which, unlike this existence check, must hold an AuthMethodRef across the
    // inspection (the extension may be uninstalled mid-statement). An API-key
    // method that provides the hook would then accept BY and hash the key.
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
  mpvio->vef_auth_info.vef_client_auth_plugin = cc->client_auth_plugin;
  // Stash the method's accept-offer predicate (may be null) so the negotiation
  // that first read triggers can ask this method whether the client's offered
  // plugin is acceptable as-is.
  mpvio->vef_auth_info.vef_accepts_client_plugin = cc->accepts_client_plugin;

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
