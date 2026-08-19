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
// along with this program; if not, see <https://www.gnu.org/licenses/>.

// =============================================================================
// VEF PREVIEW ABI HEADER -- UNSTABLE BINARY INTERFACE
// =============================================================================
// This header is both:
//   - an ABI header -- these are the raw C types on the wire. Extension authors
//     should use the C++ API in <villagesql/preview/auth.h> instead. (Preview
//     capabilities are NOT surfaced through the stable <villagesql/vsql.h>
//     umbrella; they are included explicitly.) See villagesql/abi/README.md.
//   - a preview capability -- API and ABI may change or be removed without
//     notice. See villagesql/preview/README.md.
// =============================================================================
//
// TODO(villagesql-beta): the other preview ABI headers (index, keyring, ping,
// sql_query, statement_event, status_var, storage, sys_var, thread_worker)
// still carry the old banner pointing authors at <villagesql/vsql.h>, which
// does not include any preview capability. Sweep the same fix across all of
// them.

#ifndef VILLAGESQL_ABI_PREVIEW_AUTH_H
#define VILLAGESQL_ABI_PREVIEW_AUTH_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Preview capability: "vsql::preview::auth"
//
// Lets an extension provide a server authentication method. An account opts in
// with CREATE USER ... IDENTIFIED WITH <method-name>; at connection time, when
// that name is not a loaded MySQL auth plugin, the server consults the VEF auth
// registry and invokes the registered handler over the handshake.
//
// The handler is a sibling of a VDF, not a typed function: it talks to the
// client by reading/writing handshake packets through the server-owned context
// below. The extension never sees MySQL's internal auth structs.
//
// Capability name: VEF_PREVIEW_AUTH_NAME

#define VEF_PREVIEW_AUTH_NAME "vsql::preview::auth"

// Capability ABI version compiled into this SDK snapshot.
#define VEF_PREVIEW_AUTH_ABI_VERSION 1

// Maximum length of an auth-method name (bytes, excluding NUL).
#define VEF_AUTH_MAX_NAME_LEN 64

// Result of an authentication attempt. There is deliberately NO "maybe"/fail-
// open value: the server treats anything that is not VEF_AUTH_OK as a denial.
typedef enum {
  // Authentication succeeded. The handler must have set the effective account
  // via set_authenticated_as(); the session runs as that account.
  VEF_AUTH_OK = 0,
  // Authentication failed (bad credential, policy rejection). Fail closed.
  VEF_AUTH_REJECT = 1,
  // An internal error prevented a decision (e.g. key source unavailable). Fail
  // closed -- treated identically to REJECT by the server, but distinguishes
  // "denied" from "couldn't decide" for logging.
  VEF_AUTH_ERROR = 2,
} vef_auth_result_t;

// Opaque per-attempt context. The extension holds it only for the duration of
// one authenticate call and uses the accessor vtable below; it must not retain
// the pointer. Backed server-side by the handshake VIO + auth info.
typedef struct vef_auth_ctx_t vef_auth_ctx_t;

// Server-provided operations on the context. The server fills this vtable and
// points the ctx at it; the extension only calls through it. Every function
// pointer below is always populated by the server -- none are optional, so the
// extension may call any of them without a null check.
//
// All const char* outputs are valid only for the duration of the handler call
// (copy before returning if needed). Setters copy their input.
typedef struct {
  uint32_t version;  // VEF_PREVIEW_AUTH_ABI_VERSION

  // Read the next packet the client sent during the handshake. Returns the
  // number of bytes (>=0) and sets *data to a server-owned buffer valid until
  // the next read; returns < 0 on a protocol/connection error. For a bearer
  // token (paired with mysql_clear_password) one read yields the token.
  int64_t (*read_packet)(vef_auth_ctx_t *ctx, const unsigned char **data);

  // Send a packet to the client (e.g. a challenge). Returns false on success,
  // true on failure -- the whole packet is sent or it fails, there is no
  // partial write (so this is a status, not a byte count like read_packet).
  bool (*write_packet)(vef_auth_ctx_t *ctx, const unsigned char *data,
                       uint64_t len);

  // The account name the client connected as (from the handshake). May be
  // empty before the first read_packet.
  const char *(*user_name)(vef_auth_ctx_t *ctx);

  // The AS '...' clause from CREATE USER ... IDENTIFIED WITH <m> AS '...', or
  // empty. Lets an account pin per-identity config.
  const char *(*auth_string)(vef_auth_ctx_t *ctx);

  // The client host or IP.
  const char *(*host_or_ip)(vef_auth_ctx_t *ctx);

  // Set the effective account the session authenticates AS (shown by
  // CURRENT_USER(), used for authorization). Required on VEF_AUTH_OK. Mapping
  // to a different account than user_name() is proxying and requires a
  // GRANT PROXY, exactly as for the plugin path.
  void (*set_authenticated_as)(vef_auth_ctx_t *ctx, const char *account);

  // Set the original external identity for the audit trail (@@external_user).
  void (*set_external_user)(vef_auth_ctx_t *ctx, const char *identity);

  // Stage the set of roles the token says should be active on this session,
  // replacing default-role activation for this login. `roles` is an array of
  // `n_roles` NUL-terminated role names. The server applies them AFTER account
  // resolution, using the same grant-checked activation as SET ROLE: only roles
  // actually granted to the authenticated_as account are activated -- names not
  // granted are ignored, so the token can never grant or escalate beyond what
  // the DBA provisioned. The strings are copied; the caller need not keep them.
  // Passing n_roles == 0 activates no roles (equivalent to SET ROLE NONE).
  void (*set_active_roles)(vef_auth_ctx_t *ctx, const char *const *roles,
                           uint32_t n_roles);

  // Return true if the account being authenticated does NOT exist -- i.e. this
  // login was routed here by the unknown-account opt-in
  // (auto_create_unknown_accounts). Lets an auto-creating handler tell a normal
  // login (real account) from one that needs provisioning. False for a
  // pre-existing account.
  bool (*account_unknown)(vef_auth_ctx_t *ctx);

  // Request that the server provision `account` for this (validated) login:
  // create it IDENTIFIED WITH this auth method and grant `roles` (each a
  // NUL-terminated role name, "role" or "role@host"). The server -- not the
  // extension -- runs the DDL, with its own privileges, bounded by policy; the
  // extension only describes intent (mirrors set_active_roles). Call after
  // validating the token, when account_unknown() is true.
  //
  // The intent is only recorded here; the server runs it AFTER the handler
  // returns OK, and ONLY for an account that was routed here as unknown -- a
  // request for an already-existing account is ignored. So a login the handler
  // then denies, or one against an account that already exists, creates
  // nothing. A creation failure (e.g. read_only) is surfaced by the server as a
  // failed login, not to the handler -- hence no return value. The strings are
  // copied.
  void (*request_provision)(vef_auth_ctx_t *ctx, const char *account,
                            const char *const *roles, uint32_t n_roles);

  // The name of the client-side auth plugin the connection actually used (as
  // advertised by the client), e.g. "mysql_clear_password" or
  // "authentication_openid_connect_client". Lets a handler pick how to parse
  // the credential packet by which client plugin produced it, instead of
  // sniffing packet bytes (different client plugins frame the credential
  // differently). Empty if unknown. Valid for the duration of the handler call.
  const char *(*client_auth_plugin)(vef_auth_ctx_t *ctx);
} vef_auth_ops_t;

// The handler the extension implements. Invoked synchronously on the connecting
// thread during the handshake. Returns a vef_auth_result_t; the server maps
// anything other than VEF_AUTH_OK to a denied connection (fail closed).
typedef vef_auth_result_t (*vef_auth_authenticate_func_t)(
    vef_auth_ctx_t *ctx, const vef_auth_ops_t *ops);

// Capability config (cc), filled in by the extension and passed to the server
// via vef_required_capability_t.capability_config. Stored opaquely in the
// capability's auth registry; the auth seam casts back to this. `name` is the
// auth-method name accounts bind to (IDENTIFIED WITH <name>); `handler` must
// remain valid for the lifetime of the extension.
typedef struct {
  const char *name;
  vef_auth_authenticate_func_t handler;
  // Required: the single client-side auth plugin the server pins for this
  // method (e.g. "mysql_clear_password" to receive a bearer token). The server
  // advertises exactly this plugin to the client during the handshake. Must be
  // non-null and non-empty -- a method that leaves it unset is rejected at
  // INSTALL EXTENSION.
  const char *client_auth_plugin;
  // Optional callback: return true if this method currently wants logins
  // for UNKNOWN accounts routed to it (so it can validate the token and, with
  // server support, provision the account on the fly) instead of the login
  // being rejected outright. It is QUERIED LIVE per unknown-account login, so
  // the extension can back it with a runtime sysvar (e.g. SET GLOBAL
  // vsql_oauth2.auto_create) rather than freezing the choice at registration.
  // nullptr, or a callback returning false, preserves standard "unknown account
  // -> access denied". At most one registered method may return true at a time;
  // the server routes normally (as if none opted in) if more than one does.
  bool (*auto_create_unknown_accounts)(void);
  // Optional callback: return true if this method currently wants the server to
  // GRANT the roles it stages via set_active_roles() to the (already-existing)
  // account, so a token claiming a role the account was not granted takes
  // effect instead of being skipped. Independent of
  // auto_create_unknown_accounts: this governs granting to existing accounts,
  // that governs provisioning unknown ones. QUERIED LIVE per login, so it can
  // reflect a runtime sysvar. NULL, or a callback returning false, keeps the
  // activate-only default (the DBA owns grants; a token can only activate roles
  // already granted).
  bool (*auto_grant_roles)(void);
  // Optional callback: return true if this method can parse a credential
  // delivered by the client plugin named `offered` as-is, so the server accepts
  // it without switching to client_auth_plugin. Returning false (or a nullptr
  // callback) forces a change-plugin switch to client_auth_plugin -- the client
  // resends the credential verbatim -- so a naive client still connects.
  // Queried during handshake negotiation, before the handler's first read, so
  // it must be a pure predicate (no packet I/O, no blocking, no side effects).
  // `offered` is the raw client-supplied plugin name.
  bool (*accepts_client_plugin)(const char *offered);
} vef_auth_cc_t;

// Server-side vtable. Version first, matching the other preview capabilities.
// Registration happens via capability_config; no methods are exposed here.
typedef struct {
  uint32_t version;
} vef_preview_auth_t;

#ifdef __cplusplus
}
#endif

#endif  // VILLAGESQL_ABI_PREVIEW_AUTH_H
