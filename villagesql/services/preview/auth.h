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

#ifndef VILLAGESQL_SERVICES_PREVIEW_AUTH_H
#define VILLAGESQL_SERVICES_PREVIEW_AUTH_H

#include <optional>
#include <string>
#include <string_view>

#include "villagesql/sdk/include/villagesql/abi/preview/auth.h"
#include "villagesql/services/capability_registry.h"

// Forward declarations at GLOBAL scope: THD, MPVIO_EXT, and MYSQL_LEX_CSTRING
// are the server's global types. Declaring them inside the namespace below
// would create distinct villagesql::services::* types and break linkage against
// the caller in sql/auth/, which uses the global ones.
class THD;
class Security_context;
struct MPVIO_EXT;
struct MYSQL_LEX_CSTRING;

namespace villagesql::services {

// The "vsql::preview::auth" capability owns its own in-memory registry of
// extension-provided auth methods (name -> handler config), entirely within
// this capability -- no auth-specific code lives in the core persistence or the
// veb DDL path. An auth method is a runtime callback registration (a handler
// pointer valid only while the .so is loaded), not persisted state, so it is
// registered on extension load via on_populate and removed on unload via
// on_depopulate, exactly like the statement_event capability.

// Server-side vtable for the capability (version tag only).
vef_preview_auth_t *preview_auth_vtable();

// Capability lifecycle hooks, wired into the capability registry. on_populate
// validates the auth method's config and adds it to the registry;
// on_depopulate removes it and drains any in-flight authenticate() call before
// returning, so the extension .so is safe to dlclose.
bool on_populate_auth(const PopulateContext &ctx, std::string &error_message);
void on_depopulate_auth(const DepopulateContext &ctx);

// Look up a registered auth method by its bare name (case-insensitive, matching
// the normalization used for cross-extension uniqueness). Returns the
// capability config, or nullptr if no method matches. While the returned
// pointer is in use, the caller MUST hold an AuthMethodRef (below) so a
// concurrent unload cannot free the config / dlclose the .so.
const vef_auth_cc_t *find_auth_method(std::string_view method_name);

// A scoped in-flight reference. While any AuthMethodRef is alive, on_depopulate
// blocks (drains) before returning, so the extension cannot be unloaded out
// from under an authenticate() call. Construct one for the full duration of a
// find_auth_method() result's use.
class AuthMethodRef {
 public:
  AuthMethodRef();
  ~AuthMethodRef();
  AuthMethodRef(const AuthMethodRef &) = delete;
  AuthMethodRef &operator=(const AuthMethodRef &) = delete;
};

// --- Seam for core MySQL auth (sql/auth/) ---
// The auth capability's core-facing entry points, called by core auth in
// sql/auth/ (sql_user.cc for CREATE USER, sql_authentication.cc for the login
// handshake via vsql_do_auth_once() below). Core calls these directly rather
// than reaching into the registry internals above -- mirroring how
// sql/sql_audit.cc calls on_statement_postexecute() directly.

// The name of the single registered auth method currently opting in to handle
// UNKNOWN accounts (via its live auto_create_unknown_accounts callback), or
// empty if none -- or if more than one does (ambiguous, so declined). Used to
// route an unknown-account decoy to that method (see decoy handling in
// sql/auth/) and to name the IDENTIFIED WITH method when provisioning.
std::string auth_method_for_unknown_accounts();

// True if `method_name` currently opts into auto-grant (its live
// auto_grant_roles callback returns true). Independent of the unknown-account
// opt-in above; gates maybe_apply_vef_role_grants for a login using this
// method.
bool method_wants_auto_grant(std::string_view method_name);

// Handle a CREATE USER ... IDENTIFIED WITH <method_name> [BY '...'] whose name
// is not a loaded MySQL auth plugin, deciding whether it names a VEF extension
// auth method.
//
//   std::nullopt - not a VEF method; the caller falls back to its own
//                  "plugin not loaded" error handling.
//   false        - a VEF method; accept the account. A VEF method has no MySQL
//                  plugin, no credential hash, no password history/expiration
//                  -- nothing to validate. Covers plain IDENTIFIED WITH and
//                  IDENTIFIED WITH ... AS '...' (the AS string is the
//                  extension's to interpret).
//   true         - a VEF method used with IDENTIFIED BY '...'. Asking the
//                  (non-existent) plugin to hash a password is meaningless; an
//                  ER_PASSWORD_FORMAT error is raised here and the caller
//                  returns the error.
//
// When a VEF method is recognized (either bool result) the caller must set
// what_to_set to NONE_ATTR and return the bool -- there is nothing more to
// hash.
std::optional<bool> handle_vef_user_bind(std::string_view method_name,
                                         bool uses_identified_by_clause);

// The do_auth_once() equivalent for a VEF extension-provided auth method (the
// account's method is not a loaded MySQL plugin). Bridges the server's
// MPVIO_EXT handshake context to the VEF auth ABI, drives the extension's
// handler, and returns a CR_OK/CR_ERROR result (fail closed). If the name is
// neither a plugin nor a registered VEF method (e.g. its extension was
// uninstalled), reports a neutral VillageSQL "method not available" error.
// Called from acl_authenticate() in sql/auth/sql_authentication.cc, which
// routes here instead of the stock do_auth_once() when the method is not a
// MySQL plugin -- keeping do_auth_once() itself vanilla.
int vsql_do_auth_once(THD *thd, const MYSQL_LEX_CSTRING &auth_plugin_name,
                      MPVIO_EXT *mpvio);

// Opaque per-login state a VEF auth handler stages during the handshake, to be
// applied after account resolution. Its definition and all its fields live in
// auth.cc; core auth (sql/auth/) only holds a pointer to it on MPVIO_EXT and
// forwards it to maybe_apply_vef_auth_state() below. Today it carries the roles
// set via set_active_roles(); anything else a handler needs to stage post-auth
// goes here too, with no further change to MPVIO_EXT.
struct VefAuthState;

// Apply whatever a VEF handler staged for this login, AFTER the account has
// been resolved. Currently: activate the staged role set on `sctx`, replacing
// default-role activation, using the server's grant-checked activation (a role
// not granted to the account is skipped) so a token can never escalate. The
// caller must already hold the ACL cache lock (grant-checked activation reads
// the role graph under it). Does nothing (returns false = "not handled, use
// default roles") when `mpvio` has no staged state. Returns true when it
// applied staged state, so the caller skips its own default-role activation.
// The bool is a handled/not-handled discriminator, NOT the usual MySQL
// true==error convention -- true here is the success path.
//
// `sctx` is the session's Security_context (the same one the default-role path
// activates onto); `acl_user_authid`/`acl_user_host` identify the resolved
// account for warning messages. Roles are activated but access maps are NOT
// checked out here -- the caller does checkout_access_maps() once afterward, as
// it already does for the default-role path.
bool maybe_apply_vef_auth_state(MPVIO_EXT *mpvio, Security_context *sctx,
                                const char *acl_user_authid,
                                const char *acl_user_host);

// Run the account creation staged by request_provision() during the handler,
// if any. Call only after the handler returned OK and only for a provisioning
// login (an unknown-account decoy) -- the caller gates on that; the decoy is
// re-resolved to the created account right after. Deferring creation to here
// means a denied login provisions nothing. Returns true on failure (the caller
// should fail the login closed); false if it succeeded or nothing was staged.
bool run_vef_provision(MPVIO_EXT *mpvio);

// Auto-GRANT the token-staged roles to the resolved account, when its method
// opted into auto_grant. Runs GRANT DDL on a fresh internal THD that takes ACL
// locks itself, so it MUST be called where no ACL cache lock is held -- before
// the role-activation block, not from maybe_apply_vef_auth_state (which runs
// under the ACL read lock). No-op when nothing was staged or the method did not
// opt in. Additive only: a role no longer claimed is not revoked.
void maybe_apply_vef_role_grants(MPVIO_EXT *mpvio, const char *acl_user_authid,
                                 const char *acl_user_host);

}  // namespace villagesql::services

#endif  // VILLAGESQL_SERVICES_PREVIEW_AUTH_H
