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

// Existence check, used by CREATE USER validation to accept a VEF auth-method
// name the same way an installed plugin name is accepted.
bool auth_method_exists(std::string_view method_name);

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

}  // namespace villagesql::services

#endif  // VILLAGESQL_SERVICES_PREVIEW_AUTH_H
