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
// PREVIEW CAPABILITY -- UNSTABLE API
// =============================================================================
// This header is part of the VEF preview surface. Its API and ABI may change
// or be removed without notice. See villagesql/preview/README.md for details.
// =============================================================================

#ifndef VILLAGESQL_PREVIEW_AUTH_H
#define VILLAGESQL_PREVIEW_AUTH_H

#include <villagesql/abi/preview/auth.h>
#include <villagesql/detail/capability_base.h>
#include <villagesql/detail/capability_traits.h>

namespace vsql::preview_auth {

// Result an authenticator returns. Mirrors vef_auth_result_t.
using AuthResult = vef_auth_result_t;

// The server-provided operations on the auth context (read the token, set the
// effective account, etc.). Passed to the handler. See abi/preview/auth.h.
using AuthOps = vef_auth_ops_t;
using AuthCtx = vef_auth_ctx_t;

// AuthCapability lets an extension register a single authentication method.
// Construct it with the method name accounts bind to (CREATE USER ...
// IDENTIFIED WITH <name>), the handler the server invokes during the handshake,
// and the client-side auth plugin to pin (e.g. "mysql_clear_password" to
// receive a bearer token verbatim). The client plugin is required -- the server
// advertises exactly it during the handshake and rejects a method that leaves
// it unset at INSTALL EXTENSION.
//
// Usage:
//   static vef_auth_result_t my_authenticate(vef_auth_ctx_t *ctx,
//                                             const vef_auth_ops_t *ops) {
//     const unsigned char *tok = nullptr;
//     if (ops->read_packet(ctx, &tok) < 0) return VEF_AUTH_ERROR;
//     ... validate ...
//     ops->set_authenticated_as(ctx, "alice");
//     return VEF_AUTH_OK;
//   }
//
//   static vsql::preview_auth::AuthCapability g_auth{
//       "my_auth", &my_authenticate, "mysql_clear_password"};
//   VEF_GENERATE_ENTRY_POINTS(make_extension().with(g_auth))
class AuthCapability : public ::vsql::detail::CapabilityBase<AuthCapability> {
 public:
  AuthCapability(const char *name, vef_auth_authenticate_func_t handler,
                 const char *client_auth_plugin) {
    cc.name = name;
    cc.handler = handler;
    cc.client_auth_plugin = client_auth_plugin;
  }

  // Capability config read by the server's validate step. CapabilityTraits
  // returns its address so the wire format carries a pointer to it.
  vef_auth_cc_t cc{};

 private:
  template <typename Capability>
  friend struct ::vsql::detail::CapabilityTraits;

  const vef_preview_auth_t *abi_ = nullptr;
};

}  // namespace vsql::preview_auth

#include <villagesql/preview/detail/auth_register.h>

#endif  // VILLAGESQL_PREVIEW_AUTH_H
