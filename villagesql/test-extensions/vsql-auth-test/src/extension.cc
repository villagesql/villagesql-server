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

// Trivial VillageSQL auth extension exercising the vsql::preview::auth
// capability end to end -- no JWT, no crypto. It registers an auth method named
// "vsql_auth_test"; an account created with IDENTIFIED WITH vsql_auth_test
// authenticates iff the client sends one of the fixed tokens below, and is then
// mapped to the account "vsql_auth_test_user".
//
// Accepted tokens:
//   kToken           -- accept, map to the account, stage no roles (the
//                       account's default roles apply).
//   kTokenRoles      -- accept and additionally stage a fixed role set via
//                       set_active_roles: one role the test grants to the
//                       account ("vsql_role_granted") and one it does NOT
//                       ("vsql_role_denied"). Lets auth_roles.test assert that
//                       a granted role activates while an ungranted requested
//                       role is silently skipped (the no-escalation guarantee).
//   kTokenQuotedRole -- accept and stage a role whose name contains a quoted
//                       '@' ("`vsql_role@weird`"), so auth_roles.test can
//                       assert the name is parsed whole rather than split at
//                       the '@'.
//
// Pairs with the built-in mysql_clear_password client plugin so the token
// arrives verbatim in the password slot (client must use
// --enable-cleartext-plugin).

#include <cstring>

#include <villagesql/preview/auth.h>
#include <villagesql/vsql.h>

using namespace vsql;

namespace {

constexpr char kToken[] = "vsql-auth-test-token";
constexpr char kTokenRoles[] = "vsql-auth-test-token-roles";
constexpr char kTokenQuotedRole[] = "vsql-auth-test-token-quoted-role";
constexpr char kMappedAccount[] = "vsql_auth_test_user";

using vsql::preview_auth::AuthContext;
using vsql::preview_auth::AuthResult;

// Roles the kTokenRoles path stages. The test grants the first to the account
// and leaves the second ungranted, so activation must pick up the granted one
// and skip the denied one (no escalation).
constexpr char kRoleGranted[] = "vsql_role_granted";
constexpr char kRoleDenied[] = "vsql_role_denied";

// Role staged by the kTokenQuotedRole path: a name that literally contains '@',
// backtick-quoted so the server's role parser keeps the '@' as part of the name
// (host defaults to '%') instead of splitting it as name@host. The test grants
// exactly this role, so it activates only if the '@' was not mis-split.
constexpr char kRoleWithAt[] = "`vsql_role@weird`";

bool token_matches(const unsigned char *pkt, size_t token_len,
                   const char *expected) {
  return token_len == std::strlen(expected) &&
         std::memcmp(pkt, expected, token_len) == 0;
}

// The authenticator. Reads one packet (the token), compares it to the fixed
// test tokens, and on success maps to the fixed account. Fail closed otherwise.
AuthResult authenticate(AuthContext &c) {
  auto pkt = c.read_packet();
  if (pkt.empty()) return AuthResult::kError;

  // mysql_clear_password sends a NUL-terminated string; drop the trailing NUL.
  size_t token_len = pkt.size();
  if (pkt[token_len - 1] == '\0') --token_len;

  const bool plain = token_matches(pkt.data(), token_len, kToken);
  const bool with_roles = token_matches(pkt.data(), token_len, kTokenRoles);
  const bool quoted_role =
      token_matches(pkt.data(), token_len, kTokenQuotedRole);
  if (!plain && !with_roles && !quoted_role) return AuthResult::kReject;

  c.authenticate_as(kMappedAccount);
  // @@external_user is the original identity for the audit trail: the account
  // the client connected as, NOT the account we mapped to.
  c.set_external_user(c.user_name());

  if (with_roles) {
    // Stage a granted + an ungranted role. The server activates only the
    // granted one (grant-checked); the ungranted one is silently skipped.
    const char *roles[] = {kRoleGranted, kRoleDenied};
    c.set_active_roles(roles, 2);
  } else if (quoted_role) {
    // Stage a role whose name contains a quoted '@'; it activates only if the
    // name was parsed whole rather than split at the '@'.
    const char *roles[] = {kRoleWithAt};
    c.set_active_roles(roles, 1);
  }
  return AuthResult::kOk;
}

constexpr auto AUTH_METHOD =
    vsql::preview_auth::make_auth<&authenticate>("vsql_auth_test")
        .client_plugin("mysql_clear_password")
        .build();
vsql::preview_auth::AuthCapability g_auth{AUTH_METHOD};

}  // namespace

VEF_GENERATE_ENTRY_POINTS(make_extension().with(g_auth))
