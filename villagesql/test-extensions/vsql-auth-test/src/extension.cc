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
// capability end to end -- no JWT, no crypto. It registers method
// "vsql_auth_test" that authenticates iff the client sends one of the fixed
// tokens below. The connecting account "auth_user" proxies to
// "vsql_auth_test_user"; any other account authenticates as itself. The
// auto_create / auto_grant opt-ins are exposed as sysvars (default OFF) so
// tests toggle each independently.
//
// Tokens: kToken (no roles); kTokenRoles (stages vsql_role_granted +
// vsql_role_denied); kTokenQuotedRole (stages a role whose name has a quoted
// '@'). What each proves lives in the corresponding .test.
//
// Pairs with the built-in mysql_clear_password client plugin so the token
// arrives verbatim in the password slot (client must use
// --enable-cleartext-plugin).

#include <cstring>

#include <villagesql/preview/auth.h>
#include <villagesql/preview/sys_var.h>
#include <villagesql/vsql.h>

using namespace vsql;
namespace sv = vsql::preview_sys_var;

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

// The two auto-* opt-ins, each backed by its own sysvar (default OFF) so tests
// can toggle them independently: auto_create routes unknown accounts here for
// provisioning; auto_grant has the server grant staged roles to existing
// accounts. Queried live by the auth capability's callbacks below.
bool g_auto_create = false;
bool g_auto_grant = false;
bool auto_create_enabled() { return g_auto_create; }
bool auto_grant_enabled() { return g_auto_grant; }

// The authenticator. Reads one packet (the token), compares it to the fixed
// test tokens, and on success resolves the session account. Fail closed
// otherwise.
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

  const char *const user = c.user_name();

  // Auto-create: if the account does not exist (this login was routed here by
  // the unknown-account opt-in), ask the server to provision it as itself,
  // IDENTIFIED WITH this method, granting the mapped role. The server defers
  // the DDL until this handler returns kOk and fails the login if it can't
  // create the account; the session then runs AS the (now-real) connecting
  // account.
  if (c.account_unknown()) {
    const char *roles[] = {kRoleGranted};
    c.request_provision(user, roles, 1);
    c.authenticate_as(user);
    c.set_external_user(user);
    return AuthResult::kOk;
  }

  // Pre-existing account: only "auth_user" proxies to the fixed mapped account
  // (auth_basic/auth_roles GRANT PROXY for that); everyone else -- including an
  // auto-created account on a later login -- authenticates as itself.
  const bool proxy_to_mapped =
      user != nullptr && std::strcmp(user, "auth_user") == 0;
  const char *const account = proxy_to_mapped ? kMappedAccount : user;
  c.authenticate_as(account);
  // @@external_user is the original identity for the audit trail: the account
  // the client connected as, NOT the account we mapped to.
  c.set_external_user(user);

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
        .auto_create(&auto_create_enabled)
        .auto_grant(&auto_grant_enabled)
        .build();
vsql::preview_auth::AuthCapability g_auth{AUTH_METHOD};

// Sysvars backing the two opt-ins (default OFF): SET GLOBAL
// vsql_auth_test.auto_create / .auto_grant lets a test enable each feature.
auto SYS_VARS = sv::make_capability({
    sv::make_bool("auto_create", "Route unknown accounts here for provisioning",
                  &g_auto_create, false),
    sv::make_bool("auto_grant", "Grant token-staged roles to existing accounts",
                  &g_auto_grant, false),
});

}  // namespace

VEF_GENERATE_ENTRY_POINTS(make_extension().with(g_auth).with(SYS_VARS))
