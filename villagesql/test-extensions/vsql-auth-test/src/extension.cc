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
// authenticates iff the client sends the fixed token below, and is then mapped
// to the account "vsql_auth_test_user".
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
constexpr char kMappedAccount[] = "vsql_auth_test_user";

using vsql::preview_auth::AuthContext;
using vsql::preview_auth::AuthResult;

// The authenticator. Reads one packet (the token), compares it to the fixed
// test token, and on success maps to the fixed account. Fail closed otherwise.
AuthResult authenticate(AuthContext &c) {
  auto pkt = c.read_packet();
  if (pkt.empty()) return AuthResult::kError;

  // mysql_clear_password sends a NUL-terminated string; drop the trailing NUL.
  size_t token_len = pkt.size();
  if (pkt[token_len - 1] == '\0') --token_len;

  if (token_len != std::strlen(kToken) ||
      std::memcmp(pkt.data(), kToken, token_len) != 0) {
    return AuthResult::kReject;
  }

  c.authenticate_as(kMappedAccount);
  // @@external_user is the original identity for the audit trail: the account
  // the client connected as, NOT the account we mapped to.
  c.set_external_user(c.user_name());
  return AuthResult::kOk;
}

constexpr auto AUTH_METHOD =
    vsql::preview_auth::make_auth<&authenticate>("vsql_auth_test")
        .client_plugin("mysql_clear_password")
        .build();
vsql::preview_auth::AuthCapability g_auth{AUTH_METHOD};

}  // namespace

VEF_GENERATE_ENTRY_POINTS(make_extension().with(g_auth))
