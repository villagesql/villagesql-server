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

// The authenticator. Reads one packet (the token), compares it to the fixed
// test token, and on success maps to the fixed account. Fail closed otherwise.
vef_auth_result_t authenticate(vef_auth_ctx_t *ctx, const vef_auth_ops_t *ops) {
  const unsigned char *pkt = nullptr;
  const int64_t len = ops->read_packet(ctx, &pkt);
  if (len <= 0 || pkt == nullptr) return VEF_AUTH_ERROR;

  // mysql_clear_password sends a NUL-terminated string; drop the trailing NUL.
  size_t token_len = static_cast<size_t>(len);
  if (pkt[token_len - 1] == '\0') --token_len;

  if (token_len != std::strlen(kToken) ||
      std::memcmp(pkt, kToken, token_len) != 0) {
    return VEF_AUTH_REJECT;
  }

  ops->set_authenticated_as(ctx, kMappedAccount);
  // @@external_user is the original identity for the audit trail: the account
  // the client connected as, NOT the account we mapped to.
  ops->set_external_user(ctx, ops->user_name(ctx));
  return VEF_AUTH_OK;
}

vsql::preview_auth::AuthCapability g_auth{"vsql_auth_test", &authenticate,
                                          "mysql_clear_password"};

}  // namespace

VEF_GENERATE_ENTRY_POINTS(make_extension().with(g_auth))
