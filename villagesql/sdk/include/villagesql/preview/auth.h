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
#include <villagesql/detail/span.h>

namespace vsql::preview_auth {

// Result an authenticator returns. Fail closed: anything other than kOk denies.
enum class AuthResult {
  kOk = VEF_AUTH_OK,          // authenticated; the handler set the account
  kReject = VEF_AUTH_REJECT,  // bad credential / policy rejection
  kError = VEF_AUTH_ERROR,    // couldn't decide (e.g. key source down)
};

// AuthContext is the idiomatic C++ handshake context handed to an
// authenticator. It wraps the raw ABI context + ops table (see
// abi/preview/auth.h) so authors write against methods instead of threading a
// ctx pointer through a C function table. The server owns the underlying
// context; do not retain an AuthContext beyond the handler call.
class AuthContext {
 public:
  AuthContext(vef_auth_ctx_t *ctx, const vef_auth_ops_t *ops)
      : ctx_(ctx), ops_(ops) {}

  // Read the next client packet. Returns the bytes as a span valid until the
  // next read (empty on a protocol/connection error). Paired with
  // mysql_clear_password, one read yields the bearer token.
  Span<const unsigned char> read_packet() {
    const unsigned char *data = nullptr;
    const int64_t n = ops_->read_packet(ctx_, &data);
    if (n < 0 || data == nullptr) return {};
    return {data, static_cast<size_t>(n)};
  }

  // Send a packet to the client (e.g. a challenge). Returns true on failure.
  bool write_packet(Span<const unsigned char> data) {
    return ops_->write_packet(ctx_, data.data(), data.size());
  }

  // The account name the client connected as. Empty before the first read.
  const char *user_name() const { return ops_->user_name(ctx_); }

  // The AS '...' clause from CREATE USER ... IDENTIFIED WITH <m> AS '...'.
  const char *auth_string() const { return ops_->auth_string(ctx_); }

  // The client host or IP.
  const char *host_or_ip() const { return ops_->host_or_ip(ctx_); }

  // Set the effective account the session runs AS (CURRENT_USER()). Required
  // before returning kOk. Mapping to a different account than user_name() is
  // proxying and needs a GRANT PROXY, exactly as on the plugin path.
  void authenticate_as(const char *account) {
    ops_->set_authenticated_as(ctx_, account);
  }

  // Set the original external identity for the audit trail (@@external_user).
  void set_external_user(const char *identity) {
    ops_->set_external_user(ctx_, identity);
  }

 private:
  vef_auth_ctx_t *ctx_;
  const vef_auth_ops_t *ops_;
};

// An authenticator: given the handshake context, decide the login. Authors
// write this idiomatic form; make_auth<> adapts it to the raw C ABI.
using AuthHandler = AuthResult (*)(AuthContext &);

// AuthDescriptor is the passive, fluent builder for one authentication method.
// The authenticate handler is a compile-time template argument
// (make_auth<&handler>), config is set with chained setters, and build() yields
// the descriptor. It does NOT self-register -- it is a value you hand to an
// AuthCapability token (see below).
//
// Splitting the fluent builder (this) from the self-registering token
// (AuthCapability) is what lets auth use chained setters safely: the enrolling
// object stays single-config, so a chained temporary can't double-enroll.
//
// Usage:
//   AuthResult my_authenticate(AuthContext &c) {
//     auto tok = c.read_packet();
//     if (tok.empty()) return AuthResult::kError;
//     ... validate ...
//     c.authenticate_as("alice");
//     return AuthResult::kOk;
//   }
//
//   constexpr auto MY_AUTH = vsql::preview_auth::make_auth<&my_authenticate>(
//                                "my_auth")
//                                .client_plugin("mysql_clear_password")
//                                .build();
//   static vsql::preview_auth::AuthCapability g_auth{MY_AUTH};
//   VEF_GENERATE_ENTRY_POINTS(make_extension().with(g_auth))
class AuthDescriptor {
 public:
  // `client_plugin`: the client-side auth plugin the server advertises for this
  // method (e.g. "mysql_clear_password" to receive a bearer token verbatim).
  // Optional -- defaults to "mysql_clear_password" (see make_auth), the lowest
  // common denominator every MySQL client ships, so a naive client still
  // connects.
  constexpr AuthDescriptor &client_plugin(const char *name) {
    cc_.client_auth_plugin = name;
    return *this;
  }

  constexpr vef_auth_cc_t build() const { return cc_; }

 private:
  template <AuthHandler Handler>
  friend constexpr AuthDescriptor make_auth(const char *name);

  vef_auth_cc_t cc_{};
};

namespace detail {
// The raw ABI shim make_auth<> stores as the C handler: it wraps the ctx/ops
// into an AuthContext, calls the author's typed handler, and maps the result
// back to the C enum. One instantiation per handler; its address is a stable
// function pointer suitable for the vef_auth_cc_t.handler slot.
template <AuthHandler Handler>
vef_auth_result_t auth_shim(vef_auth_ctx_t *ctx, const vef_auth_ops_t *ops) {
  AuthContext c(ctx, ops);
  return static_cast<vef_auth_result_t>(Handler(c));
}
}  // namespace detail

// make_auth<&handler>("name") -- entry point for the fluent auth builder. The
// handler is a compile-time template argument, so a null or wrong-signature
// handler is a compile error, not a runtime failure. The advertised client
// plugin defaults to "mysql_clear_password"; override with .client_plugin().
template <AuthHandler Handler>
constexpr AuthDescriptor make_auth(const char *name) {
  AuthDescriptor d;
  d.cc_.name = name;
  d.cc_.handler = &detail::auth_shim<Handler>;
  d.cc_.client_auth_plugin = "mysql_clear_password";
  return d;
}

// AuthCapability is the self-registering token consumed by
// make_extension().with(...). It wraps one built AuthDescriptor (passed to its
// constructor); its cc member is what CapabilityTraits hands to the server (by
// address), so it must outlive registration (declare the token static).
class AuthCapability : public ::vsql::detail::CapabilityBase<AuthCapability> {
 public:
  // Construct the token from a built descriptor. AuthCapability is non-copyable
  // (it self-registers at a fixed address), so a token must be constructed in
  // place, not copy-initialized from a temporary. Usage:
  //   static AuthCapability g_auth{MY_AUTH};
  explicit AuthCapability(const vef_auth_cc_t &desc) { cc = desc; }

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
