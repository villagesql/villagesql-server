// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, see <https://www.gnu.org/licenses/>.

// vsql_preview_ping_legacy_test extension: requires the ping capability by
// its legacy "ver-1" tag instead of its structural fingerprint, the way an
// extension built with an SDK from before fingerprints does (including the
// stable_sdk/v3 snapshot and the Rust SDK).  The server keeps those tags
// registered, so this must load and ping must work.
//
// The SDK's PingCapability now sends the fingerprint, so this hand-rolls the
// wrapper and its CapabilityTraits to send the old tag.

#include <cstdint>

#include <villagesql/abi/preview/ping.h>
#include <villagesql/detail/capability_base.h>
#include <villagesql/detail/capability_traits.h>
#include <villagesql/vsql.h>

using namespace vsql;

struct LegacyPingCapability
    : public ::vsql::detail::CapabilityBase<LegacyPingCapability> {
  LegacyPingCapability() {}

  const vef_preview_ping_t *abi = nullptr;
};

namespace vsql::detail {

template <>
struct CapabilityTraits<LegacyPingCapability> {
  static constexpr const char *kName = VEF_PREVIEW_PING_NAME;
  static constexpr const char *kCppTypeName = "LegacyPingCapability";
  static constexpr const char *kVtableHash = "ver-1";

  static constexpr void *vtable_destination(LegacyPingCapability *p) noexcept {
    return static_cast<void *>(&p->abi);
  }
};

}  // namespace vsql::detail

static LegacyPingCapability g_ping{};

static void ping_impl(IntResult out) {
  if (g_ping.abi == nullptr) {
    out.set_null();
    return;
  }
  out.set(static_cast<long long>(g_ping.abi->ping()));
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .func(make_func<&ping_impl>("ping").returns(INT).no_params().build())
        .with(g_ping))
