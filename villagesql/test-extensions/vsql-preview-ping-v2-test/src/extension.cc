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

// vsql_preview_ping_v2_test extension: simulates an extension compiled against
// a future ping ABI v2 (which includes pong()) loading against a server that
// only provides ping ABI v1.
//
// Uses a local ping_abi_v2.h instead of the SDK's ping header so that it can
// declare kAbiVersion=2 without waiting for the real ping v2 to exist. The
// server's preview_ping_compat checks min_version against its vtable version
// field (1) and rejects the extension with "capability version too old".

#include "ping_abi_v2.h"

#include <cstdint>

#include <villagesql/vsql.h>
#include <villagesql/detail/capability_base.h>
#include <villagesql/detail/capability_traits.h>

using namespace vsql;

struct PingV2Capability
    : public ::vsql::detail::CapabilityBase<PingV2Capability> {
  PingV2Capability() {}

  const vef_preview_ping_v2_t *abi = nullptr;
};

namespace vsql::detail {

template <>
struct CapabilityTraits<PingV2Capability> {
  static constexpr const char *kName = VEF_PREVIEW_PING_NAME;
  static constexpr uint32_t kAbiVersion = VEF_PREVIEW_PING_V2_ABI_VERSION;
  using AbiType = vef_preview_ping_v2_t;

  static constexpr void *vtable_destination(PingV2Capability *p) noexcept {
    return static_cast<void *>(&p->abi);
  }
};

}  // namespace vsql::detail

static PingV2Capability g_ping{};

static void ping_v2_value_impl(IntResult out) {
  if (g_ping.abi == nullptr) {
    out.set_null();
    return;
  }
  out.set(static_cast<long long>(g_ping.abi->ping()));
}

static void pong_value_impl(IntResult out) {
  if (g_ping.abi == nullptr || g_ping.abi->version < 2) {
    out.set_null();
    return;
  }
  out.set(static_cast<long long>(g_ping.abi->pong()));
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .func(make_func<&ping_v2_value_impl>("ping_v2_value")
                  .returns(INT)
                  .build())
        .func(make_func<&pong_value_impl>("pong_value").returns(INT).build())
        .with(g_ping))
