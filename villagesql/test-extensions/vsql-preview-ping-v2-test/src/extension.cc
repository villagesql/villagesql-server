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
// Uses a local ping_abi_v2.h instead of the SDK's ping header.  The
// extension's CapabilityTraits<PingV2Capability>::kVtableHash is
// "ver-2" -- guaranteed not to match the server's "ver-1" entry for
// vef_preview_ping_t, so INSTALL EXTENSION fails with "no matching
// ABI version for capability 'vsql::preview::ping'".

#include "ping_abi_v2.h"

#include <cstdint>

#include <villagesql/detail/capability_base.h>
#include <villagesql/detail/capability_traits.h>
#include <villagesql/vsql.h>

using namespace vsql;

// This extension drops below the usual high-level capability API (no
// vsql::preview_ping_v2::* wrapper, no shipped _register.h) and hand-rolls
// the pieces a real capability would get from the SDK:
//   - PingV2Capability is the minimal wrapper a CapabilityTraits
//     specialization needs (just the abi pointer slot the server writes
//     into).
//   - The CapabilityTraits<PingV2Capability> specialization below is what a
//     `<villagesql/preview/ping_v2_register.h>` header *would* provide if
//     ping v2 actually existed.
// We do this because the whole point of the test is to assert mismatch
// behaviour against a hypothetical future ABI shape -- introducing a full
// high-level wrapper just for the test would be more machinery than the
// failure mode it exercises.
struct PingV2Capability
    : public ::vsql::detail::CapabilityBase<PingV2Capability> {
  PingV2Capability() {}

  const vef_preview_ping_v2_t *abi = nullptr;
};

namespace vsql::detail {

template <>
struct CapabilityTraits<PingV2Capability> {
  static constexpr const char *kName = VEF_PREVIEW_PING_NAME;
  static constexpr const char *kCppTypeName = "PingV2Capability";
  // Deliberately mismatched version -- the server registers
  // vef_preview_ping_t as "ver-1", so "ver-2" guarantees INSTALL hits
  // the "no matching ABI version" path which this test exercises.
  static constexpr const char *kVtableHash = "ver-2";

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
  out.set(g_ping.abi->ping());
}

static void pong_value_impl(IntResult out) {
  if (g_ping.abi == nullptr || g_ping.abi->version < 2) {
    out.set_null();
    return;
  }
  out.set(g_ping.abi->pong());
}

VEF_GENERATE_ENTRY_POINTS(
    make_extension()
        .func(make_func<&ping_v2_value_impl>("ping_v2_value")
                  .returns(INT)
                  .no_params()
                  .build())
        .func(make_func<&pong_value_impl>("pong_value")
                  .returns(INT)
                  .no_params()
                  .build())
        .with(g_ping))
