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

#ifndef VILLAGESQL_PREVIEW_PING_REGISTER_H
#define VILLAGESQL_PREVIEW_PING_REGISTER_H

#include <villagesql/abi/preview/ping.h>
#include <villagesql/detail/capability_traits.h>
#include <villagesql/preview/ping.h>

namespace vsql::detail {

template <>
struct CapabilityTraits<::vsql::preview_ping::PingCapability> {
  static constexpr const char *kName = VEF_PREVIEW_PING_NAME;
  static constexpr const char *kCppTypeName =
      "vsql::preview_ping::PingCapability";
  static constexpr uint32_t kAbiVersion = VEF_PREVIEW_PING_ABI_VERSION;
  using AbiType = vef_preview_ping_t;

  static constexpr void *vtable_destination(
      ::vsql::preview_ping::PingCapability *p) noexcept {
    return static_cast<void *>(&p->abi_);
  }
};

}  // namespace vsql::detail

#endif  // VILLAGESQL_PREVIEW_PING_REGISTER_H
