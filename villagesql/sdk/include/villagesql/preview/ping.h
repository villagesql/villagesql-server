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

#ifndef VILLAGESQL_PREVIEW_PING_H
#define VILLAGESQL_PREVIEW_PING_H

#include <type_traits>

#include <villagesql/abi/preview/ping.h>
#include <villagesql/detail/capability_hash.h>
#include <villagesql/vsql/extension_builder.h>

namespace vsql::preview::ping {

// C++ wrapper around vef_preview_ping_t.
//
// Usage:
//   static auto g_ping = vsql::preview::ping::make_capability();
//
//   // In extension code:
//   uint64_t n = g_ping.ping();
//
// Register with:
//   make_extension().with<preview_ping<g_ping>>()
class Capability {
 public:
  static constexpr const char *kName = VEF_PREVIEW_PING_NAME;
  static constexpr uint32_t kAbiVersion = VEF_PREVIEW_PING_ABI_VERSION;

  // Returns the next counter value from the server, or 0 if unavailable.
  uint64_t ping() const {
    if (!available()) return 0;
    return abi_->ping();
  }

  bool available() const { return version() > 0; }

  // Returns the server-side capability ABI version, or 0 if unavailable.
  // Compare against VEF_PREVIEW_PING_ABI_VERSION to check what the current
  // SDK was compiled against.
  uint32_t version() const { return abi_ != nullptr ? abi_->version : 0; }

  // Public so that cap_receive() can store the server vtable pointer here.
  // Do not access directly — use ping() and available() instead.
  const vef_preview_ping_t *abi_ = nullptr;
};

inline Capability make_capability() { return Capability{}; }

}  // namespace vsql::preview::ping

namespace vsql::preview {

// Traits type for registering the ping capability via
// .with<preview_ping<cap>>. Only available when this header is included.
template <auto &cap>
struct preview_ping {
  template <typename Inner>
  static constexpr auto bind(Inner builder) {
    using Cap = ping::Capability;
    return builder.required_capability(
        {Cap::kName, &::vsql::cap_receive<Cap, &cap>,
         ::villagesql::detail::abi_type_hash<
             std::remove_cv_t<std::remove_pointer_t<decltype(cap.abi_)>>>(),
         Cap::kAbiVersion});
  }
};

}  // namespace vsql::preview

#endif  // VILLAGESQL_PREVIEW_PING_H
