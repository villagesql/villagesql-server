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

#include <cstdint>

#include <villagesql/abi/preview/ping.h>

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
//   make_extension().preview_require<g_ping>()
class PingCapability {
 public:
  static constexpr const char *kName = VEF_PREVIEW_PING_NAME;
  static constexpr uint32_t kVersion = VEF_PREVIEW_PING_VERSION;

  // Returns the next counter value from the server, or 0 if unavailable.
  uint64_t ping() const {
    if (abi_.ping == nullptr) return 0;
    return abi_.ping();
  }

  bool available() const { return abi_.ping != nullptr; }

  // Public so that preview_require() can take &abi_ as a constexpr pointer.
  // Do not access directly — use ping() and available() instead.
  vef_preview_ping_t abi_;
};

inline PingCapability make_capability() { return PingCapability{}; }

}  // namespace vsql::preview::ping

#endif  // VILLAGESQL_PREVIEW_PING_H
