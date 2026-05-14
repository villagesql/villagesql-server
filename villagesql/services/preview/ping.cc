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
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, see <https://www.gnu.org/licenses/>.

#include "villagesql/services/preview/ping.h"

#include <atomic>
#include <string>

#include "villagesql/sdk/include/villagesql/abi/types.h"

namespace villagesql::services {

namespace {

std::atomic<uint64_t> g_ping_counter{0};
uint64_t vsql_ping() { return ++g_ping_counter; }
vef_preview_ping_t g_ping_vtable{VEF_PREVIEW_PING_ABI_VERSION, &vsql_ping};

}  // namespace

vef_preview_ping_t *preview_ping_vtable() { return &g_ping_vtable; }

// Custom server-side compat check for the ping capability.
//
// Intentionally skips the ABI hash check. Because the ping vtable is versioned
// (version field always first), extensions compiled against a newer ABI
// (e.g. vef_preview_ping_v2_t with pong) have a different struct hash but can
// still safely receive a pointer to this server's vtable — they will only
// access fields up to the server's declared version.
//
// Fails if the server's vtable version is less than the extension's declared
// min_version, meaning the server is too old to satisfy the extension's needs.
bool preview_ping_compat(const vef_required_capability_t &req, void *vtable,
                         std::string &error_message) {
  uint32_t server_version = *static_cast<const uint32_t *>(vtable);
  if (req.min_version > server_version) {
    error_message = std::string("capability version too old: ") +
                    VEF_PREVIEW_PING_NAME +
                    " (server=" + std::to_string(server_version) +
                    ", required=" + std::to_string(req.min_version) + ")";
    return false;
  }
  *req.vtable_dest = vtable;
  return true;
}

}  // namespace villagesql::services
