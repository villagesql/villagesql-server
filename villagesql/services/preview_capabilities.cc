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

#include "villagesql/services/preview_capabilities.h"

#include <atomic>
#include <cstring>
#include <unordered_map>

#include "villagesql/sdk/include/villagesql/abi/preview/ping.h"

namespace villagesql::services {

namespace {

// Registry key: capability name + version.
struct CapabilityKey {
  std::string name;
  uint32_t version;

  bool operator==(const CapabilityKey &o) const {
    return version == o.version && name == o.name;
  }
};

struct CapabilityKeyHash {
  size_t operator()(const CapabilityKey &k) const {
    size_t h = std::hash<std::string>{}(k.name);
    h ^= std::hash<uint32_t>{}(k.version) + 0x9e3779b9 + (h << 6) + (h >> 2);
    return h;
  }
};

struct CapabilityEntry {
  void *vtable;
  size_t vtable_size;
};

// Global capability registry. Maps (name, version) -> vtable + size.
std::unordered_map<CapabilityKey, CapabilityEntry, CapabilityKeyHash>
    g_registry;

// Built-in "vsql::ping" implementation.
std::atomic<uint64_t> g_ping_counter{0};
uint64_t vsql_ping() { return ++g_ping_counter; }
vef_preview_ping_t g_ping_vtable{&vsql_ping};

}  // namespace

void register_capability(std::string name, uint32_t version, void *vtable,
                         size_t vtable_size) {
  g_registry[{std::move(name), version}] = {vtable, vtable_size};
}

void unregister_capability(const std::string &name, uint32_t version) {
  g_registry.erase({name, version});
}

void register_builtin_capabilities() {
  register_capability(VEF_PREVIEW_PING_NAME, VEF_PREVIEW_PING_VERSION,
                      &g_ping_vtable, sizeof(vef_preview_ping_t));
  // TODO(villagesql-beta): register "vsql::thread_worker" and "vsql::sql" here
}

void populate_preview_capabilities(const vef_registration_t *reg) {
  if (reg == nullptr || reg->protocol < VEF_PROTOCOL_2 ||
      reg->required_capabilities == nullptr ||
      reg->required_capability_count == 0)
    return;

  for (uint32_t i = 0; i < reg->required_capability_count; ++i) {
    const vef_required_capability_t &req = reg->required_capabilities[i];
    if (req.name == nullptr || req.capability == nullptr) continue;

    auto it = g_registry.find({req.name, req.version});
    if (it == g_registry.end()) continue;

    // Copy the vtable into the extension's capability struct. Both are the
    // same ABI type and the registered size matches sizeof(that type).
    memcpy(req.capability, it->second.vtable, it->second.vtable_size);
  }
}

}  // namespace villagesql::services
