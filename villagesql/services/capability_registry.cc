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

#include "villagesql/services/capability_registry.h"

#include <unordered_map>

#include "villagesql/sdk/include/villagesql/detail/capability_hash.h"
#include "villagesql/services/preview/ping.h"

namespace villagesql::services {

namespace {

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

struct CapabilityValue {
  void *vtable;
  size_t abi_type_hash;
};

std::unordered_map<CapabilityKey, CapabilityValue, CapabilityKeyHash>
    g_registry;

}  // namespace

void register_capability(std::string name, uint32_t version, void *vtable,
                         size_t abi_type_hash) {
  g_registry[{std::move(name), version}] = {vtable, abi_type_hash};
}

void unregister_capability(const std::string &name, uint32_t version) {
  g_registry.erase({name, version});
}

static const CapabilityValue *find_capability_entry(const std::string &name,
                                                    uint32_t version) {
  auto it = g_registry.find({name, version});
  if (it == g_registry.end()) return nullptr;
  return &it->second;
}

void register_builtin_capabilities() {
  register_capability(VEF_PREVIEW_PING_NAME, VEF_PREVIEW_PING_VERSION,
                      preview_ping_vtable(),
                      villagesql::detail::abi_type_hash<vef_preview_ping_t>());
  // TODO(villagesql-beta): register "vsql::thread_worker" and "vsql::sql" here
}

// TODO(villagesql-beta): Verify that the capabilities declared in
// vef_registration_t match those listed in the extension's manifest.
bool populate_capabilities(const vef_registration_t *reg,
                           std::string &error_message) {
  if (reg == nullptr || reg->protocol < VEF_PROTOCOL_2 ||
      reg->required_capabilities == nullptr ||
      reg->required_capability_count == 0)
    return false;

  for (uint32_t i = 0; i < reg->required_capability_count; ++i) {
    const vef_required_capability_t &req = reg->required_capabilities[i];
    if (req.name == nullptr || req.receive == nullptr) continue;

    const CapabilityValue *entry = find_capability_entry(req.name, req.version);
    if (entry == nullptr) {
      error_message =
          std::string("required capability not registered: ") + req.name;
      return true;
    }
    if (entry->abi_type_hash != req.abi_type_hash) {
      error_message = std::string("capability ABI mismatch: ") + req.name;
      return true;
    }

    req.receive(entry->vtable);
  }

  return false;
}

}  // namespace villagesql::services
