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

#include <string_view>
#include <unordered_map>

#include "villagesql/sdk/include/villagesql/detail/capability_hash.h"
#include "villagesql/services/preview/column_store.h"
#include "villagesql/services/preview/keyring.h"
#include "villagesql/services/preview/ping.h"
#include "villagesql/services/preview/sql_query.h"
#include "villagesql/services/preview/storage.h"
#include "villagesql/services/preview/thread_worker.h"

bool vsql_allow_preview_extensions = false;

namespace villagesql::services {

namespace {

struct CapabilityValue {
  void *vtable;
  size_t abi_type_hash;
  size_t descriptor_abi_hash;  // 0 if capability has no descriptor
  cap_compat_fn compat_fn;
  // Optional. Called after the compat check for each extension that requires
  // this capability. Receives the extension name and extension_data pointer.
  // NULL for capabilities that need no post-populate setup.
  void (*on_populate)(std::string_view extension_name,
                      const void *extension_data);
  // Optional. Called before unloading an extension that required this
  // capability. Receives the extension_data pointer. Used to stop threads or
  // clean up server-side resources before the extension is removed.
  // NULL for capabilities that need no cleanup.
  void (*on_depopulate)(const void *extension_data);
};

std::unordered_map<std::string, CapabilityValue> g_registry;

const CapabilityValue *find_capability_entry(const std::string &name) {
  auto it = g_registry.find(name);
  if (it == g_registry.end()) return nullptr;
  return &it->second;
}

}  // namespace

// Default server-side compatibility check. Used for capabilities that pass
// nullptr as compat_fn to register_capability().
//
// Checks in order:
//   1. ABI hash: extension must have been compiled against the exact same
//      struct layout as the server. Catches binary incompatibilities.
//   2. min_version floor: server's vtable version must be >= the extension's
//      declared minimum. Guards against extensions that require features not
//      yet present on this server.
// On success, writes the vtable pointer into the extension's vtable_dest.
static bool default_compat_fn(const vef_required_capability_t &req,
                              void *vtable, std::string &error_message) {
  const CapabilityValue *entry = find_capability_entry(req.name);
  if (entry->abi_type_hash != req.abi_type_hash) {
    error_message = std::string("capability ABI mismatch: ") + req.name;
    return false;
  }
  if (req.min_version > 0) {
    uint32_t server_version = *static_cast<const uint32_t *>(vtable);
    if (server_version < req.min_version) {
      error_message = std::string("capability version too old: ") + req.name +
                      " (server=" + std::to_string(server_version) +
                      ", required=" + std::to_string(req.min_version) + ")";
      return false;
    }
  }
  *req.vtable_dest = vtable;
  return true;
}

void register_capability(std::string name, CapabilityRegistration reg) {
  if (reg.on_server_startup != nullptr) reg.on_server_startup();
  g_registry[std::move(name)] = {
      reg.vtable,
      reg.abi_type_hash,
      reg.descriptor_abi_hash,
      reg.compat_fn ? reg.compat_fn : default_compat_fn,
      reg.on_populate,
      reg.on_depopulate};
}

void unregister_capability(const std::string &name) { g_registry.erase(name); }

void register_builtin_capabilities() {
  // Ping uses a custom compat function: skips the ABI hash check so extensions
  // compiled against future ping ABI versions (e.g. with pong()) can still
  // load against older servers, as long as the server meets their min_version.
  register_capability(
      VEF_PREVIEW_PING_NAME,
      {.vtable = preview_ping_vtable(),
       .abi_type_hash = villagesql::detail::abi_type_hash<vef_preview_ping_t>(),
       .compat_fn = preview_ping_compat});
  // Keyring uses the default compat function: strict ABI hash match required.
  register_capability(
      VEF_PREVIEW_KEYRING_NAME,
      {.vtable = preview_keyring_vtable(),
       .abi_type_hash =
           villagesql::detail::abi_type_hash<vef_preview_keyring_t>()});
  register_capability(
      VEF_PREVIEW_STORAGE_NAME,
      {.vtable = preview_storage_vtable(),
       .abi_type_hash =
           villagesql::detail::abi_type_hash<vef_preview_storage_t>()});
  register_capability(
      VEF_PREVIEW_COLUMN_STORE_NAME,
      {.vtable = preview_column_store_vtable(),
       .abi_type_hash =
           villagesql::detail::abi_type_hash<vef_preview_column_store_t>(),
       .descriptor_abi_hash = villagesql::detail::abi_type_hash<
           vef_preview_column_store_ext_desc_t>()});
  register_capability(
      VEF_PREVIEW_THREAD_WORKER_NAME,
      {.vtable = preview_thread_worker_vtable(),
       .abi_type_hash =
           villagesql::detail::abi_type_hash<vef_preview_thread_worker_t>(),
       .descriptor_abi_hash =
           villagesql::detail::abi_type_hash<vef_thread_worker_descriptor_t>(),
       .on_server_startup = init_thread_worker_psi_keys,
       .on_populate = on_populate_thread_worker,
       .on_depopulate = on_depopulate_thread_worker});
  register_capability(
      VEF_PREVIEW_SQL_QUERY_NAME,
      {.vtable = preview_sql_query_vtable(),
       .abi_type_hash =
           villagesql::detail::abi_type_hash<vef_preview_sql_query_t>()});
}

// TODO(villagesql-preview): Verify that the capabilities declared in
// vef_registration_t match those listed in the extension's manifest.
bool populate_capabilities(const vef_registration_t *reg,
                           std::string_view extension_name,
                           std::string &error_message) {
  if (reg == nullptr || reg->protocol < VEF_PROTOCOL_2 ||
      reg->required_capabilities == nullptr ||
      reg->required_capability_count == 0)
    return false;

  if (!vsql_allow_preview_extensions) {
    error_message =
        "extension requires preview capabilities but "
        "vsql_allow_preview_extensions is OFF";
    return true;
  }

  for (uint32_t i = 0; i < reg->required_capability_count; ++i) {
    const vef_required_capability_t &req = reg->required_capabilities[i];
    if (req.name == nullptr || req.vtable_dest == nullptr) continue;

    const CapabilityValue *entry = find_capability_entry(req.name);
    if (entry == nullptr) {
      error_message =
          std::string("required capability not registered: ") + req.name;
      return true;
    }
    if (!entry->compat_fn(req, entry->vtable, error_message)) return true;
    if (entry->on_populate != nullptr) {
      entry->on_populate(extension_name, req.extension_data);
    }
  }

  return false;
}

void depopulate_capabilities(const vef_registration_t *reg) {
  if (reg == nullptr || reg->protocol < VEF_PROTOCOL_2 ||
      reg->required_capabilities == nullptr ||
      reg->required_capability_count == 0)
    return;

  for (uint32_t i = 0; i < reg->required_capability_count; ++i) {
    const vef_required_capability_t &req = reg->required_capabilities[i];
    if (req.name == nullptr) continue;

    const CapabilityValue *entry = find_capability_entry(req.name);
    if (entry == nullptr || entry->on_depopulate == nullptr) continue;
    entry->on_depopulate(req.extension_data);
  }
}

}  // namespace villagesql::services
