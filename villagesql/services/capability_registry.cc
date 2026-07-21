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

#include <cstring>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "villagesql/sdk/include/villagesql/abi/preview/keyring.h"
#include "villagesql/sdk/include/villagesql/abi/preview/ping.h"
#include "villagesql/sdk/include/villagesql/abi/preview/sql_query.h"
#include "villagesql/sdk/include/villagesql/abi/preview/statement_event.h"
#include "villagesql/sdk/include/villagesql/abi/preview/status_var.h"
#include "villagesql/sdk/include/villagesql/abi/preview/storage.h"
#include "villagesql/sdk/include/villagesql/abi/preview/sys_var.h"
#include "villagesql/sdk/include/villagesql/abi/preview/thread_worker.h"
#include "villagesql/services/preview/auth.h"
#include "villagesql/services/preview/column_store.h"
#include "villagesql/services/preview/index_profile.h"
#include "villagesql/services/preview/index_type.h"
#include "villagesql/services/preview/keyring.h"
#include "villagesql/services/preview/ping.h"
#include "villagesql/services/preview/sql_query.h"
#include "villagesql/services/preview/statement_event.h"
#include "villagesql/services/preview/status_var.h"
#include "villagesql/services/preview/storage.h"
#include "villagesql/services/preview/sys_var.h"
#include "villagesql/services/preview/thread_worker.h"

// Each register_capability() call declares the version of each ABI
// struct this server is willing to serve.  Extensions carry the same
// version literal in their CapabilityTraits specializations; the
// literals are matched at extension load time via strcmp on
// vtable_hash + capability_config_hash on the wire.  Bump the literal
// (e.g. "ver-1" -> "ver-2") whenever the struct's layout changes in a
// way the server cannot tolerate.

bool vsql_allow_preview_extensions = false;

namespace villagesql::services {

namespace {

// One registered (capability, ABI version) pair.  A capability name may map
// to multiple CapabilityVersion entries -- the server can serve multiple
// ABI shapes of the same capability simultaneously, and the extension
// chooses which by providing vtable_hash (plus capability_config_hash
// if the capability has a descriptor).
struct CapabilityVersion {
  void *vtable;
  // Static-lifetime version tag ("ver-1", "ver-2", ...) identifying the
  // ABI struct shape this entry serves.  Comparison with
  // vef_required_capability_t::vtable_hash is by strcmp.
  const char *vtable_hash;
  // Version tag of the descriptor struct type for capabilities that take
  // one (e.g. thread_worker).  nullptr for capabilities without a
  // descriptor.  Comparison with
  // vef_required_capability_t::capability_config_hash is by strcmp, treating
  // nullptr as a sentinel that only matches another nullptr.
  const char *capability_config_hash;
  // Optional. Called after the hash match succeeds for each extension that
  // requires this capability. Returns true on error (sets error_message),
  // false on success. NULL for capabilities that need no per-extension
  // setup.
  bool (*on_populate)(const PopulateContext &ctx, std::string &error_message);
  // Optional. Called before unloading an extension that required this
  // capability. Used to stop threads or clean up server-side resources
  // before the extension is removed. NULL for capabilities that need no
  // cleanup.
  void (*on_depopulate)(const DepopulateContext &ctx);
};

// Invariant: g_registry is mutated only from register_builtin_capabilities()
// (server startup) and unregister_capability() (server shutdown), both of
// which run single-threaded.  populate_capabilities() reads it from whatever
// thread runs INSTALL EXTENSION but only after startup registration is done.
// No locking is required as long as this invariant holds.  If a future
// capability ever needs post-startup registration, this map needs a mutex
// -- the inner vector's address-stability would be lost on concurrent
// push_back.
std::unordered_map<std::string, std::vector<CapabilityVersion>> g_registry;

// Compare two descriptor hash strings, with nullptr meaning "no descriptor".
// Two nullptrs match; nullptr vs a real string does not.
bool capability_config_hash_matches(const char *a, const char *b) {
  if (a == nullptr && b == nullptr) return true;
  if (a == nullptr || b == nullptr) return false;
  return std::strcmp(a, b) == 0;
}

// Look up the specific (name, vtable_hash, capability_config_hash)
// triple.  Returns nullptr when no matching version is registered.
const CapabilityVersion *find_capability_version(
    const char *name, const char *vtable_hash,
    const char *capability_config_hash) {
  if (name == nullptr || vtable_hash == nullptr) return nullptr;
  auto it = g_registry.find(name);
  if (it == g_registry.end()) return nullptr;
  for (const auto &v : it->second) {
    if (std::strcmp(v.vtable_hash, vtable_hash) != 0) continue;
    if (!capability_config_hash_matches(v.capability_config_hash,
                                        capability_config_hash))
      continue;
    return &v;
  }
  return nullptr;
}

}  // namespace

void register_capability(std::string name, CapabilityRegistration reg) {
  if (reg.on_server_startup != nullptr) reg.on_server_startup();
  g_registry[std::move(name)].push_back({reg.vtable, reg.vtable_hash,
                                         reg.capability_config_hash,
                                         reg.on_populate, reg.on_depopulate});
}

void unregister_capability(const std::string &name) { g_registry.erase(name); }

void register_builtin_capabilities() {
  // Each (capability, ABI version) is registered as a separate entry.
  // To add a new ABI version for an existing capability, register an
  // additional entry under the same name with the new vtable and its
  // new version tag (e.g. "ver-2").  The corresponding extension-side
  // literal in each capability's _register.h must use the same value;
  // that is what makes the wire-level strcmp at extension load time
  // meaningful.
  register_capability(VEF_PREVIEW_PING_NAME, {.vtable = preview_ping_vtable(),
                                              .vtable_hash = "ver-1"});
  register_capability(
      VEF_PREVIEW_KEYRING_NAME,
      {.vtable = preview_keyring_vtable(), .vtable_hash = "ver-1"});
  register_capability(
      VEF_PREVIEW_STORAGE_NAME,
      {.vtable = preview_storage_vtable(), .vtable_hash = "ver-1"});
  register_capability(VEF_PREVIEW_THREAD_WORKER_NAME,
                      {.vtable = preview_thread_worker_vtable(),
                       .vtable_hash = "ver-1",
                       .capability_config_hash = "ver-1",
                       .on_server_startup = init_thread_worker_psi_keys,
                       .on_populate = on_populate_thread_worker,
                       .on_depopulate = on_depopulate_thread_worker});
  register_capability(VEF_PREVIEW_COLUMN_STORE_NAME,
                      {.vtable = preview_column_store_vtable(),
                       .vtable_hash = "ver-1",
                       .capability_config_hash = "ver-1"});
  register_capability(VEF_PREVIEW_INDEX_TYPE_NAME,
                      {.vtable = preview_index_type_vtable(),
                       .vtable_hash = "ver-1",
                       .capability_config_hash = "ver-1"});
  // Auth: on_populate validates the method config and adds it to the auth
  // capability's own in-memory registry; on_depopulate removes it and drains
  // any in-flight login before unload. No auth-specific code in core.
  register_capability(VEF_PREVIEW_AUTH_NAME,
                      {.vtable = preview_auth_vtable(),
                       .vtable_hash = "ver-1",
                       .capability_config_hash = "ver-1",
                       .on_populate = on_populate_auth,
                       .on_depopulate = on_depopulate_auth});
  register_capability(VEF_PREVIEW_INDEX_PROFILE_NAME,
                      {.vtable = preview_index_profile_vtable(),
                       .vtable_hash = "ver-1",
                       .capability_config_hash = "ver-1"});
  register_capability(
      VEF_PREVIEW_SQL_QUERY_NAME,
      {.vtable = preview_sql_query_vtable(), .vtable_hash = "ver-1"});
  // Status var: on_populate registers the extension's variables with
  // MySQL; on_depopulate unregisters them on extension unload.
  register_capability(VEF_PREVIEW_STATUS_VAR_NAME,
                      {.vtable = preview_status_var_vtable(),
                       .vtable_hash = "ver-1",
                       .capability_config_hash = "ver-1",
                       .on_populate = on_populate_status_var,
                       .on_depopulate = on_depopulate_status_var});
  // Sys var: on_populate registers the extension's system variables
  // with MySQL; on_depopulate unregisters them on extension unload.
  register_capability(VEF_PREVIEW_SYS_VAR_NAME,
                      {.vtable = preview_sys_var_vtable(),
                       .vtable_hash = "ver-1",
                       .capability_config_hash = "ver-1",
                       .on_populate = on_populate_sys_var,
                       .on_depopulate = on_depopulate_sys_var});
  // Statement event: on_populate appends to the global dispatch list;
  // on_depopulate removes the handler on extension unload.
  register_capability(VEF_PREVIEW_STATEMENT_EVENT_NAME,
                      {.vtable = preview_statement_event_vtable(),
                       .vtable_hash = "ver-1",
                       .capability_config_hash = "ver-1",
                       .on_populate = on_populate_statement_event,
                       .on_depopulate = on_depopulate_statement_event});
}

// TODO(villagesql-preview): Verify that the capabilities declared in
// vef_registration_t match those listed in the extension's manifest.
bool populate_capabilities(const PopulateContext &ctx,
                           const vef_registration_t *reg,
                           std::string &error_message) {
  if (reg == nullptr || reg->protocol < VEF_PROTOCOL_3 ||
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

    // vtable_hash is mandatory: every extension built against the current
    // SDK has it populated by vef_register.h.  An extension that arrives
    // without one was built against an SDK that predates the structural
    // fingerprint, which we do not support.
    if (req.vtable_hash == nullptr) {
      error_message =
          std::string("required capability missing ABI fingerprint: ") +
          req.name;
      return true;
    }
    // Distinguish "name unknown" from "name known but no version matches"
    // so the diagnostic points at the right thing.
    auto it = g_registry.find(req.name);
    if (it == g_registry.end()) {
      error_message =
          std::string("required capability not registered: ") + req.name;
      return true;
    }
    const CapabilityVersion *entry = find_capability_version(
        req.name, req.vtable_hash, req.capability_config_hash);
    if (entry == nullptr) {
      error_message =
          std::string("no matching ABI version for capability '") + req.name +
          "' (extension requires " + req.vtable_hash +
          (req.capability_config_hash != nullptr
               ? std::string(" + descriptor ") + req.capability_config_hash
               : std::string()) +
          ")";
      return true;
    }
    *req.vtable_dest = entry->vtable;
    if (entry->on_populate != nullptr) {
      // ctx carries shared fields (reason, thd, extension_name);
      // capability_config is capability-specific and comes from the
      // per-capability req entry.
      PopulateContext cap_ctx = ctx;
      cap_ctx.capability_config = req.capability_config;
      if (entry->on_populate(cap_ctx, error_message)) return true;
    }
  }

  return false;
}

void depopulate_capabilities(const DepopulateContext &ctx,
                             const vef_registration_t *reg) {
  if (reg == nullptr || reg->protocol < VEF_PROTOCOL_3 ||
      reg->required_capabilities == nullptr ||
      reg->required_capability_count == 0)
    return;

  for (uint32_t i = 0; i < reg->required_capability_count; ++i) {
    const vef_required_capability_t &req = reg->required_capabilities[i];
    if (req.name == nullptr || req.vtable_hash == nullptr) continue;

    const CapabilityVersion *entry = find_capability_version(
        req.name, req.vtable_hash, req.capability_config_hash);
    if (entry == nullptr || entry->on_depopulate == nullptr) continue;
    // Same as populate: ctx carries shared fields, capability_config is
    // per-cap.
    DepopulateContext cap_ctx = ctx;
    cap_ctx.capability_config = req.capability_config;
    entry->on_depopulate(cap_ctx);
  }
}

}  // namespace villagesql::services
