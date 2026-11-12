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

#ifndef VILLAGESQL_SERVICES_CAPABILITY_REGISTRY_H
#define VILLAGESQL_SERVICES_CAPABILITY_REGISTRY_H

#include <cstddef>
#include <string>
#include <string_view>

#include "villagesql/sdk/include/villagesql/abi/types.h"

class THD;

// When false (default), loading an extension that declares any preview
// capabilities fails with an error. Set to true to allow preview capabilities.
extern bool vsql_allow_preview_extensions;

namespace villagesql::services {

enum class LoadReason { kStartup, kInstall };
enum class UnloadReason { kShutdown, kUninstall };

// Context passed to on_populate. capability_config is filled in by
// populate_capabilities for each capability; all other fields are set by
// the caller before calling populate_capabilities.
struct PopulateContext {
  std::string_view extension_name;
  const void *capability_config = nullptr;
  LoadReason reason;
  THD *thd = nullptr;
};

// Context passed to on_depopulate. capability_config is filled in by
// depopulate_capabilities for each capability; reason and thd are set by the
// caller (uninstall path or shutdown).
struct DepopulateContext {
  const void *capability_config = nullptr;
  UnloadReason reason;
  THD *thd = nullptr;
};

// Parameters for register_capability(). Zero/null fields use defaults.
struct CapabilityRegistration {
  // Required: server-side vtable pointer.
  void *vtable = nullptr;
  // Required: version tag of the vtable type ("ver-1", "ver-2", ...).
  // Matched by strcmp against the extension-side literal.  Bump when
  // the vtable layout changes in a way the server cannot tolerate.
  const char *vtable_hash = nullptr;
  // Version tag of the capability_config struct type, same form as
  // vtable_hash.  Null if the capability has no capability_config.
  const char *capability_config_hash = nullptr;
  // Called once at server startup (e.g. to register PSI keys). May be null.
  void (*on_server_startup)() = nullptr;
  // Called after the (vtable_hash, capability_config_hash) match succeeds
  // for each extension that requires this capability.  Returns true on
  // error (sets error_message), false on success.  Null for capabilities
  // that need no server-side setup per extension.
  bool (*on_populate)(const PopulateContext &ctx,
                      std::string &error_message) = nullptr;
  // Called before unloading an extension. Null if no cleanup is needed.
  void (*on_depopulate)(const DepopulateContext &ctx) = nullptr;
};

// Register a capability by name.
void register_capability(std::string name, CapabilityRegistration reg);

// Unregister a capability. No-op if not registered.
void unregister_capability(const std::string &name);

// Register all server built-in capabilities. Called once at server startup.
void register_builtin_capabilities();

// Populate capabilities declared in a vef_registration_t for one extension.
//
// Called after vef_register() returns. For each entry in
// reg->required_capabilities, looks up the (name, vtable_hash,
// capability_config_hash) triple in the registry; on a match writes the
// vtable pointer into the extension's vtable_dest slot and invokes the
// capability's on_populate hook (if any).
//
// On failure, sets error_message to a description of what went wrong
// (missing capability or ABI mismatch) and returns true.  Returns false
// if all capabilities were satisfied.
bool populate_capabilities(const PopulateContext &ctx,
                           const vef_registration_t *reg,
                           std::string &error_message);

// Called before vef_unregister() when an extension is being unloaded.
// Invokes on_depopulate for each capability that registered one, allowing
// capabilities to stop threads or clean up server-side resources.
void depopulate_capabilities(const DepopulateContext &ctx,
                             const vef_registration_t *reg);

}  // namespace villagesql::services

#endif  // VILLAGESQL_SERVICES_CAPABILITY_REGISTRY_H
