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

// Context passed to on_populate. extension_data is filled in by
// populate_capabilities for each capability; all other fields are set by
// the caller before calling populate_capabilities.
struct PopulateContext {
  std::string_view extension_name;
  const void *extension_data = nullptr;
  LoadReason reason;
  THD *thd = nullptr;
};

// Context passed to on_depopulate. extension_data is filled in by
// depopulate_capabilities for each capability; reason and thd are set by the
// caller (uninstall path or shutdown).
struct DepopulateContext {
  const void *extension_data = nullptr;
  UnloadReason reason;
  THD *thd = nullptr;
};

// Server-side compatibility check function for a capability.
//
// Decides whether the extension's declared requirements are satisfiable given
// the server's current vtable. On success, writes the vtable pointer into
// *req.vtable_dest so the extension can call into it.
//
// Capability authors implement cap_compat_fn to customise server-side checks
// (e.g. skipping the ABI hash check for versioned capabilities). Pass it as
// the compat_fn argument to register_capability(). When nullptr, the
// default_compat_fn is used: strict ABI hash match + min_version floor.
//
// Returns true if the extension is compatible. On failure, writes a reason
// into error_message and returns false.
using cap_compat_fn = bool (*)(const vef_required_capability_t &req,
                               void *vtable, std::string &error_message);

// Parameters for register_capability(). Zero/null fields use defaults.
struct CapabilityRegistration {
  // Required: server-side vtable pointer.
  void *vtable = nullptr;
  // Required: villagesql::detail::abi_type_hash<VtableType>().
  size_t abi_type_hash = 0;
  // Hash of the descriptor struct type (0 if capability has no descriptor).
  size_t descriptor_abi_hash = 0;
  // Called once at server startup (e.g. to register PSI keys). May be null.
  void (*on_server_startup)() = nullptr;
  // Called after the compat check for each extension that requires this
  // capability. Returns true on error (sets error_message), false on success.
  // Null for capabilities that need no per-extension setup.
  bool (*on_populate)(const PopulateContext &ctx,
                      std::string &error_message) = nullptr;
  // Called before unloading an extension. Null if no cleanup is needed.
  void (*on_depopulate)(const DepopulateContext &ctx) = nullptr;
  // Overrides the default server-side compat check (ABI hash + min_version).
  // Null uses default_compat_fn.
  cap_compat_fn compat_fn = nullptr;
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
// reg->required_capabilities, looks up the named capability in the registry,
// runs its compat function, and on success writes the vtable pointer into the
// extension's vtable_dest slot. Then calls on_populate (if set).
//
// On failure, sets error_message to a description of what went wrong
// (missing capability or ABI type mismatch) and returns true.
// Returns false if all capabilities were satisfied.
bool populate_capabilities(const vef_registration_t *reg,
                           std::string &error_message,
                           const PopulateContext &ctx);

// Called before vef_unregister() when an extension is being unloaded.
// Invokes on_depopulate for each capability that registered one, allowing
// capabilities to stop threads or clean up server-side resources.
void depopulate_capabilities(const vef_registration_t *reg,
                             const DepopulateContext &ctx);

}  // namespace villagesql::services

#endif  // VILLAGESQL_SERVICES_CAPABILITY_REGISTRY_H
