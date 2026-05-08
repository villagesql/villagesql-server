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

#include "villagesql/sdk/include/villagesql/abi/types.h"

// When false (default), loading an extension that declares any preview
// capabilities fails with an error. Set to true to allow preview capabilities.
extern bool vsql_allow_preview_extensions;

namespace villagesql::services {

// Server-side compatibility check function for a capability.
//
// Compatibility checking is two-tiered:
//   1. Server side (this function): runs before the extension's receive() is
//      called. Decides whether the extension's declared requirements are
//      satisfiable given the server's current vtable. Responsible for calling
//      req.receive() if checks pass.
//   2. Extension side (req.receive): called by the compat function on success.
//      The extension makes its own decision — e.g. strict exact-version match
//      via cap_receive, or custom logic for graceful degradation.
//
// Capability authors implement cap_compat_fn to customise server-side checks
// (e.g. skipping the ABI hash check for versioned capabilities). Pass it as
// the optional 4th argument to register_capability(). When nullptr, the
// default_compat_fn is used: strict ABI hash match + min_version floor.
//
// Returns true if the extension is compatible. On failure, writes a reason
// into error_message and returns false.
using cap_compat_fn = bool (*)(const vef_required_capability_t &req,
                               void *vtable, std::string &error_message);

// Register all server built-in capabilities. Called once at server startup.
void register_builtin_capabilities();

// Populate capabilities declared in a vef_registration_t for one extension.
//
// Called after vef_register() returns. For each entry in
// reg->required_capabilities, looks up the named capability in the registry and
// invokes its receive callback with the vtable pointer.
//
// On failure, sets error_message to a description of what went wrong
// (missing capability or ABI type mismatch) and returns true.
// Returns false if all capabilities were satisfied.
bool populate_capabilities(const vef_registration_t *reg,
                           const vef_register_arg_t *arg,
                           std::string &error_message);

// Called before vef_unregister() when an extension is being unloaded.
// No-op currently; hook exists for future per-capability cleanup.
void depopulate_capabilities(const vef_registration_t *reg);

}  // namespace villagesql::services

#endif  // VILLAGESQL_SERVICES_CAPABILITY_REGISTRY_H
