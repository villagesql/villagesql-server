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
#include <cstdint>
#include <string>

#include "villagesql/sdk/include/villagesql/abi/types.h"

namespace villagesql::services {

// General-purpose registry mapping (name, version) to a vtable pointer.
// vtable must remain valid for the lifetime of the registration.

// Register a capability. abi_type_hash is
// villagesql::detail::abi_type_hash<AbiType>() and is compared against the
// extension's hash at populate time to detect ABI struct mismatches.
void register_capability(std::string name, uint32_t version, void *vtable,
                         size_t abi_type_hash);

// Unregister a capability. No-op if not registered.
void unregister_capability(const std::string &name, uint32_t version);

// Register all server built-in capabilities. Called once at server startup.
void register_builtin_capabilities();

// Populate capabilities declared in a vef_registration_t.
//
// Called after vef_register() returns. For each entry in
// reg->required_capabilities, looks up the named capability in the registry and
// invokes its receive callback with the vtable pointer.
//
// On failure, sets error_message to a description of what went wrong
// (missing capability or ABI type mismatch) and returns true.
// Returns false if all capabilities were satisfied.
bool populate_capabilities(const vef_registration_t *reg,
                           std::string &error_message);

}  // namespace villagesql::services

#endif  // VILLAGESQL_SERVICES_CAPABILITY_REGISTRY_H
