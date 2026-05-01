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

#ifndef VILLAGESQL_SERVICES_PREVIEW_CAPABILITIES_H
#define VILLAGESQL_SERVICES_PREVIEW_CAPABILITIES_H

#include <cstdint>
#include <string>

#include "villagesql/sdk/include/villagesql/abi/types.h"

namespace villagesql::services {

// Registry of named preview capabilities.
//
// Built-in capabilities (e.g. "vsql::ping") are registered at server startup
// via register_builtin_capabilities(). Extension-provided capabilities are
// registered via register_capabilities_from_extension() and unregistered via
// unregister_capabilities_from_extension().
//
// populate_preview_capabilities() does a registry lookup for each capability
// declared in a vef_registration_t, so the consumer path is identical
// regardless of how the capability was registered.

// Register a capability by name and version. vtable must remain valid for
// the lifetime of the server (or until unregister_capability is called).
// vtable_size is the size in bytes of the capability struct (e.g.
// sizeof(vef_preview_ping_t)) — used to copy the vtable into the extension's
// capability struct at populate time.
// Intended for built-in capabilities registered at startup and for
// extension-provided capabilities.
void register_capability(std::string name, uint32_t version, void *vtable,
                         size_t vtable_size);

// Unregister a capability. Called when an extension providing a capability
// is uninstalled. No-op if the capability was not registered.
void unregister_capability(const std::string &name, uint32_t version);

// Register all server built-in capabilities. Called once at server startup.
void register_builtin_capabilities();

// Populate preview capabilities declared in a vef_registration_t.
//
// Called after vef_register() returns. For each entry in
// reg->required_capabilities, looks up the named capability in the registry and
// writes the vtable pointer into the capability struct. Unknown names or
// unsupported versions leave the struct untouched (function pointers remain
// nullptr).
void populate_preview_capabilities(const vef_registration_t *reg);

}  // namespace villagesql::services

#endif  // VILLAGESQL_SERVICES_PREVIEW_CAPABILITIES_H
