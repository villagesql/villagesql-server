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

#ifndef VILLAGESQL_SERVICES_AIRLOCK_REGISTRY_H
#define VILLAGESQL_SERVICES_AIRLOCK_REGISTRY_H

// Server-side airlock handler registry.
//
// Companion to capability_registry. An airlock channel is identified by a
// name; a registered handler is invoked with bytes the extension supplied
// in its airlock request and does whatever the channel defines — including
// writing through pointers the bytes contain. The dispatch itself doesn't
// know what the bytes mean; only the bridge layer on each side does.

#include <cstddef>
#include <functional>
#include <string>

#include "villagesql/sdk/include/villagesql/abi/types.h"

namespace villagesql::services {

// Server-side handler. Receives the bytes the extension put in the airlock
// request and writes an error message on failure. Returns true on error.
using AirlockHandler = std::function<bool(
    const unsigned char *in_bytes, size_t in_size, std::string &error_message)>;

void register_airlock_handler(std::string name, AirlockHandler handler);
void unregister_airlock_handler(const std::string &name);

// Register all server built-in airlock handlers. Called once at server
// startup, alongside register_builtin_capabilities().
void register_builtin_airlock_handlers();

// Process every entry in reg->airlock_requests by looking up the named
// handler and invoking it with the request payload. On any failure
// (handler not registered or handler returns true), populates error_message
// and returns true.
bool populate_airlock_requests(const vef_registration_t *reg,
                               std::string &error_message);

}  // namespace villagesql::services

#endif  // VILLAGESQL_SERVICES_AIRLOCK_REGISTRY_H
