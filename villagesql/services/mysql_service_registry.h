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

#ifndef VILLAGESQL_SERVICES_MYSQL_SERVICE_REGISTRY_H
#define VILLAGESQL_SERVICES_MYSQL_SERVICE_REGISTRY_H

// Server-side half of the MySQL Services airlock bridge.
//
// During extension load, the airlock handlers do the acquire / register
// work directly using `mysql_plugin_registry_acquire()`, write any results
// through pointers the extension supplied in the request payload, and track
// acquired handles + registered impls on a per-extension load context so
// the server can release / unregister them at unload.

#include <string>
#include <unordered_set>
#include <vector>

#include "mysql/components/services/registry.h"

namespace villagesql::services {

// What the extension declared in its manifest. Provided through
// scoped_set_load_context() for the duration of populate_airlock_requests().
// Enforcement: a required-service airlock request whose name is not in
// `required_services` fails. Same for provided.
struct ExtensionManifestServices {
  std::unordered_set<std::string> required_services;
  std::unordered_set<std::string> provided_services;
};

// What the bridge acquired / registered on behalf of an extension. Stored
// on the ExtensionRegistration so unload_vef_extension can tear down.
struct ExtensionAirlockState {
  // Each entry is a handle returned by registry->acquire(); release it
  // with registry->release().
  std::vector<my_h_service> acquired_handles;
  // Service names (no impl suffix) for each entry in acquired_handles, so
  // we can decide which need release-before-dlclose vs after.
  std::vector<std::string> acquired_service_names;
  // Each entry is a full "service_name.implementation_name" string.
  std::vector<std::string> registered_impls;
};

struct LoadContext {
  const ExtensionManifestServices *manifest;
  ExtensionAirlockState *state;
};

// RAII guard: sets the thread-local load context for the duration of
// populate_airlock_requests(). The pointer must remain valid for the guard's
// lifetime.
class ScopedLoadContext {
 public:
  explicit ScopedLoadContext(const LoadContext *ctx);
  ~ScopedLoadContext();
  ScopedLoadContext(const ScopedLoadContext &) = delete;
  ScopedLoadContext &operator=(const ScopedLoadContext &) = delete;

 private:
  const LoadContext *previous_;
};

// Registers both the required/v1 and provided/v1 airlock channel handlers.
void register_mysql_service_airlock_handlers();

// Teardown phases. Caller invokes in this order:
//   1. release_self_consumed_handles(state)  — releases any handle whose
//      service name is the prefix of one of `state.registered_impls`.
//   2. unregister_impls(state)               — unregisters every entry in
//      `state.registered_impls`. Must precede dlclose.
//   --- caller calls dlclose here ---
//   3. release_remaining_handles(state)      — releases handles not already
//      released in step 1.

void release_self_consumed_handles(ExtensionAirlockState &state);
void unregister_impls(ExtensionAirlockState &state);
void release_remaining_handles(ExtensionAirlockState &state);

}  // namespace villagesql::services

#endif  // VILLAGESQL_SERVICES_MYSQL_SERVICE_REGISTRY_H
