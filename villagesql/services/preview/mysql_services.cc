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

#include "villagesql/services/preview/mysql_services.h"

#include <mutex>
#include <unordered_map>
#include <vector>

#include "mysql/components/services/registry.h"
#include "mysql/service_plugin_registry.h"
#include "villagesql/include/error.h"
#include "villagesql/sdk/include/villagesql/abi/preview/mysql_services.h"

namespace villagesql::services {

namespace {

// What the capability acquired for one extension, so on_depopulate can release
// it. Keyed by the config pointer.
struct ServiceState {
  std::vector<my_h_service> acquired_handles;
};

// WorkerState-style keying: config pointer is stable and unique per loaded
// extension. Protected by g_mu (populate/depopulate run on the INSTALL /
// UNINSTALL thread, but a mutex keeps this robust to concurrent loads).
std::mutex g_mu;
std::unordered_map<const vef_preview_mysql_services_t *, ServiceState> g_states;

// Acquire every consumed service and write its pointer back into the extension.
// On any failure, roll back what this call already did.
//
// TODO(villagesql-general): consider whether to enforce a manifest allow-list —
// refusing any service the code consumes that is not listed in the extension's
// manifest "required_mysql_services". This is an open design decision, not a
// pending task: it's one way to constrain which services an extension may
// acquire, but may not be worth its cost (threading the manifest list through
// PopulateContext). Today any registered service can be acquired without a
// manifest declaration.
bool populate(const vef_preview_mysql_services_t *cfg, ServiceState &state,
              std::string &error_message) {
  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry == nullptr) {
    error_message = "could not acquire MySQL plugin registry";
    return true;
  }

  for (size_t i = 0; i < cfg->required_count; ++i) {
    const vef_mysql_service_required_t &req = cfg->required[i];
    if (req.service_name == nullptr || req.destination == nullptr) {
      error_message = "mysql_services: required entry has null field";
      mysql_plugin_registry_release(registry);
      return true;
    }
    my_h_service handle = nullptr;
    if (registry->acquire(req.service_name, &handle) || handle == nullptr) {
      error_message = std::string("failed to acquire MySQL service '") +
                      req.service_name + "'";
      mysql_plugin_registry_release(registry);
      return true;
    }
    *req.destination = reinterpret_cast<const void *>(handle);
    state.acquired_handles.push_back(handle);
  }

  mysql_plugin_registry_release(registry);
  return false;
}

// Release every acquired service handle. Runs at extension unload.
void depopulate(ServiceState &state) {
  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry == nullptr) return;
  for (auto h : state.acquired_handles) {
    registry->release(h);
  }
  mysql_plugin_registry_release(registry);
}

}  // namespace

bool on_populate_mysql_services(const PopulateContext &ctx,
                                std::string &error_message) {
  if (ctx.capability_config == nullptr) return false;
  const auto *cfg =
      static_cast<const vef_preview_mysql_services_t *>(ctx.capability_config);

  ServiceState state;
  if (populate(cfg, state, error_message)) {
    depopulate(state);  // roll back partial work
    return true;
  }

  std::lock_guard<std::mutex> lock(g_mu);
  g_states[cfg] = std::move(state);
  return false;
}

void on_depopulate_mysql_services(const DepopulateContext &ctx) {
  if (ctx.capability_config == nullptr) return;
  const auto *cfg =
      static_cast<const vef_preview_mysql_services_t *>(ctx.capability_config);

  ServiceState state;
  {
    std::lock_guard<std::mutex> lock(g_mu);
    auto it = g_states.find(cfg);
    if (it == g_states.end()) return;
    state = std::move(it->second);
    g_states.erase(it);
  }
  depopulate(state);
}

}  // namespace villagesql::services
