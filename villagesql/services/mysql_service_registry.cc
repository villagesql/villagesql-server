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

#include "villagesql/services/mysql_service_registry.h"

#include <string_view>

#include "mysql/components/my_service.h"
#include "mysql/components/services/registry.h"
#include "mysql/service_plugin_registry.h"
#include "villagesql/include/error.h"
#include "villagesql/mysql_services/include/vsql/mysql_services/registry_abi.h"
#include "villagesql/services/airlock_registry.h"

namespace villagesql::services {

namespace {

thread_local const LoadContext *t_load_ctx = nullptr;

// Required-service handler: read payload, check manifest, acquire from MySQL
// registry, write through destination pointer, record handle for teardown.
bool handle_required_v1(const unsigned char *in_bytes, size_t in_size,
                        std::string &error_message) {
  if (in_size < sizeof(vsql_mysql_service_required_v1_t)) {
    error_message = "required/v1 payload too small";
    return true;
  }
  const auto *req =
      reinterpret_cast<const vsql_mysql_service_required_v1_t *>(in_bytes);
  if (req->service_name == nullptr || req->destination == nullptr) {
    error_message = "required/v1 payload has null field";
    return true;
  }

  if (t_load_ctx == nullptr || t_load_ctx->manifest == nullptr ||
      t_load_ctx->state == nullptr) {
    error_message = "load context not set";
    return true;
  }

  const std::string name(req->service_name);
  if (t_load_ctx->manifest->required_services.count(name) == 0) {
    error_message = "service '" + name +
                    "' is requested by code but not declared in manifest "
                    "'required_mysql_services'";
    return true;
  }

  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry == nullptr) {
    error_message = "could not acquire MySQL plugin registry";
    return true;
  }
  my_h_service handle = nullptr;
  const bool acq_err = registry->acquire(req->service_name, &handle);
  mysql_plugin_registry_release(registry);
  if (acq_err || handle == nullptr) {
    error_message = "failed to acquire MySQL service '" + name + "'";
    return true;
  }

  *req->destination = reinterpret_cast<const void *>(handle);
  t_load_ctx->state->acquired_handles.push_back(handle);
  t_load_ctx->state->acquired_service_names.push_back(name);
  return false;
}

// Provided-service handler: read payload, check manifest, register impl,
// record full name for teardown.
bool handle_provided_v1(const unsigned char *in_bytes, size_t in_size,
                        std::string &error_message) {
  if (in_size < sizeof(vsql_mysql_service_provided_v1_t)) {
    error_message = "provided/v1 payload too small";
    return true;
  }
  const auto *req =
      reinterpret_cast<const vsql_mysql_service_provided_v1_t *>(in_bytes);
  if (req->full_name == nullptr || req->impl == nullptr) {
    error_message = "provided/v1 payload has null field";
    return true;
  }

  if (t_load_ctx == nullptr || t_load_ctx->manifest == nullptr ||
      t_load_ctx->state == nullptr) {
    error_message = "load context not set";
    return true;
  }

  const std::string full_name(req->full_name);
  if (t_load_ctx->manifest->provided_services.count(full_name) == 0) {
    error_message = "service implementation '" + full_name +
                    "' is provided by code but not declared in manifest "
                    "'provided_mysql_services'";
    return true;
  }

  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry == nullptr) {
    error_message = "could not acquire MySQL plugin registry";
    return true;
  }
  my_service<SERVICE_TYPE(registry_registration)> reg("registry_registration",
                                                      registry);
  bool err = true;
  if (reg.is_valid()) {
    err = reg->register_service(
        req->full_name,
        reinterpret_cast<my_h_service>(const_cast<void *>(req->impl)));
  }
  mysql_plugin_registry_release(registry);

  if (err) {
    error_message =
        "failed to register MySQL service implementation '" + full_name + "'";
    return true;
  }

  t_load_ctx->state->registered_impls.push_back(full_name);
  return false;
}

// Extract the service-name prefix from a full name "service_name.impl".
std::string service_prefix(std::string_view full_name) {
  const auto dot = full_name.find('.');
  if (dot == std::string_view::npos) return std::string(full_name);
  return std::string(full_name.substr(0, dot));
}

}  // namespace

ScopedLoadContext::ScopedLoadContext(const LoadContext *ctx)
    : previous_(t_load_ctx) {
  t_load_ctx = ctx;
}

ScopedLoadContext::~ScopedLoadContext() { t_load_ctx = previous_; }

void register_mysql_service_airlock_handlers() {
  register_airlock_handler(VSQL_MYSQL_SERVICE_REQUIRED_V1_CHANNEL,
                           &handle_required_v1);
  register_airlock_handler(VSQL_MYSQL_SERVICE_PROVIDED_V1_CHANNEL,
                           &handle_provided_v1);
}

// Teardown phases (see header for the required call order).

void release_self_consumed_handles(ExtensionAirlockState &state) {
  if (state.acquired_handles.empty() || state.registered_impls.empty()) return;

  std::unordered_set<std::string> our_provided_prefixes;
  for (const auto &full : state.registered_impls) {
    our_provided_prefixes.insert(service_prefix(full));
  }

  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry == nullptr) return;

  for (size_t i = 0; i < state.acquired_handles.size(); /* manual */) {
    if (our_provided_prefixes.count(state.acquired_service_names[i]) > 0) {
      registry->release(state.acquired_handles[i]);
      state.acquired_handles.erase(state.acquired_handles.begin() + i);
      state.acquired_service_names.erase(state.acquired_service_names.begin() +
                                         i);
    } else {
      ++i;
    }
  }

  mysql_plugin_registry_release(registry);
}

void unregister_impls(ExtensionAirlockState &state) {
  if (state.registered_impls.empty()) return;
  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry == nullptr) return;
  my_service<SERVICE_TYPE(registry_registration)> reg("registry_registration",
                                                      registry);
  if (reg.is_valid()) {
    for (auto it = state.registered_impls.rbegin();
         it != state.registered_impls.rend(); ++it) {
      if (reg->unregister(it->c_str())) {
        LogVSQL(WARNING_LEVEL,
                "Failed to unregister MySQL service implementation '%s' — "
                "outstanding references may dangle after extension unload",
                it->c_str());
      }
    }
  }
  state.registered_impls.clear();
  mysql_plugin_registry_release(registry);
}

void release_remaining_handles(ExtensionAirlockState &state) {
  if (state.acquired_handles.empty()) return;
  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry == nullptr) return;
  for (auto h : state.acquired_handles) {
    registry->release(h);
  }
  state.acquired_handles.clear();
  state.acquired_service_names.clear();
  mysql_plugin_registry_release(registry);
}

}  // namespace villagesql::services
