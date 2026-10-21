/* Copyright (c) 2026 VillageSQL Contributors
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see <https://www.gnu.org/licenses/>.
 */

#ifndef VILLAGESQL_SERVICES_PLUGIN_REGISTRY_H_
#define VILLAGESQL_SERVICES_PLUGIN_REGISTRY_H_

#include "mysql/components/service.h"
#include "mysql/components/services/registry.h"
#include "mysql/service_plugin_registry.h"

namespace villagesql::services {

// Holds a reference to the server's plugin registry for the life of the
// object. Converts to the registry pointer, so it can be passed straight to a
// my_service<>. Declare it before any my_service<> built from it: locals are
// destroyed in reverse order, so those services are released first.
class PluginRegistry {
 public:
  PluginRegistry() : m_registry(mysql_plugin_registry_acquire()) {}
  ~PluginRegistry() {
    if (m_registry != nullptr) mysql_plugin_registry_release(m_registry);
  }

  PluginRegistry(const PluginRegistry &) = delete;
  PluginRegistry &operator=(const PluginRegistry &) = delete;

  bool is_valid() const { return m_registry != nullptr; }

  operator SERVICE_TYPE(registry) * () const { return m_registry; }
  SERVICE_TYPE(registry) *operator->() const { return m_registry; }

 private:
  SERVICE_TYPE(registry) * m_registry;
};

}  // namespace villagesql::services

#endif  // VILLAGESQL_SERVICES_PLUGIN_REGISTRY_H_
