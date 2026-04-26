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

#include "villagesql/services/status_vars.h"

#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "mysql/components/my_service.h"
#include "mysql/components/services/component_status_var_service.h"
#include "mysql/service_plugin_registry.h"
#include "mysql/status_var.h"
#include "villagesql/include/error.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"

namespace villagesql {
namespace services {

namespace {

// One registered status variable together with its SHOW_VAR storage and the
// extension it belongs to. The SHOW_VAR array must remain valid until
// unregister_variable() is called, so we store it inline here.
struct RegisteredStatusVar {
  std::string extension_name;
  // Full "extension.varname" string used as the SHOW_VAR name and as the key
  // for unregistration.
  std::string full_name;
  // show_var[0] = the variable; show_var[1] = sentinel.
  SHOW_VAR show_var[2];
};

std::mutex g_status_vars_mutex;
// unique_ptr elements in std::list: heap allocation gives stable addresses for
// full_name (used as SHOW_VAR.name by MySQL) and show_var (passed to
// register_variable/unregister_variable), regardless of list operations.
std::list<std::unique_ptr<RegisteredStatusVar>> g_status_vars;

}  // namespace

bool register_status_vars_from_extension(
    const std::string &extension_name,
    const veb::ExtensionRegistration &ext_reg) {
  const vef_registration_t *reg = ext_reg.registration;
  if (reg == nullptr || ext_reg.negotiated_protocol < VEF_PROTOCOL_2 ||
      reg->status_var_count == 0) {
    return false;
  }

  // The plugin registry and status_variable_registration service should always
  // be available when an extension is installed, but can be absent during
  // early startup or late shutdown if the component infrastructure has not yet
  // initialised (or has already been torn down).
  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry == nullptr) {
    LogVSQL(ERROR_LEVEL,
            "register_status_vars_from_extension: failed to acquire registry");
    return true;
  }

  my_service<SERVICE_TYPE(status_variable_registration)> reg_svc(
      "status_variable_registration", registry);
  if (!reg_svc.is_valid()) {
    LogVSQL(ERROR_LEVEL,
            "register_status_vars_from_extension: "
            "status_variable_registration service unavailable");
    mysql_plugin_registry_release(registry);
    return true;
  }

  bool error = false;
  for (unsigned int i = 0; i < reg->status_var_count; i++) {
    vef_status_var_desc_t *v = reg->status_vars[i];

    enum_mysql_show_type show_type;
    char *value_ptr;
    switch (v->type) {
      case VEF_STATUS_VAR_INT:
        show_type = SHOW_LONGLONG;
        value_ptr = reinterpret_cast<char *>(v->integer_ptr);
        break;
      case VEF_STATUS_VAR_DOUBLE:
        show_type = SHOW_DOUBLE;
        value_ptr = reinterpret_cast<char *>(v->double_ptr);
        break;
      default:
        LogVSQL(ERROR_LEVEL,
                "Extension '%s' status variable '%s' has unknown type %d",
                extension_name.c_str(), v->name, static_cast<int>(v->type));
        error = true;
        break;
    }
    if (error) break;

    // Build the full struct on the heap before locking, so that full_name
    // (used as SHOW_VAR.name) is at a stable address. show_var[0].name points
    // into full_name, which remains valid for MySQL's use after
    // register_variable() returns.
    auto rsv = std::make_unique<RegisteredStatusVar>();
    rsv->extension_name = extension_name;
    rsv->full_name = extension_name + "." + v->name;
    rsv->show_var[0] = {rsv->full_name.c_str(), value_ptr, show_type,
                        SHOW_SCOPE_GLOBAL};
    rsv->show_var[1] = {nullptr, nullptr, SHOW_UNDEF, SHOW_SCOPE_GLOBAL};

    RegisteredStatusVar *stored = nullptr;
    {
      std::lock_guard<std::mutex> lock(g_status_vars_mutex);
      g_status_vars.push_back(std::move(rsv));
      stored = g_status_vars.back().get();
    }

    if (reg_svc->register_variable(stored->show_var)) {
      LogVSQL(ERROR_LEVEL,
              "Failed to register status variable '%s' for extension '%s'",
              stored->full_name.c_str(), extension_name.c_str());
      std::lock_guard<std::mutex> lock(g_status_vars_mutex);
      // Remove the entry we just added.
      for (auto it = g_status_vars.begin(); it != g_status_vars.end(); ++it) {
        if (it->get() == stored) {
          g_status_vars.erase(it);
          break;
        }
      }
      error = true;
      break;
    }

    LogVSQL(INFORMATION_LEVEL,
            "Registered status variable '%s' for extension '%s'", v->name,
            extension_name.c_str());
  }

  mysql_plugin_registry_release(registry);
  return error;
}

void unregister_status_vars_from_extension(const std::string &extension_name) {
  // Collect iterators to the entries to remove — but do NOT erase yet.
  // The SHOW_VAR.name pointers point into RegisteredStatusVar::full_name.
  // Those must remain valid until after unregister_variable() returns,
  // since MySQL may still be reading the list during the call.
  std::vector<std::list<std::unique_ptr<RegisteredStatusVar>>::iterator>
      to_remove;
  {
    std::lock_guard<std::mutex> lock(g_status_vars_mutex);
    for (auto it = g_status_vars.begin(); it != g_status_vars.end(); ++it) {
      if ((*it)->extension_name == extension_name) to_remove.push_back(it);
    }
  }

  if (to_remove.empty()) return;

  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry != nullptr) {
    my_service<SERVICE_TYPE(status_variable_registration)> reg_svc(
        "status_variable_registration", registry);
    if (reg_svc.is_valid()) {
      for (auto it : to_remove) {
        // Pass the original SHOW_VAR[2] array — name pointer is still valid.
        reg_svc->unregister_variable((*it)->show_var);
      }
    }
    mysql_plugin_registry_release(registry);
  }

  // Now it is safe to free the entries: MySQL has removed its references.
  {
    std::lock_guard<std::mutex> lock(g_status_vars_mutex);
    for (auto it : to_remove) g_status_vars.erase(it);
  }
}

}  // namespace services
}  // namespace villagesql
