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

#include "villagesql/services/preview/status_var.h"

#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "mysql/components/my_service.h"
#include "mysql/components/services/component_status_var_service.h"
#include "mysql/service_plugin_registry.h"
#include "mysql/status_var.h"
#include "villagesql/include/error.h"
#include "villagesql/sdk/include/villagesql/abi/preview/status_var.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"

namespace villagesql::services {

namespace {

vef_preview_status_var_t g_status_var_vtable{
    VEF_PREVIEW_STATUS_VAR_ABI_VERSION};

// One registered status variable together with its SHOW_VAR storage.
// The SHOW_VAR array must remain valid until unregister_variable() is called.
struct RegisteredStatusVar {
  std::string full_name;
  SHOW_VAR show_var[2];
};

// Tracks all registered status vars for one extension load.
struct ExtensionStatusVars {
  std::string extension_name;
  std::list<std::unique_ptr<RegisteredStatusVar>> vars;
};

std::mutex g_mutex;
// Keyed by extension_data pointer so on_depopulate can look up by it.
std::unordered_map<const void *, ExtensionStatusVars> g_extensions;

}  // namespace

vef_preview_status_var_t *preview_status_var_vtable() {
  return &g_status_var_vtable;
}

bool on_populate_status_var(const PopulateContext &ctx,
                            std::string &error_message) {
  const auto *list =
      static_cast<const vef_status_var_descriptor_list_t *>(ctx.extension_data);
  if (list == nullptr || list->var_count == 0) return false;

  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry == nullptr) {
    error_message = "on_populate_status_var: failed to acquire plugin registry";
    LogVSQL(ERROR_LEVEL, "%s", error_message.c_str());
    return true;
  }

  my_service<SERVICE_TYPE(status_variable_registration)> reg_svc(
      "status_variable_registration", registry);
  if (!reg_svc.is_valid()) {
    error_message =
        "on_populate_status_var: status_variable_registration unavailable";
    LogVSQL(ERROR_LEVEL, "%s", error_message.c_str());
    mysql_plugin_registry_release(registry);
    return true;
  }

  ExtensionStatusVars entry;
  entry.extension_name = std::string(ctx.extension_name);

  for (uint32_t i = 0; i < list->var_count; ++i) {
    const vef_status_var_desc_t *v = list->vars[i];

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
                entry.extension_name.c_str(), v->name,
                static_cast<int>(v->type));
        continue;
    }

    auto rsv = std::make_unique<RegisteredStatusVar>();
    rsv->full_name = entry.extension_name + "." + v->name;
    rsv->show_var[0] = {rsv->full_name.c_str(), value_ptr, show_type,
                        SHOW_SCOPE_GLOBAL};
    rsv->show_var[1] = {nullptr, nullptr, SHOW_UNDEF, SHOW_SCOPE_GLOBAL};

    if (reg_svc->register_variable(rsv->show_var)) {
      error_message = std::string("Failed to register status variable '") +
                      rsv->full_name + "' for extension '" +
                      entry.extension_name + "'";
      LogVSQL(ERROR_LEVEL, "%s", error_message.c_str());
      // Roll back any variables registered so far.
      for (auto &already : entry.vars)
        reg_svc->unregister_variable(already->show_var);
      mysql_plugin_registry_release(registry);
      return true;
    }

    LogVSQL(INFORMATION_LEVEL,
            "Registered status variable '%s' for extension '%s'", v->name,
            entry.extension_name.c_str());
    entry.vars.push_back(std::move(rsv));
  }

  mysql_plugin_registry_release(registry);

  std::lock_guard<std::mutex> lock(g_mutex);
  g_extensions.emplace(ctx.extension_data, std::move(entry));
  return false;
}

void on_depopulate_status_var(const DepopulateContext &ctx) {
  std::vector<std::unique_ptr<RegisteredStatusVar>> to_unregister;

  {
    std::lock_guard<std::mutex> lock(g_mutex);
    auto it = g_extensions.find(ctx.extension_data);
    if (it == g_extensions.end()) return;
    // Move vars out so we can unregister without holding the lock.
    for (auto &v : it->second.vars) to_unregister.push_back(std::move(v));
    g_extensions.erase(it);
  }

  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry != nullptr) {
    my_service<SERVICE_TYPE(status_variable_registration)> reg_svc(
        "status_variable_registration", registry);
    if (reg_svc.is_valid()) {
      for (auto &rsv : to_unregister)
        reg_svc->unregister_variable(rsv->show_var);
    }
    mysql_plugin_registry_release(registry);
  }
}

}  // namespace villagesql::services
