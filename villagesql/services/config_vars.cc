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

#include "villagesql/services/config_vars.h"

#include <mutex>
#include <string>
#include <vector>

#include "my_sys.h"
#include "mysql/components/my_service.h"
#include "mysql/components/services/component_sys_var_service.h"
#include "mysql/service_plugin_registry.h"
#include "villagesql/include/error.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"

namespace villagesql {
namespace services {

namespace {

// A registered config variable together with the extension it belongs to, so
// we can unregister it on extension uninstall.
struct RegisteredConfigVar {
  std::string extension_name;
  std::string var_name;
};

std::mutex g_config_vars_mutex;
std::vector<RegisteredConfigVar> g_config_vars;

}  // namespace

bool register_config_vars_from_extension(
    const std::string &extension_name,
    const veb::ExtensionRegistration &ext_reg) {
  const vef_registration_t *reg = ext_reg.registration;
  if (reg == nullptr || ext_reg.negotiated_protocol < VEF_PROTOCOL_3 ||
      reg->config_var_count == 0) {
    return false;
  }

  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry == nullptr) {
    LogVSQL(ERROR_LEVEL,
            "register_config_vars_from_extension: failed to acquire registry");
    return true;
  }

  my_service<SERVICE_TYPE(component_sys_variable_register)> reg_svc(
      "component_sys_variable_register", registry);
  if (!reg_svc.is_valid()) {
    LogVSQL(ERROR_LEVEL,
            "register_config_vars_from_extension: "
            "component_sys_variable_register service unavailable");
    mysql_plugin_registry_release(registry);
    return true;
  }

  bool error = false;
  for (unsigned int i = 0; i < reg->config_var_count; i++) {
    vef_config_var_desc_t *v = reg->config_vars[i];
    if (v == nullptr || v->name == nullptr) {
      LogVSQL(ERROR_LEVEL,
              "Extension '%s' has NULL config var descriptor at index %u",
              extension_name.c_str(), i);
      error = true;
      break;
    }

    int flags = PLUGIN_VAR_RQCMDARG;
    void *check_arg = nullptr;

    // Build the type-specific check_arg struct on the stack. The service
    // copies the values it needs before register_variable returns.
    // These macros define a struct type (e.g. INTEGRAL_CHECK_ARG(longlong)
    // defines struct longlong_check_arg_s), then we declare a variable of it.
    BOOL_CHECK_ARG(bool) bool_arg;
    INTEGRAL_CHECK_ARG(longlong) int_arg;
    INTEGRAL_CHECK_ARG(double) dbl_arg;
    STR_CHECK_ARG(str) str_arg;
    memset(&bool_arg, 0, sizeof(bool_arg));
    memset(&int_arg, 0, sizeof(int_arg));
    memset(&dbl_arg, 0, sizeof(dbl_arg));
    memset(&str_arg, 0, sizeof(str_arg));

    switch (v->type) {
      case VEF_VAR_BOOL:
        flags |= PLUGIN_VAR_BOOL;
        bool_arg.def_val = v->boolean.def_val;
        check_arg = &bool_arg;
        break;
      case VEF_VAR_INT:
        flags |= PLUGIN_VAR_LONGLONG;
        int_arg.def_val = static_cast<longlong>(v->integer.def_val);
        int_arg.min_val = static_cast<longlong>(v->integer.min_val);
        int_arg.max_val = static_cast<longlong>(v->integer.max_val);
        int_arg.blk_sz = 0;
        check_arg = &int_arg;
        break;
      case VEF_VAR_DOUBLE:
        // TODO(villagesql-beta): component_sys_variable_register does not
        // support PLUGIN_VAR_DOUBLE; register_variable will fail with
        // "Unknown variable type code 0x8". Until MySQL adds support,
        // extensions should use VEF_VAR_INT (milliseconds) instead.
        flags |= PLUGIN_VAR_DOUBLE;
        dbl_arg.def_val = v->dbl.def_val;
        dbl_arg.min_val = v->dbl.min_val;
        dbl_arg.max_val = v->dbl.max_val;
        dbl_arg.blk_sz = 0;
        check_arg = &dbl_arg;
        break;
      case VEF_VAR_STR:
        // PLUGIN_VAR_MEMALLOC tells the server to copy the string on SET,
        // which is required for the variable to be writable at runtime.
        flags |= PLUGIN_VAR_STR | PLUGIN_VAR_MEMALLOC;
        str_arg.def_val = const_cast<char *>(v->str.def_val);
        check_arg = &str_arg;
        break;
    }

    if (reg_svc->register_variable(extension_name.c_str(), v->name, flags,
                                   v->comment ? v->comment : "", nullptr,
                                   nullptr, check_arg, v->value_ptr)) {
      LogVSQL(ERROR_LEVEL,
              "Failed to register config var '%s' for extension '%s'", v->name,
              extension_name.c_str());
      error = true;
      break;
    }

    {
      std::lock_guard<std::mutex> lock(g_config_vars_mutex);
      g_config_vars.push_back({extension_name, std::string(v->name)});
    }

    LogVSQL(INFORMATION_LEVEL, "Registered config var '%s' for extension '%s'",
            v->name, extension_name.c_str());
  }

  mysql_plugin_registry_release(registry);
  return error;
}

void unregister_config_vars_from_extension(const std::string &extension_name) {
  std::vector<std::string> var_names;
  {
    std::lock_guard<std::mutex> lock(g_config_vars_mutex);
    auto it = std::remove_if(g_config_vars.begin(), g_config_vars.end(),
                             [&](const RegisteredConfigVar &v) {
                               if (v.extension_name == extension_name) {
                                 var_names.push_back(v.var_name);
                                 return true;
                               }
                               return false;
                             });
    g_config_vars.erase(it, g_config_vars.end());
  }

  if (var_names.empty()) return;

  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry == nullptr) return;

  my_service<SERVICE_TYPE(component_sys_variable_unregister)> unreg_svc(
      "component_sys_variable_unregister", registry);
  if (unreg_svc.is_valid()) {
    for (const auto &name : var_names) {
      unreg_svc->unregister_variable(extension_name.c_str(), name.c_str());
    }
  }

  mysql_plugin_registry_release(registry);
}

}  // namespace services
}  // namespace villagesql
