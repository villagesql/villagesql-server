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

#include "villagesql/services/sys_vars.h"

#include <algorithm>
#include <cstdlib>
#include <mutex>
#include <string>
#include <vector>

#include "my_sys.h"
#include "mysql/components/my_service.h"
#include "mysql/components/services/component_sys_var_service.h"
#include "mysql/components/services/mysql_string.h"
#include "mysql/components/services/mysql_system_variable.h"
#include "mysql/service_plugin_registry.h"
#include "sql/persisted_variable.h"
#include "villagesql/include/error.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"

namespace villagesql {
namespace services {

namespace {

// A registered system variable together with the extension it belongs to, so
// we can unregister it on extension uninstall, and with its type so
// set_variable can dispatch to the correct MySQL update service.
struct RegisteredSysVar {
  std::string extension_name;
  std::string var_name;
  vef_var_type_t type;
  // Storage pointer for this variable (same value passed to register_variable
  // as variable_value). Used to look up on_change from the update trampoline.
  void *value_ptr;
  vef_sys_var_on_change_func_t on_change;
};

std::mutex g_sys_vars_mutex;
std::vector<RegisteredSysVar> g_sys_vars;

// Generic update trampoline used when on_change is non-null.
// Performs the default memcpy update then calls the extension callback.
static void vef_sys_var_update_trampoline(MYSQL_THD, SYS_VAR *, void *val_ptr,
                                          const void *save) {
  // The default update for all variable types is a pointer-sized memcpy.
  memcpy(val_ptr, save, sizeof(void *));

  vef_sys_var_on_change_func_t on_change = nullptr;
  vef_sys_var_change_t change{};
  {
    std::lock_guard<std::mutex> lock(g_sys_vars_mutex);
    for (const RegisteredSysVar &v : g_sys_vars) {
      if (v.value_ptr == val_ptr) {
        on_change = v.on_change;
        change.var_name = v.var_name.c_str();
        change.type = v.type;
        // Capture the committed value from `save` while still under the lock
        // so a concurrent SET cannot overwrite val_ptr before we read it.
        switch (v.type) {
          case VEF_VAR_BOOL:
            change.bool_val = *static_cast<const bool *>(save);
            break;
          case VEF_VAR_INT:
            change.int_val = *static_cast<const long long *>(save);
            break;
          case VEF_VAR_DOUBLE:
            change.dbl_val = *static_cast<const double *>(save);
            break;
          case VEF_VAR_STR:
            change.str_val = *static_cast<const char *const *>(save);
            break;
        }
        break;
      }
    }
  }

  if (on_change != nullptr && change.var_name != nullptr) on_change(&change);
}

}  // namespace

bool get_variable(const char *component_name, const char *name, void **val,
                  size_t *val_len) {
  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry == nullptr) return true;

  my_service<SERVICE_TYPE(component_sys_variable_register)> svc(
      "component_sys_variable_register", registry);
  bool result = true;
  if (svc.is_valid()) {
    result = svc->get_variable(component_name, name, val, val_len);
  }
  mysql_plugin_registry_release(registry);
  return result;
}

// Dispatches to the appropriate MySQL update service based on the registered
// variable type. String and bool variables go through
// mysql_system_variable_update_string; integer variables go through
// mysql_system_variable_update_integer (set_signed). All services handle
// locking and support GLOBAL, PERSIST, and PERSIST_ONLY scopes.
bool set_variable(const char *component_name, const char *name,
                  const char *scope, const char *val) {
  if (val == nullptr) return true;

  // Look up the variable type so we can pick the right update service.
  vef_var_type_t var_type = VEF_VAR_STR;
  {
    std::lock_guard<std::mutex> lock(g_sys_vars_mutex);
    bool found = false;
    for (const RegisteredSysVar &v : g_sys_vars) {
      if (v.extension_name == component_name && v.var_name == name) {
        var_type = v.type;
        found = true;
        break;
      }
    }
    if (!found) return true;
  }

  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry == nullptr) return true;

  bool result = true;

  if (var_type == VEF_VAR_INT) {
    my_service<SERVICE_TYPE(mysql_string_factory)> str_factory(
        "mysql_string_factory", registry);
    my_service<SERVICE_TYPE(mysql_string_converter)> str_conv(
        "mysql_string_converter", registry);
    my_service<SERVICE_TYPE(mysql_system_variable_update_integer)> update_svc(
        "mysql_system_variable_update_integer", registry);

    if (str_factory.is_valid() && str_conv.is_valid() &&
        update_svc.is_valid()) {
      my_h_string h_base = nullptr;
      my_h_string h_name = nullptr;

      if (!str_conv->convert_from_buffer(&h_base, component_name,
                                         strlen(component_name), "utf8mb3") &&
          !str_conv->convert_from_buffer(&h_name, name, strlen(name),
                                         "utf8mb3")) {
        result = update_svc->set_signed(nullptr, scope, h_base, h_name,
                                        strtoll(val, nullptr, 10));
      }

      if (h_base) str_factory->destroy(h_base);
      if (h_name) str_factory->destroy(h_name);
    }
  } else {
    // VEF_VAR_STR, VEF_VAR_BOOL, VEF_VAR_DOUBLE all go through string update.
    my_service<SERVICE_TYPE(mysql_string_factory)> str_factory(
        "mysql_string_factory", registry);
    my_service<SERVICE_TYPE(mysql_string_converter)> str_conv(
        "mysql_string_converter", registry);
    my_service<SERVICE_TYPE(mysql_system_variable_update_string)> update_svc(
        "mysql_system_variable_update_string", registry);

    if (str_factory.is_valid() && str_conv.is_valid() &&
        update_svc.is_valid()) {
      my_h_string h_base = nullptr;
      my_h_string h_name = nullptr;
      my_h_string h_value = nullptr;

      if (!str_conv->convert_from_buffer(&h_base, component_name,
                                         strlen(component_name), "utf8mb3") &&
          !str_conv->convert_from_buffer(&h_name, name, strlen(name),
                                         "utf8mb3") &&
          !str_conv->convert_from_buffer(&h_value, val, strlen(val),
                                         "utf8mb3")) {
        result = update_svc->set(nullptr, scope, h_base, h_name, h_value);
      }

      if (h_base) str_factory->destroy(h_base);
      if (h_name) str_factory->destroy(h_name);
      if (h_value) str_factory->destroy(h_value);
    }
  }

  mysql_plugin_registry_release(registry);
  return result;
}

bool register_sys_vars_from_extension(
    const std::string &extension_name,
    const veb::ExtensionRegistration &ext_reg) {
  const vef_registration_t *reg = ext_reg.registration;
  if (reg == nullptr || ext_reg.negotiated_protocol < VEF_PROTOCOL_2 ||
      reg->sys_var_count == 0) {
    return false;
  }

  // The plugin registry and component_sys_variable_register service should
  // always be available when an extension is installed, but can be absent
  // during early startup or late shutdown if the component infrastructure has
  // not yet initialised (or has already been torn down).
  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry == nullptr) {
    LogVSQL(ERROR_LEVEL,
            "register_sys_vars_from_extension: failed to acquire registry");
    return true;
  }

  my_service<SERVICE_TYPE(component_sys_variable_register)> reg_svc(
      "component_sys_variable_register", registry);
  if (!reg_svc.is_valid()) {
    LogVSQL(ERROR_LEVEL,
            "register_sys_vars_from_extension: "
            "component_sys_variable_register service unavailable");
    mysql_plugin_registry_release(registry);
    return true;
  }

  bool error = false;
  for (unsigned int i = 0; i < reg->sys_var_count; i++) {
    vef_sys_var_desc_t *v = reg->sys_vars[i];
    int flags = PLUGIN_VAR_RQCMDARG;
    void *check_arg = nullptr;
    void *value_ptr = nullptr;

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
        value_ptr = v->boolean.value_ptr;
        break;
      case VEF_VAR_INT:
        flags |= PLUGIN_VAR_LONGLONG;
        int_arg.def_val = static_cast<longlong>(v->integer.def_val);
        int_arg.min_val = static_cast<longlong>(v->integer.min_val);
        int_arg.max_val = static_cast<longlong>(v->integer.max_val);
        int_arg.blk_sz = 0;
        check_arg = &int_arg;
        value_ptr = v->integer.value_ptr;
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
        value_ptr = v->dbl.value_ptr;
        break;
      case VEF_VAR_STR:
        // PLUGIN_VAR_MEMALLOC tells the server to copy the string on SET,
        // which is required for the variable to be writable at runtime.
        flags |= PLUGIN_VAR_STR | PLUGIN_VAR_MEMALLOC;
        str_arg.def_val = const_cast<char *>(v->str.def_val);
        check_arg = &str_arg;
        value_ptr = v->str.value_ptr;
        break;
    }

    mysql_sys_var_update_func update_fn =
        v->on_change != nullptr ? vef_sys_var_update_trampoline : nullptr;

    if (reg_svc->register_variable(extension_name.c_str(), v->name, flags,
                                   v->comment ? v->comment : "", nullptr,
                                   update_fn, check_arg, value_ptr)) {
      LogVSQL(ERROR_LEVEL, "Failed to register system variable '%s' for extension '%s'",
              v->name, extension_name.c_str());
      error = true;
      break;
    }

    {
      std::lock_guard<std::mutex> lock(g_sys_vars_mutex);
      g_sys_vars.push_back({extension_name, std::string(v->name), v->type,
                            value_ptr, v->on_change});
    }

    LogVSQL(INFORMATION_LEVEL, "Registered system variable '%s' for extension '%s'",
            v->name, extension_name.c_str());
  }

  mysql_plugin_registry_release(registry);
  return error;
}

void unregister_sys_vars_from_extension(const std::string &extension_name,
                                        THD *thd) {
  std::vector<std::string> var_names;
  {
    std::lock_guard<std::mutex> lock(g_sys_vars_mutex);
    auto it = std::remove_if(g_sys_vars.begin(), g_sys_vars.end(),
                             [&](const RegisteredSysVar &v) {
                               if (v.extension_name == extension_name) {
                                 var_names.push_back(v.var_name);
                                 return true;
                               }
                               return false;
                             });
    g_sys_vars.erase(it, g_sys_vars.end());
  }

  if (var_names.empty()) return;

  // Remove persisted values from mysqld-auto.cnf. This is done before
  // unregister_variable so that the variable name is still resolvable in
  // reset_persisted_variables (it looks up the alias via
  // LOCK_system_variables_hash). Only done on explicit UNINSTALL EXTENSION (thd
  // != nullptr); on server shutdown thd is null and persisted values are
  // intentionally left intact.
  if (thd != nullptr) {
    Persisted_variables_cache *pvc = Persisted_variables_cache::get_instance();
    if (pvc != nullptr) {
      // Get the set of persisted plugin/component variables once. We check
      // membership before calling reset_persisted_variables to avoid the
      // warning that MySQL emits when the variable is not in the persisted
      // config file (reset_persisted_variables with if_exists=true warns
      // rather than errors, but we want complete silence for variables that
      // were never persisted).
      Persisted_variables_uset *plugin_vars =
          pvc->get_persisted_dynamic_plugin_variables();
      for (const auto &name : var_names) {
        std::string full_name = extension_name + "." + name;
        if (plugin_vars != nullptr) {
          auto it = std::find_if(plugin_vars->begin(), plugin_vars->end(),
                                 [&full_name](const st_persist_var &v) {
                                   return v.key == full_name;
                                 });
          if (it == plugin_vars->end()) continue;
        }
        pvc->reset_persisted_variables(thd, full_name.c_str(), true);
      }
    }
  }

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
