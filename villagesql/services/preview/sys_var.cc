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

#include "villagesql/services/preview/sys_var.h"

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
#include "villagesql/sdk/include/villagesql/abi/preview/sys_var.h"
#include "villagesql/services/sys_var_access.h"

namespace villagesql::services {

// Forward declaration: set_variable for the vtable, defined below after
// g_sys_vars (which it needs to look up the variable type).
static bool sys_var_set(const char *component_name, const char *name,
                        const char *scope, const char *val);

namespace {

static vef_preview_sys_var_t g_sys_var_vtable{VEF_PREVIEW_SYS_VAR_ABI_VERSION,
                                              get_variable, sys_var_set};

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
  // Back-pointer to the descriptor list this var came from, used as the
  // depopulate key.
  const void *capability_config;
};

std::mutex g_sys_vars_mutex;
std::vector<RegisteredSysVar> g_sys_vars;

// Generic update trampoline used when on_change is non-null.
// Performs the default typed update (mirroring MySQL's update_func_* family)
// then calls the extension callback.
static void vef_sys_var_update_trampoline(MYSQL_THD, SYS_VAR *, void *val_ptr,
                                          const void *save) {
  vef_sys_var_on_change_func_t on_change = nullptr;
  vef_sys_var_change_t change{};
  // These copies own the strings so change.var_name and change.str_val remain
  // valid after the lock is released. A concurrent on_depopulate_sys_var may
  // erase the g_sys_vars entry (invalidating var_name), and a concurrent SET
  // may free the old string value (invalidating str_val).
  std::string var_name_copy;
  std::string str_val_copy;
  {
    std::lock_guard<std::mutex> lock(g_sys_vars_mutex);
    for (const RegisteredSysVar &v : g_sys_vars) {
      if (v.value_ptr == val_ptr) {
        on_change = v.on_change;
        var_name_copy = v.var_name;
        change.type = v.type;
        // Perform the typed update (mirroring MySQL's update_func_* family)
        // and capture the new value for the callback, all under the lock so
        // a concurrent SET cannot overwrite val_ptr before we finish.
        switch (v.type) {
          case VEF_VAR_BOOL:
            change.bool_val = *static_cast<const bool *>(save);
            *static_cast<bool *>(val_ptr) = change.bool_val;
            break;
          case VEF_VAR_INT:
            change.int_val = *static_cast<const long long *>(save);
            *static_cast<long long *>(val_ptr) = change.int_val;
            break;
          case VEF_VAR_DOUBLE:
            change.dbl_val = *static_cast<const double *>(save);
            *static_cast<double *>(val_ptr) = change.dbl_val;
            break;
          case VEF_VAR_STR:
            change.str_val = *static_cast<const char *const *>(save);
            *static_cast<const char **>(val_ptr) = change.str_val;
            if (change.str_val != nullptr) str_val_copy = change.str_val;
            break;
        }
        break;
      }
    }
  }

  change.var_name = var_name_copy.c_str();
  if (change.type == VEF_VAR_STR && change.str_val != nullptr)
    change.str_val = str_val_copy.c_str();
  // TODO(villagesql-crash): on_change points into extension code captured under
  // g_sys_vars_mutex. If UNINSTALL EXTENSION races with a concurrent SET
  // GLOBAL, on_depopulate_sys_var may erase the entry and return (allowing
  // dlclose) while this thread holds the captured on_change pointer but has
  // already released the lock. A drain mechanism analogous to statement_event's
  // g_inflight counter is needed to close this window.
  if (on_change != nullptr && change.var_name != nullptr) on_change(&change);
}

}  // namespace

vef_preview_sys_var_t *preview_sys_var_vtable() { return &g_sys_var_vtable; }

// Dispatches to the appropriate MySQL update service based on the registered
// variable type. Integer variables go through
// mysql_system_variable_update_integer; everything else goes through
// mysql_system_variable_update_string.
static bool sys_var_set(const char *component_name, const char *name,
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

bool on_populate_sys_var(const PopulateContext &ctx,
                         std::string &error_message) {
  const auto *list =
      static_cast<const vef_sys_var_descriptor_list_t *>(ctx.capability_config);
  if (list == nullptr || list->vars == nullptr || list->var_count == 0)
    return false;
  const std::string extension_name(ctx.extension_name);

  // The plugin registry and component_sys_variable_register service should
  // always be available when an extension is installed, but can be absent
  // during early startup or late shutdown if the component infrastructure has
  // not yet initialised (or has already been torn down).
  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry == nullptr) {
    error_message = "on_populate_sys_var: failed to acquire registry";
    LogVSQL(ERROR_LEVEL, "%s", error_message.c_str());
    return true;
  }

  my_service<SERVICE_TYPE(component_sys_variable_register)> reg_svc(
      "component_sys_variable_register", registry);
  if (!reg_svc.is_valid()) {
    error_message =
        "on_populate_sys_var: component_sys_variable_register unavailable";
    LogVSQL(ERROR_LEVEL, "%s", error_message.c_str());
    mysql_plugin_registry_release(registry);
    return true;
  }

  for (uint32_t i = 0; i < list->var_count; i++) {
    const vef_sys_var_desc_t *v = list->vars[i];
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
        // TODO(villagesql-general): component_sys_variable_register does not
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
      LogVSQL(ERROR_LEVEL,
              "Failed to register system variable '%s' for extension '%s'",
              v->name, extension_name.c_str());
      error_message = std::string("Failed to register system variable '") +
                      v->name + "' for extension '" + extension_name + "'";
      // Roll back any variables registered so far.
      {
        std::vector<std::string> to_unreg;
        {
          std::lock_guard<std::mutex> lock(g_sys_vars_mutex);
          auto it = std::remove_if(
              g_sys_vars.begin(), g_sys_vars.end(),
              [&](const RegisteredSysVar &rv) {
                if (rv.capability_config == ctx.capability_config) {
                  to_unreg.push_back(rv.var_name);
                  return true;
                }
                return false;
              });
          g_sys_vars.erase(it, g_sys_vars.end());
        }
        my_service<SERVICE_TYPE(component_sys_variable_unregister)> unreg_svc(
            "component_sys_variable_unregister", registry);
        if (unreg_svc.is_valid()) {
          for (const auto &name : to_unreg)
            unreg_svc->unregister_variable(extension_name.c_str(),
                                           name.c_str());
        }
      }
      mysql_plugin_registry_release(registry);
      return true;
    }

    {
      std::lock_guard<std::mutex> lock(g_sys_vars_mutex);
      g_sys_vars.push_back({extension_name, std::string(v->name), v->type,
                            value_ptr, v->on_change, ctx.capability_config});
    }

    LogVSQL(INFORMATION_LEVEL,
            "Registered system variable '%s' for extension '%s'", v->name,
            extension_name.c_str());
  }

  mysql_plugin_registry_release(registry);
  return false;
}

void on_depopulate_sys_var(const DepopulateContext &ctx) {
  std::vector<std::string> var_names;
  std::string extension_name;
  {
    std::lock_guard<std::mutex> lock(g_sys_vars_mutex);
    auto it = std::remove_if(
        g_sys_vars.begin(), g_sys_vars.end(), [&](const RegisteredSysVar &v) {
          if (v.capability_config == ctx.capability_config) {
            if (extension_name.empty()) extension_name = v.extension_name;
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
  // LOCK_system_variables_hash). Only done on explicit UNINSTALL EXTENSION
  // (ctx.thd != nullptr); on server shutdown thd is null and persisted values
  // are intentionally left intact.
  if (ctx.reason == villagesql::services::UnloadReason::kUninstall &&
      ctx.thd != nullptr) {
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
        pvc->reset_persisted_variables(ctx.thd, full_name.c_str(), true);
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

}  // namespace villagesql::services
