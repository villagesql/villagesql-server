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

#include "villagesql/services/sys_var_access.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <vector>

#include "my_compiler.h"
#include "mysql/components/my_service.h"
#include "mysql/components/services/component_sys_var_service.h"
#include "mysql/components/services/mysql_string.h"
#include "mysql/components/services/mysql_system_variable.h"
#include "mysql/service_plugin_registry.h"
#include "villagesql/include/error.h"

namespace villagesql::services {

namespace {

struct OneVarEntry {
  std::string extension_name;
  std::string var_name;
  void *value_ptr;
  void (*on_change)(const char *var_name, bool new_val);
};

std::mutex g_one_vars_mutex;
std::vector<OneVarEntry> g_one_vars;

static void one_var_update_trampoline(MYSQL_THD, SYS_VAR *, void *val_ptr,
                                      const void *save) {
  void (*on_change)(const char *, bool) = nullptr;
  std::string var_name_copy;
  bool new_val = false;
  {
    std::lock_guard<std::mutex> lock(g_one_vars_mutex);
    for (OneVarEntry &v : g_one_vars) {
      if (v.value_ptr == val_ptr) {
        on_change = v.on_change;
        var_name_copy = v.var_name;
        new_val = *static_cast<const bool *>(save);
        *static_cast<bool *>(val_ptr) = new_val;
        break;
      }
    }
  }
  if (on_change != nullptr) on_change(var_name_copy.c_str(), new_val);
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
    // get_variable requires a caller-supplied buffer. Use a stack buffer to
    // read the value, then malloc a copy to hand ownership to the caller.
    char buf[1024];
    void *ptr = buf;
    size_t len = sizeof(buf) - 1;
    // TODO(villagesql-rebase): component_sys_variable_register::get_variable is
    // deprecated in favour of mysql_system_variable_reader::get(), which needs
    // a THD and an explicit variable scope. Migrate once we can test the new
    // service; until then suppress the deprecation so -Werror builds pass.
    MY_COMPILER_DIAGNOSTIC_PUSH()
    MY_COMPILER_CLANG_DIAGNOSTIC_IGNORE("-Wdeprecated-declarations")
    MY_COMPILER_GCC_DIAGNOSTIC_IGNORE("-Wdeprecated-declarations")
    result = svc->get_variable(component_name, name, &ptr, &len);
    MY_COMPILER_DIAGNOSTIC_POP()
    if (!result) {
      char *copy = static_cast<char *>(malloc(len + 1));
      if (copy == nullptr) {
        result = true;
      } else {
        memcpy(copy, ptr, len);
        copy[len] = '\0';
        if (ptr != static_cast<void *>(buf)) free(ptr);
        *val = copy;
        *val_len = len;
      }
    }
  }
  mysql_plugin_registry_release(registry);
  return result;
}

bool set_variable(const char *component_name, const char *name,
                  const char *scope, const char *val) {
  if (val == nullptr) return true;

  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry == nullptr) return true;

  my_service<SERVICE_TYPE(mysql_string_factory)> str_factory(
      "mysql_string_factory", registry);
  my_service<SERVICE_TYPE(mysql_string_converter)> str_conv(
      "mysql_string_converter", registry);
  my_service<SERVICE_TYPE(mysql_system_variable_update_string)> update_svc(
      "mysql_system_variable_update_string", registry);

  bool result = true;
  if (str_factory.is_valid() && str_conv.is_valid() && update_svc.is_valid()) {
    my_h_string h_base = nullptr;
    my_h_string h_name = nullptr;
    my_h_string h_value = nullptr;
    if (!str_conv->convert_from_buffer(&h_base, component_name,
                                       strlen(component_name), "utf8mb3") &&
        !str_conv->convert_from_buffer(&h_name, name, strlen(name),
                                       "utf8mb3") &&
        !str_conv->convert_from_buffer(&h_value, val, strlen(val), "utf8mb3")) {
      result = update_svc->set(nullptr, scope, h_base, h_name, h_value);
    }
    if (h_base) str_factory->destroy(h_base);
    if (h_name) str_factory->destroy(h_name);
    if (h_value) str_factory->destroy(h_value);
  }

  mysql_plugin_registry_release(registry);
  return result;
}

bool register_one_sys_var(std::string_view extension_name,
                          const SysVarDesc &desc) {
  const std::string ext_name(extension_name);
  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry == nullptr) {
    LogVSQL(ERROR_LEVEL, "register_one_sys_var: failed to acquire registry");
    return true;
  }

  my_service<SERVICE_TYPE(component_sys_variable_register)> reg_svc(
      "component_sys_variable_register", registry);
  if (!reg_svc.is_valid()) {
    LogVSQL(
        ERROR_LEVEL,
        "register_one_sys_var: component_sys_variable_register unavailable");
    mysql_plugin_registry_release(registry);
    return true;
  }

  BOOL_CHECK_ARG(bool) bool_arg;
  memset(&bool_arg, 0, sizeof(bool_arg));
  bool_arg.def_val = desc.def_val;

  mysql_sys_var_update_func update_fn =
      desc.on_change != nullptr ? one_var_update_trampoline : nullptr;

  if (reg_svc->register_variable(ext_name.c_str(), desc.name,
                                 PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_BOOL,
                                 desc.comment ? desc.comment : "", nullptr,
                                 update_fn, &bool_arg, desc.value_ptr)) {
    LogVSQL(ERROR_LEVEL, "Failed to register system variable '%s' for '%s'",
            desc.name, ext_name.c_str());
    mysql_plugin_registry_release(registry);
    return true;
  }

  {
    std::lock_guard<std::mutex> lock(g_one_vars_mutex);
    g_one_vars.push_back(
        {ext_name, std::string(desc.name), desc.value_ptr, desc.on_change});
  }

  mysql_plugin_registry_release(registry);
  return false;
}

void unregister_one_sys_var(std::string_view extension_name,
                            const char *var_name) {
  const std::string ext_name(extension_name);
  {
    std::lock_guard<std::mutex> lock(g_one_vars_mutex);
    auto &vec = g_one_vars;
    vec.erase(std::remove_if(vec.begin(), vec.end(),
                             [&](const OneVarEntry &e) {
                               return e.extension_name == ext_name &&
                                      e.var_name == var_name;
                             }),
              vec.end());
  }

  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry == nullptr) return;

  my_service<SERVICE_TYPE(component_sys_variable_unregister)> unreg_svc(
      "component_sys_variable_unregister", registry);
  if (unreg_svc.is_valid()) {
    unreg_svc->unregister_variable(ext_name.c_str(), var_name);
  }
  mysql_plugin_registry_release(registry);
}

}  // namespace villagesql::services
