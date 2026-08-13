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

#include "villagesql/services/preview/session_var.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <vector>

#include "my_sys.h"
#include "mysql/components/my_service.h"
#include "mysql/components/services/component_sys_var_service.h"
#include "mysql/service_plugin_registry.h"
#include "sql/current_thd.h"
#include "sql/set_var.h"
#include "sql/sql_class.h"
#include "sql/sql_plugin_var.h"
#include "villagesql/include/error.h"
#include "villagesql/sdk/include/villagesql/abi/preview/session_var.h"

namespace villagesql::services {

// Forward declarations: session readers for the vtable, defined below.
static bool session_var_get_int(const char *component_name, const char *name,
                                long long *out);
static bool session_var_get_str(const char *component_name, const char *name,
                                void **val, size_t *val_len);

namespace {

static vef_preview_session_var_t g_session_var_vtable{
    VEF_PREVIEW_SESSION_VAR_ABI_VERSION, session_var_get_int,
    session_var_get_str};

// A registered session variable and the extension it belongs to, so we can
// unregister it on extension uninstall. capability_config is the depopulate
// key (the descriptor list pointer this var came from).
struct RegisteredSessionVar {
  std::string extension_name;
  std::string var_name;
  const void *capability_config;
};

std::mutex g_session_vars_mutex;
std::vector<RegisteredSessionVar> g_session_vars;

// Resolves the session (THD-local) value pointer for a component variable on
// the current connection thread. Returns nullptr if there is no connection
// thread, the variable does not exist, or it is not a THD-local variable. The
// component_name/name pair is resolved the same way the component get_variable
// service does: the implicit "mysql_server" component has no prefix, everything
// else is prefixed by the component name.
const uchar *session_value_ptr(const char *component_name, const char *name) {
  THD *thd = current_thd;
  if (thd == nullptr) return nullptr;

  const char *prefix =
      strcmp(component_name, "mysql_server") == 0 ? "" : component_name;

  const uchar *result = nullptr;
  auto fn = [thd, &result](const System_variable_tracker &, sys_var *var) {
    sys_var_pluginvar *pv = var->cast_pluginvar();
    if (pv == nullptr) return;
    // Only THD-local variables have a per-session value.
    if (!(pv->plugin_var->flags & PLUGIN_VAR_THDLOCAL)) return;
    result = pv->real_value_ptr(thd, OPT_SESSION);
  };
  System_variable_tracker::make_tracker(prefix, name)
      .access_system_variable(thd, fn, Suppress_not_found_error::YES);
  return result;
}

// Unregisters and forgets every variable registered so far for the given
// capability_config. Used to roll back a partially completed on_populate.
void rollback_registered_vars(SERVICE_TYPE(registry) * registry,
                              const std::string &extension_name,
                              const void *capability_config) {
  std::vector<std::string> to_unreg;
  {
    std::lock_guard<std::mutex> lock(g_session_vars_mutex);
    auto it = std::remove_if(g_session_vars.begin(), g_session_vars.end(),
                             [&](const RegisteredSessionVar &rv) {
                               if (rv.capability_config == capability_config) {
                                 to_unreg.push_back(rv.var_name);
                                 return true;
                               }
                               return false;
                             });
    g_session_vars.erase(it, g_session_vars.end());
  }
  my_service<SERVICE_TYPE(component_sys_variable_unregister)> unreg_svc(
      "component_sys_variable_unregister", registry);
  if (unreg_svc.is_valid()) {
    for (const auto &name : to_unreg)
      unreg_svc->unregister_variable(extension_name.c_str(), name.c_str());
  }
}

}  // namespace

vef_preview_session_var_t *preview_session_var_vtable() {
  return &g_session_var_vtable;
}

static bool session_var_get_int(const char *component_name, const char *name,
                                long long *out) {
  if (out == nullptr) return true;
  const uchar *p = session_value_ptr(component_name, name);
  if (p == nullptr) return true;
  // Session INT variables are registered as PLUGIN_VAR_LONGLONG (see
  // on_populate_session_var), so the per-THD storage holds a longlong.
  *out = *reinterpret_cast<const long long *>(p);
  return false;
}

static bool session_var_get_str(const char *component_name, const char *name,
                                void **val, size_t *val_len) {
  if (val == nullptr || val_len == nullptr) return true;
  const uchar *p = session_value_ptr(component_name, name);
  if (p == nullptr) return true;
  const char *s = *reinterpret_cast<const char *const *>(p);
  if (s == nullptr) return true;
  const size_t len = strlen(s);
  char *buf = static_cast<char *>(malloc(len + 1));
  if (buf == nullptr) return true;
  memcpy(buf, s, len + 1);
  *val = buf;
  *val_len = len;
  return false;
}

bool on_populate_session_var(const PopulateContext &ctx,
                             std::string &error_message) {
  const auto *list = static_cast<const vef_session_var_descriptor_list_t *>(
      ctx.capability_config);
  if (list == nullptr || list->vars == nullptr || list->var_count == 0)
    return false;
  const std::string extension_name(ctx.extension_name);

  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry == nullptr) {
    error_message = "on_populate_session_var: failed to acquire registry";
    LogVSQL(ERROR_LEVEL, "%s", error_message.c_str());
    return true;
  }

  my_service<SERVICE_TYPE(component_sys_variable_register)> reg_svc(
      "component_sys_variable_register", registry);
  if (!reg_svc.is_valid()) {
    error_message =
        "on_populate_session_var: component_sys_variable_register unavailable";
    LogVSQL(ERROR_LEVEL, "%s", error_message.c_str());
    mysql_plugin_registry_release(registry);
    return true;
  }

  for (uint32_t i = 0; i < list->var_count; i++) {
    const vef_session_var_desc_t *v = list->vars[i];
    // PLUGIN_VAR_THDLOCAL makes this a per-session variable; the server
    // allocates per-connection storage and stores an offset (no global
    // value pointer is needed, so variable_value is nullptr below).
    int flags = PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_THDLOCAL;
    void *check_arg = nullptr;

    INTEGRAL_CHECK_ARG(longlong) int_arg;
    STR_CHECK_ARG(str) str_arg;
    memset(&int_arg, 0, sizeof(int_arg));
    memset(&str_arg, 0, sizeof(str_arg));

    switch (v->type) {
      case VEF_SESSION_VAR_INT:
        flags |= PLUGIN_VAR_LONGLONG;
        int_arg.def_val = static_cast<longlong>(v->integer.def_val);
        int_arg.min_val = static_cast<longlong>(v->integer.min_val);
        int_arg.max_val = static_cast<longlong>(v->integer.max_val);
        int_arg.blk_sz = 0;
        check_arg = &int_arg;
        break;
      case VEF_SESSION_VAR_STR:
        // PLUGIN_VAR_MEMALLOC tells the server to copy the string on SET,
        // which is required for the variable to be writable at runtime.
        flags |= PLUGIN_VAR_STR | PLUGIN_VAR_MEMALLOC;
        str_arg.def_val = const_cast<char *>(v->str.def_val);
        check_arg = &str_arg;
        break;
    }

    if (reg_svc->register_variable(extension_name.c_str(), v->name, flags,
                                   v->comment ? v->comment : "", nullptr,
                                   nullptr, check_arg, nullptr)) {
      LogVSQL(ERROR_LEVEL,
              "Failed to register session variable '%s' for extension '%s'",
              v->name, extension_name.c_str());
      error_message = std::string("Failed to register session variable '") +
                      v->name + "' for extension '" + extension_name + "'";
      rollback_registered_vars(registry, extension_name, ctx.capability_config);
      mysql_plugin_registry_release(registry);
      return true;
    }

    {
      std::lock_guard<std::mutex> lock(g_session_vars_mutex);
      g_session_vars.push_back(
          {extension_name, std::string(v->name), ctx.capability_config});
    }

    LogVSQL(INFORMATION_LEVEL,
            "Registered session variable '%s' for extension '%s'", v->name,
            extension_name.c_str());
  }

  mysql_plugin_registry_release(registry);
  return false;
}

void on_depopulate_session_var(const DepopulateContext &ctx) {
  std::vector<std::string> var_names;
  std::string extension_name;
  {
    std::lock_guard<std::mutex> lock(g_session_vars_mutex);
    auto it =
        std::remove_if(g_session_vars.begin(), g_session_vars.end(),
                       [&](const RegisteredSessionVar &v) {
                         if (v.capability_config == ctx.capability_config) {
                           if (extension_name.empty())
                             extension_name = v.extension_name;
                           var_names.push_back(v.var_name);
                           return true;
                         }
                         return false;
                       });
    g_session_vars.erase(it, g_session_vars.end());
  }

  if (var_names.empty()) return;

  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry == nullptr) return;

  my_service<SERVICE_TYPE(component_sys_variable_unregister)> unreg_svc(
      "component_sys_variable_unregister", registry);
  if (unreg_svc.is_valid()) {
    for (const auto &name : var_names)
      unreg_svc->unregister_variable(extension_name.c_str(), name.c_str());
  }

  mysql_plugin_registry_release(registry);
}

}  // namespace villagesql::services
