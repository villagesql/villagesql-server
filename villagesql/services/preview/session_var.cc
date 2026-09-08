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
#include <new>
#include <string>
#include <vector>

#include "my_sys.h"
#include "mysql/components/my_service.h"
#include "mysql/components/services/component_sys_var_service.h"
#include "mysql/components/services/mysql_system_variable.h"
#include "mysql/service_plugin_registry.h"
#include "sql/current_thd.h"
#include "sql/set_var.h"
#include "sql/sql_class.h"
#include "sql/sql_plugin_var.h"
#include "villagesql/include/error.h"
#include "villagesql/sdk/include/villagesql/abi/preview/session_var.h"

// Definition of the ABI's opaque handle (forward-declared in
// abi/preview/session_var.h at global scope). Holds only the per-session
// storage offset, which is identical on every connection and stable for the
// life of the registration, so session_var_read_int can resolve any connection
// thread's value from it with a lock-free offset load.
struct vef_session_var_handle {
  int offset;
};

namespace villagesql::services {

// Forward declarations: session readers for the vtable, defined below. The
// `cap` argument is the calling extension's descriptor-list pointer (the
// capability_config token), which the server maps to the extension name; the
// extension never names itself.
static bool session_var_get_int(const void *cap, const char *name,
                                long long *out);
static bool session_var_get_str(const void *cap, const char *name, void **val,
                                size_t *val_len);
static bool session_var_resolve_int_handle(
    const void *cap, const char *name, vef_session_var_handle_t **out_handle);
static bool session_var_read_int(const vef_session_var_handle_t *handle,
                                 long long *out);
static void session_var_free_handle(vef_session_var_handle_t *handle);

namespace {

static vef_preview_session_var_t g_session_var_vtable{
    VEF_PREVIEW_SESSION_VAR_ABI_VERSION,
    session_var_get_int,
    session_var_get_str,
    session_var_resolve_int_handle,
    session_var_read_int,
    session_var_free_handle};

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

// Maps a capability_config token (the descriptor-list pointer the SDK passes as
// `cap`) to the extension that registered its variables. The extension name is
// bound here, server-side, at on_populate time; the extension never supplies
// it. Returns empty if the token is unknown (e.g. the capability is not
// currently loaded).
std::string extension_name_for_cap(const void *cap) {
  std::lock_guard<std::mutex> lock(g_session_vars_mutex);
  for (const RegisteredSessionVar &v : g_session_vars) {
    if (v.capability_config == cap) return v.extension_name;
  }
  return std::string();
}

// Reads the caller's session (THD-local) value of an extension variable as a
// newly allocated null-terminated string; the caller must free() it. Returns
// nullptr if there is no connection thread, the variable does not exist, or the
// read otherwise fails. Uses the mysql_system_variable_reader service with the
// current connection thread and "SESSION" scope, so the value returned is the
// per-connection value (the equivalent of THDVAR(current_thd, var)). The
// extension name is resolved from `cap` server-side and passed to the reader as
// the component name.
char *read_session_var_string(const void *cap, const char *name) {
  THD *thd = current_thd;
  // The reader errors on a SESSION read with a null THD, so a value can only be
  // resolved on a connection thread.
  if (thd == nullptr) return nullptr;

  const std::string extension_name = extension_name_for_cap(cap);
  if (extension_name.empty()) return nullptr;

  SERVICE_TYPE(registry) *registry = mysql_plugin_registry_acquire();
  if (registry == nullptr) return nullptr;

  char *result = nullptr;
  {
    my_service<SERVICE_TYPE(mysql_system_variable_reader)> reader(
        "mysql_system_variable_reader", registry);
    if (reader.is_valid()) {
      // Two-try grow-and-retry: the reader fails and reports the required size
      // in val_len when the buffer is too small.
      char stack_buf[256];
      char *buf = stack_buf;
      size_t val_len = sizeof(stack_buf);
      void *val = buf;
      bool err = reader->get(thd, "SESSION", extension_name.c_str(), name, &val,
                             &val_len);
      char *heap_buf = nullptr;
      if (err && val_len > sizeof(stack_buf)) {
        heap_buf = static_cast<char *>(malloc(val_len));
        if (heap_buf != nullptr) {
          val = heap_buf;
          err = reader->get(thd, "SESSION", extension_name.c_str(), name, &val,
                            &val_len);
        }
      }
      if (!err) {
        result = static_cast<char *>(malloc(val_len + 1));
        if (result != nullptr) {
          memcpy(result, val, val_len);
          result[val_len] = '\0';
        }
      }
      free(heap_buf);
    }
  }

  mysql_plugin_registry_release(registry);
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

static bool session_var_get_int(const void *cap, const char *name,
                                long long *out) {
  if (out == nullptr) return true;
  // The reader returns every value as a string; INT variables come back as
  // their decimal representation.
  char *s = read_session_var_string(cap, name);
  if (s == nullptr) return true;
  *out = strtoll(s, nullptr, 10);
  free(s);
  return false;
}

static bool session_var_get_str(const void *cap, const char *name, void **val,
                                size_t *val_len) {
  if (val == nullptr || val_len == nullptr) return true;
  char *s = read_session_var_string(cap, name);
  if (s == nullptr) return true;
  *val = s;
  *val_len = strlen(s);
  return false;
}

static bool session_var_resolve_int_handle(
    const void *cap, const char *name, vef_session_var_handle_t **out_handle) {
  if (out_handle == nullptr) return true;
  *out_handle = nullptr;

  const std::string extension_name = extension_name_for_cap(cap);
  if (extension_name.empty()) return true;
  const char *prefix =
      extension_name == "mysql_server" ? "" : extension_name.c_str();

  // Resolve the variable by name once, under the server's lock, and capture its
  // per-session storage offset. Only INT, THD-local variables have a longlong
  // session slot that read_int can dereference.
  int found_offset = -1;
  auto fn = [&found_offset](const System_variable_tracker &, sys_var *var) {
    sys_var_pluginvar *pv = var->cast_pluginvar();
    if (pv == nullptr) return;
    if (!(pv->plugin_var->flags & PLUGIN_VAR_THDLOCAL)) return;
    if ((pv->plugin_var->flags & PLUGIN_VAR_TYPEMASK) != PLUGIN_VAR_LONGLONG)
      return;
    // The offset is stored immediately after the plugin_var header, the same
    // location real_value_ptr reads for a THD-local variable.
    found_offset = *reinterpret_cast<const int *>(pv->plugin_var + 1);
  };
  System_variable_tracker::make_tracker(prefix, name)
      .access_system_variable(current_thd, fn, Suppress_not_found_error::YES);

  if (found_offset < 0) return true;

  auto *handle = new (std::nothrow) vef_session_var_handle{found_offset};
  if (handle == nullptr) return true;
  *out_handle = handle;
  return false;
}

static bool session_var_read_int(const vef_session_var_handle_t *handle,
                                 long long *out) {
  if (handle == nullptr || out == nullptr) return true;
  THD *thd = current_thd;
  if (thd == nullptr) return true;
  // Lock-free per-thread read: base pointer for this connection's session
  // variable storage plus the captured offset. global_lock=false matches how
  // the server reads THD-local variables on the connection thread (THDVAR).
  const uchar *p = intern_sys_var_ptr(thd, handle->offset, false);
  if (p == nullptr) return true;
  *out = *reinterpret_cast<const long long *>(p);
  return false;
}

static void session_var_free_handle(vef_session_var_handle_t *handle) {
  delete handle;
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
