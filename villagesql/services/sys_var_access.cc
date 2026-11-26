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
#include <optional>
#include <string>
#include <vector>

#include "my_sys.h"
#include "my_thread.h"
#include "mysql/components/my_service.h"
#include "mysql/components/services/component_sys_var_service.h"
#include "mysql/components/services/mysql_string.h"
#include "mysql/components/services/mysql_system_variable.h"
#include "sql/current_thd.h"
#include "sql/server_component/mysql_system_variable_bits.h"
#include "sql/server_component/storing_auto_thd.h"
#include "sql/set_var.h"
#include "villagesql/include/error.h"
#include "villagesql/services/plugin_registry.h"

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
  if (on_change == nullptr) return;
  DEBUG_SYNC_C("vef_one_var_before_on_change");
  on_change(var_name_copy.c_str(), new_val);
}

// A temporary internal THD for a caller with no session, with the privileges
// of upstream's internal THDs. Unlike Storing_auto_THD, it ends the thread's
// mysys state only if it initialized it: the main thread at startup has that
// state but no THD, and ending it there breaks the next THD the thread uses.
// TODO(villagesql-rebase): once upstream reads a SENSITIVE variable with no
// THD without dereferencing it, and Storing_auto_THD no longer ends the mysys
// state of a thread that has it but no THD, drop NoSessionThd.
class NoSessionThd {
 public:
  NoSessionThd() : m_end_thread(!my_thread_is_inited()) {
    if (m_end_thread) my_thread_init();
    m_thd = create_internal_thd_ctx(m_ctx);
  }

  ~NoSessionThd() {
    destroy_internal_thd_ctx(m_thd, m_ctx);
    if (m_end_thread) my_thread_end();
  }

  NoSessionThd(const NoSessionThd &) = delete;
  NoSessionThd &operator=(const NoSessionThd &) = delete;

  THD *get() const { return m_thd; }

 private:
  const bool m_end_thread;
  THD *m_thd;
  Sctx_ptr<Security_context> m_ctx;
};

}  // namespace

bool get_variable(const char *component_name, const char *name,
                  bool force_sensitive_access, void **val, size_t *val_len) {
  // A SENSITIVE variable's lookup dereferences the THD, so a caller with no
  // session, such as an extension's on_init at startup, reads under a
  // temporary internal THD.
  std::optional<NoSessionThd> no_session_thd;
  THD *thd = current_thd;
  if (thd == nullptr) thd = no_session_thd.emplace().get();
  // Built-in variables have no prefix.
  const char *prefix =
      strcmp(component_name, "mysql_server") == 0 ? "" : component_name;
  const System_variable_tracker tracker =
      System_variable_tracker::make_tracker(prefix, name);
  const Force_sensitive_system_variable_access force =
      force_sensitive_access ? Force_sensitive_system_variable_access::YES
                             : Force_sensitive_system_variable_access::NO;
  // get_variable_value() fails when the value does not fit, and sets len to
  // the size needed, so a second attempt with that size reads the value
  // unless it grew in between.
  std::string buf(1024, '\0');
  for (int attempt = 0; attempt < 2; ++attempt) {
    size_t len = buf.size();
    const std::optional<bool> read = tracker.access_system_variable<bool>(
        thd,
        [&](const System_variable_tracker &, sys_var *var) {
          return get_variable_value(thd, var, buf.data(), OPT_GLOBAL, &len) !=
                 nullptr;
        },
        Suppress_not_found_error::YES, force);
    if (!read.has_value()) return true;
    if (*read) {
      char *copy = static_cast<char *>(malloc(len + 1));
      if (copy == nullptr) return true;
      memcpy(copy, buf.data(), len);
      copy[len] = '\0';
      *val = copy;
      *val_len = len;
      return false;
    }
    if (len <= buf.size()) return true;
    buf.assign(len, '\0');
  }
  return true;
}

bool set_variable(const char *component_name, const char *name,
                  const char *scope, const char *val) {
  if (val == nullptr) return true;

  PluginRegistry registry;
  if (!registry.is_valid()) return true;

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

  return result;
}

bool register_one_sys_var(std::string_view extension_name,
                          const SysVarDesc &desc) {
  const std::string ext_name(extension_name);
  PluginRegistry registry;
  if (!registry.is_valid()) {
    LogVSQL(ERROR_LEVEL, "register_one_sys_var: failed to acquire registry");
    return true;
  }

  my_service<SERVICE_TYPE(component_sys_variable_register)> reg_svc(
      "component_sys_variable_register", registry);
  if (!reg_svc.is_valid()) {
    LogVSQL(
        ERROR_LEVEL,
        "register_one_sys_var: component_sys_variable_register unavailable");
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
    return true;
  }

  {
    std::lock_guard<std::mutex> lock(g_one_vars_mutex);
    g_one_vars.push_back(
        {ext_name, std::string(desc.name), desc.value_ptr, desc.on_change});
  }

  return false;
}

void unregister_one_sys_var(std::string_view extension_name,
                            const char *var_name) {
  const std::string ext_name(extension_name);
  {
    std::lock_guard<std::mutex> lock(g_one_vars_mutex);
    auto &vec = g_one_vars;
    std::erase_if(vec, [&](const OneVarEntry &e) {
      return e.extension_name == ext_name && e.var_name == var_name;
    });
  }

  PluginRegistry registry;
  if (!registry.is_valid()) return;

  my_service<SERVICE_TYPE(component_sys_variable_unregister)> unreg_svc(
      "component_sys_variable_unregister", registry);
  if (unreg_svc.is_valid()) {
    unreg_svc->unregister_variable(ext_name.c_str(), var_name);
  }
}

}  // namespace villagesql::services
