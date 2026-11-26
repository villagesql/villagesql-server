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
#include <cstring>
#include <iterator>
#include <memory>
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
#include "sql/auth/auth_common.h"
#include "sql/auth/sql_security_ctx.h"
#include "sql/persisted_variable.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_plugin_var.h"
#include "sql/sql_thd_internal_api.h"
#include "villagesql/include/error.h"
#include "villagesql/sdk/include/villagesql/abi/preview/sys_var.h"
#include "villagesql/services/plugin_registry.h"
#include "villagesql/services/sys_var_access.h"

namespace villagesql::services {

// Forward declarations for the vtable, defined below after g_sys_vars, which
// they need to look the variable up.
static bool sys_var_get(const char *component_name, const char *name,
                        void **val, size_t *val_len);
static bool sys_var_set(const char *component_name, const char *name,
                        const char *scope, const char *val);
static bool sys_var_set_int(const char *component_name, const char *name,
                            const char *scope, long long val);

namespace {

static vef_preview_sys_var_t g_sys_var_vtable{
    VEF_PREVIEW_SYS_VAR_ABI_VERSION, sys_var_get, sys_var_set, sys_var_set_int};

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
  // For VEF_VAR_ENUM only: the TYPE_LIB listing the variable's allowed values,
  // and its fully server-owned backing arrays. register_variable stores the
  // TYPE_LIB pointer (it does not deep-copy), so it must outlive the registered
  // variable. Everything the TYPE_LIB points at is copied here -- the name
  // strings included -- so nothing references the extension's memory, which is
  // unmapped when the extension unloads. Held on the heap (unique_ptr) so a
  // g_sys_vars reallocation cannot move the TYPE_LIB while the registered
  // SYS_VAR points at it; freed when this entry is destroyed, after the
  // variable has been unregistered. Empty for non-enum variables.
  std::vector<std::string> enum_names;       // owned copies of the name strings
  std::vector<const char *> enum_name_ptrs;  // -> enum_names, NULL-terminated
  std::vector<unsigned int> enum_name_lengths;
  std::unique_ptr<TYPE_LIB> enum_typelib;
};

std::mutex g_sys_vars_mutex;
std::vector<RegisteredSysVar> g_sys_vars;

// Generic update trampoline used when on_change is non-null.
// Performs the default typed update (mirroring MySQL's update_func_* family)
// then calls the extension callback.
static void vef_sys_var_update_trampoline(MYSQL_THD, SYS_VAR *var,
                                          void *val_ptr, const void *save) {
  // MySQL hands us the new value in save and expects it stored in val_ptr. For
  // a MEMALLOC string it frees the old buffer as soon as this returns
  // (plugin_var_memalloc_global_update), so not storing the new one would leave
  // val_ptr pointing at freed memory for UNINSTALL to free a second time. The
  // write below is therefore unconditional, and takes its type from MySQL's
  // descriptor rather than from a g_sys_vars lookup that can miss. Concurrent
  // global updates are serialized by MySQL on LOCK_global_system_variables.
  vef_sys_var_change_t change{};
  switch (var->flags & PLUGIN_VAR_TYPEMASK) {
    case PLUGIN_VAR_BOOL:
      change.type = VEF_VAR_BOOL;
      change.bool_val = *static_cast<const bool *>(save);
      *static_cast<bool *>(val_ptr) = change.bool_val;
      break;
    case PLUGIN_VAR_LONGLONG:
      change.type = VEF_VAR_INT;
      change.int_val = *static_cast<const long long *>(save);
      *static_cast<long long *>(val_ptr) = change.int_val;
      break;
    case PLUGIN_VAR_DOUBLE:
      change.type = VEF_VAR_DOUBLE;
      change.dbl_val = *static_cast<const double *>(save);
      *static_cast<double *>(val_ptr) = change.dbl_val;
      break;
    case PLUGIN_VAR_STR:
      change.type = VEF_VAR_STR;
      change.str_val = *static_cast<const char *const *>(save);
      *static_cast<const char **>(val_ptr) = change.str_val;
      break;
    case PLUGIN_VAR_ENUM:
      // An enum's value is its zero-based index into the TYPE_LIB, stored as an
      // unsigned long (MySQL's TYPE_LIB backing width). The server already
      // validated the name against the list before calling us, so the index is
      // in range.
      change.type = VEF_VAR_ENUM;
      change.enum_val = *static_cast<const unsigned long *>(save);
      *static_cast<unsigned long *>(val_ptr) = change.enum_val;
      break;
    default:
      // on_populate_sys_var registers no other types, and writing val_ptr
      // would mean guessing its width.
      LogVSQL(ERROR_LEVEL,
              "System variable update trampoline: unexpected type 0x%x",
              var->flags & PLUGIN_VAR_TYPEMASK);
      return;
  }

  // on_change and the variable name do come from the registry. A miss means the
  // variable is still registering or has already been depopulated: the write
  // above stands either way, there is just no callback to run.
  vef_sys_var_on_change_func_t on_change = nullptr;
  std::string var_name_copy;
  {
    std::lock_guard<std::mutex> lock(g_sys_vars_mutex);
    for (const RegisteredSysVar &v : g_sys_vars) {
      if (v.value_ptr == val_ptr) {
        on_change = v.on_change;
        var_name_copy = v.var_name;
        break;
      }
    }
  }
  if (on_change == nullptr) return;

  // These copies own the strings so change.var_name and change.str_val stay
  // valid once the lock is released: a concurrent on_depopulate_sys_var may
  // erase the entry, and the next SET frees this buffer as its old value.
  std::string str_val_copy;
  change.var_name = var_name_copy.c_str();
  if (change.type == VEF_VAR_STR && change.str_val != nullptr) {
    str_val_copy = change.str_val;
    change.str_val = str_val_copy.c_str();
  }

  // on_change runs with g_sys_vars_mutex released (the callback may re-enter
  // via SYS_VARS.set()). MySQL keeps the .so loaded across the call: a
  // component variable update holds LOCK_system_variables_hash for read
  // (visit_component_variable in set_var.cc), which the unregister_variable in
  // on_depopulate_sys_var needs for write before the caller dlcloses. Hence
  // there is no drain counter here, unlike other capabilities whose callbacks
  // dispatch under no such lock. This assumption is validated in
  // sys_var_uninstall_race.test and should fail if that assumption is ever
  // broken.
  DEBUG_SYNC_C("vef_sys_var_before_on_change");
  on_change(&change);
}

// Moves every entry registered from capability_config out of g_sys_vars. The
// caller must keep the returned entries alive until their variables are
// unregistered. The TYPE_LIB passed to register_variable() lists an enum
// variable's allowed values. The server keeps a pointer to it, but the
// variable's RegisteredSysVar owns it. Destroying the RegisteredSysVar first
// would let a concurrent SET statement on that variable validate against the
// freed list.
std::vector<RegisteredSysVar> extract_registered_vars(
    const void *capability_config) {
  std::lock_guard<std::mutex> lock(g_sys_vars_mutex);
  auto it = std::stable_partition(
      g_sys_vars.begin(), g_sys_vars.end(), [&](const RegisteredSysVar &rv) {
        return rv.capability_config != capability_config;
      });
  std::vector<RegisteredSysVar> extracted(
      std::make_move_iterator(it), std::make_move_iterator(g_sys_vars.end()));
  g_sys_vars.erase(it, g_sys_vars.end());
  return extracted;
}

void unregister_vars(SERVICE_TYPE(registry) * registry,
                     const std::vector<RegisteredSysVar> &vars) {
  my_service<SERVICE_TYPE(component_sys_variable_unregister)> unreg_svc(
      "component_sys_variable_unregister", registry);
  if (!unreg_svc.is_valid()) return;
  for (const RegisteredSysVar &rv : vars)
    unreg_svc->unregister_variable(rv.extension_name.c_str(),
                                   rv.var_name.c_str());
}

}  // namespace

vef_preview_sys_var_t *preview_sys_var_vtable() { return &g_sys_var_vtable; }

// The declared type of a variable this extension registered, or nullopt if it
// registered no such name. Each setter checks the type it is for: a name
// registered as some other type is not settable through the vtable entry
// asking for it.
static std::optional<vef_var_type_t> registered_var_type(
    const char *component_name, const char *name) {
  std::lock_guard<std::mutex> lock(g_sys_vars_mutex);
  for (const RegisteredSysVar &v : g_sys_vars) {
    if (v.extension_name == component_name && v.var_name == name) {
      return v.type;
    }
  }
  return std::nullopt;
}

// Reads a variable. One that an extension registered is read without the
// SENSITIVE check: that keeps the value from SQL users without
// SENSITIVE_VARIABLES_OBSERVER, not from extension code, which has to read the
// secret it was configured with whoever is connected, or with no connection at
// all. Any other variable is read under MySQL's rules.
static bool sys_var_get(const char *component_name, const char *name,
                        void **val, size_t *val_len) {
  return get_variable(component_name, name,
                      registered_var_type(component_name, name).has_value(),
                      val, val_len);
}

// Serializes the extension's own writes. Each ExtensionSetThd grants its
// privileges to kExtensionSetUser and revokes them when it goes away, and
// MySQL does not count the grants, so one write ending would strip the
// privileges from another still running. A write blocked on another lock
// holds it for up to the 5 second lock_wait_timeout set below.
std::mutex g_extension_set_mutex;

// The account the extension's own writes run as. Upstream's internal THDs
// share "sys_session". A separate account keeps the extra privilege below
// from reaching them. Nothing reserves this name. An account created with it
// would hold the privileges during a write and lose them after it.
// TODO(villagesql-production): reserve this account name, or create it locked
// at install, so no admin-created account can share it.
constexpr const char *kExtensionSetUser = "villagesql.sys_var";

// The internal THD the extension's own writes run under, passed to the
// update service. Given no THD, the service creates one much like it, but
// without SENSITIVE_VARIABLES_OBSERVER, and a SENSITIVE variable cannot be
// looked up without that privilege. Hold g_extension_set_mutex for its whole
// lifetime. Errors raised on it are copied to the calling session's THD, if
// there is one, when it is destroyed.
// TODO(villagesql-rebase): once upstream fixes MySQL Bug #121923 and Bug
// #121943 (Storing_auto_THD ends the mysys state of a thread that has it but
// no THD, such as the main thread at startup), pass no THD to the update
// service again and drop ExtensionSetThd, kExtensionSetUser and
// g_extension_set_mutex.
class ExtensionSetThd {
 public:
  ExtensionSetThd()
      : m_previous_thd(current_thd), m_end_thread(!my_thread_is_inited()) {
    if (m_end_thread) my_thread_init();
    m_thd = create_internal_thd();
    m_thd->set_system_user(false);
    m_thd->set_connection_admin(false);
    lex_start(m_thd);
    const std::vector<std::string> privileges = {
        "SYSTEM_VARIABLES_ADMIN", "PERSIST_RO_VARIABLES_ADMIN",
        "SENSITIVE_VARIABLES_OBSERVER"};
    Security_context_factory factory(
        m_thd, kExtensionSetUser, "localhost", Default_local_authid(m_thd),
        Grant_temporary_dynamic_privileges(m_thd, privileges),
        Grant_temporary_static_privileges(m_thd, 0),
        Drop_temporary_dynamic_privileges(privileges));
    m_ctx = factory.create();
    if (m_ctx != nullptr) m_thd->set_security_context(m_ctx.get());
    m_thd->real_id = my_thread_self();
    // The update service gives its own THD this timeout, so a SET blocked on
    // another lock fails rather than holding LOCK_system_variables_hash.
    m_thd->variables.lock_wait_timeout = 5;
  }

  ~ExtensionSetThd() {
    if (m_previous_thd != nullptr) {
      Diagnostics_area *prev_da = m_previous_thd->get_stmt_da();
      Diagnostics_area *curr_da = m_thd->get_stmt_da();
      if (curr_da->is_error())
        prev_da->set_error_status(curr_da->mysql_errno(),
                                  curr_da->message_text(),
                                  curr_da->returned_sqlstate());
      prev_da->copy_sql_conditions_from_da(m_previous_thd, curr_da);
    }
    m_thd->free_items();
    lex_end(m_thd->lex);
    m_ctx.reset(nullptr);
    destroy_internal_thd(m_thd);
    if (m_end_thread) my_thread_end();
    if (m_previous_thd != nullptr) m_previous_thd->store_globals();
  }

  ExtensionSetThd(const ExtensionSetThd &) = delete;
  ExtensionSetThd &operator=(const ExtensionSetThd &) = delete;

  // The THD, or nullptr if its security context could not be created.
  THD *get() const { return m_ctx != nullptr ? m_thd : nullptr; }

 private:
  THD *const m_previous_thd;
  // Whether this object initialized the thread's mysys state, and so ends it.
  // The main thread at startup has that state but no THD.
  const bool m_end_thread;
  THD *m_thd;
  Sctx_ptr<Security_context> m_ctx;
};

// Sets a non-INT variable through mysql_system_variable_update_string.
// VEF_VAR_STR, VEF_VAR_BOOL, VEF_VAR_DOUBLE and VEF_VAR_ENUM all take a
// string (an enum is set by its name, which the server maps to the TYPE_LIB
// index). An INT variable is set through sys_var_set_int(): Sys_var_integer
// accepts only INT_RESULT, so this path would reject even a well-formed "42".
static bool sys_var_set(const char *component_name, const char *name,
                        const char *scope, const char *val) {
  if (val == nullptr) return true;

  // An unregistered name is refused, and so is an INT variable: it cannot be
  // set from a string and belongs to sys_var_set_int().
  const std::optional<vef_var_type_t> var_type =
      registered_var_type(component_name, name);
  if (!var_type.has_value() || *var_type == VEF_VAR_INT) return true;

  PluginRegistry registry;
  if (!registry.is_valid()) return true;

  bool result = true;

  {
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
        std::lock_guard<std::mutex> set_lock(g_extension_set_mutex);
        ExtensionSetThd set_thd;
        if (set_thd.get() != nullptr)
          result =
              update_svc->set(set_thd.get(), scope, h_base, h_name, h_value);
      }

      if (h_base) str_factory->destroy(h_base);
      if (h_name) str_factory->destroy(h_name);
      if (h_value) str_factory->destroy(h_value);
    }
  }

  return result;
}

// Sets an INT variable through mysql_system_variable_update_integer. The
// value arrives as a long long and is handed straight to set_signed(), so
// there is no text form to misparse. The server still applies the declared
// range, PERSIST and any on_change callback.
static bool sys_var_set_int(const char *component_name, const char *name,
                            const char *scope, long long val) {
  const std::optional<vef_var_type_t> var_type =
      registered_var_type(component_name, name);
  if (!var_type.has_value() || *var_type != VEF_VAR_INT) return true;

  PluginRegistry registry;
  if (!registry.is_valid()) return true;

  bool result = true;

  {
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
        std::lock_guard<std::mutex> set_lock(g_extension_set_mutex);
        ExtensionSetThd set_thd;
        if (set_thd.get() != nullptr)
          result =
              update_svc->set_signed(set_thd.get(), scope, h_base, h_name, val);
      }

      if (h_base) str_factory->destroy(h_base);
      if (h_name) str_factory->destroy(h_name);
    }
  }

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
  PluginRegistry registry;
  if (!registry.is_valid()) {
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
    return true;
  }

  for (uint32_t i = 0; i < list->var_count; i++) {
    const vef_sys_var_desc_t *v = list->vars[i];
    int flags = PLUGIN_VAR_RQCMDARG;
    if (v->sensitive) flags |= PLUGIN_VAR_SENSITIVE;
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
    ENUM_CHECK_ARG(enum) enum_arg;
    memset(&bool_arg, 0, sizeof(bool_arg));
    memset(&int_arg, 0, sizeof(int_arg));
    memset(&dbl_arg, 0, sizeof(dbl_arg));
    memset(&str_arg, 0, sizeof(str_arg));
    memset(&enum_arg, 0, sizeof(enum_arg));

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
      case VEF_VAR_ENUM:
        // check_arg.typelib is filled in after the g_sys_vars entry is created
        // below, because the TYPE_LIB must live in that (durable) entry:
        // register_variable stores the pointer without copying. def_val (an
        // index) is safe to set here.
        flags |= PLUGIN_VAR_ENUM;
        enum_arg.def_val = v->enumeration.def_val;
        check_arg = &enum_arg;
        value_ptr = v->enumeration.value_ptr;
        break;
    }

    mysql_sys_var_update_func update_fn =
        v->on_change != nullptr ? vef_sys_var_update_trampoline : nullptr;

    // Record the variable before registering it: register_variable applies an
    // already-persisted value inline, and the trampoline looks the variable up
    // by storage pointer to find its on_change. Registering first would skip
    // that callback for the persisted value.
    {
      std::lock_guard<std::mutex> lock(g_sys_vars_mutex);
      g_sys_vars.push_back({extension_name, std::string(v->name), v->type,
                            value_ptr, v->on_change, ctx.capability_config,
                            /*enum_names=*/{},
                            /*enum_name_ptrs=*/{},
                            /*enum_name_lengths=*/{},
                            /*enum_typelib=*/nullptr});

      // For an enum, build the TYPE_LIB into the just-pushed (durable) entry
      // and point check_arg at it. Everything is copied onto server-owned
      // storage: register_variable keeps the TYPE_LIB pointer without copying,
      // and the extension's memory is gone once it unloads.
      // unique_ptr<TYPE_LIB> and the vectors keep stable heap addresses across
      // a later push_back's reallocation of g_sys_vars, so the registered
      // variable's typelib pointer stays valid for the variable's whole
      // lifetime.
      if (v->type == VEF_VAR_ENUM) {
        RegisteredSysVar &entry = g_sys_vars.back();
        const uint32_t n = v->enumeration.name_count;
        entry.enum_names.reserve(n);
        entry.enum_name_ptrs.reserve(n + 1);
        entry.enum_name_lengths.reserve(n);
        for (uint32_t j = 0; j < n; ++j)
          entry.enum_names.emplace_back(v->enumeration.names[j] != nullptr
                                            ? v->enumeration.names[j]
                                            : "");
        for (const std::string &s : entry.enum_names) {
          entry.enum_name_ptrs.push_back(s.c_str());
          entry.enum_name_lengths.push_back(
              static_cast<unsigned int>(s.length()));
        }
        entry.enum_name_ptrs.push_back(nullptr);  // TYPE_LIB expects NULL-term
        entry.enum_typelib = std::make_unique<TYPE_LIB>();
        entry.enum_typelib->count = n;
        entry.enum_typelib->name = "";
        entry.enum_typelib->type_names = entry.enum_name_ptrs.data();
        entry.enum_typelib->type_lengths = entry.enum_name_lengths.data();
        enum_arg.typelib = entry.enum_typelib.get();
      }
    }

    if (reg_svc->register_variable(extension_name.c_str(), v->name, flags,
                                   v->comment ? v->comment : "", nullptr,
                                   update_fn, check_arg, value_ptr)) {
      // This variable never registered, so drop its entry before the rollback
      // below, which unregisters by name.
      {
        std::lock_guard<std::mutex> lock(g_sys_vars_mutex);
        std::erase_if(g_sys_vars, [&](const RegisteredSysVar &rv) {
          return rv.capability_config == ctx.capability_config &&
                 rv.value_ptr == value_ptr;
        });
      }
      LogVSQL(ERROR_LEVEL,
              "Failed to register system variable '%s' for extension '%s'",
              v->name, extension_name.c_str());
      error_message = std::string("Failed to register system variable '") +
                      v->name + "' for extension '" + extension_name + "'";
      // Roll back the variables registered so far.
      // See extract_registered_vars().
      const std::vector<RegisteredSysVar> retired =
          extract_registered_vars(ctx.capability_config);
      // The variables are out of g_sys_vars but stay registered with MySQL
      // until unregister_vars(), so a concurrent SET can still reach them. The
      // sync point lets a test run that SET here; see
      // sys_var_rollback_set_race.test.
      DEBUG_SYNC_C("vef_sys_var_after_rollback_extract");
      unregister_vars(registry, retired);
      return true;
    }

    LogVSQL(INFORMATION_LEVEL,
            "Registered system variable '%s' for extension '%s'", v->name,
            extension_name.c_str());
  }

  return false;
}

void on_depopulate_sys_var(const DepopulateContext &ctx) {
  // Destroyed at end of scope, after unregister_vars().
  // See extract_registered_vars().
  const std::vector<RegisteredSysVar> retired =
      extract_registered_vars(ctx.capability_config);

  // The variables are out of g_sys_vars but stay registered with MySQL until
  // unregister_variable below, so a concurrent SET still reaches the update
  // trampoline and finds no entry for them. The sync point lets a test run that
  // SET here; see sys_var_depopulate_set_race.test.
  DEBUG_SYNC_C("vef_sys_var_after_depopulate_erase");

  if (retired.empty()) return;

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
      // Get the sets of persisted plugin/component variables once. A
      // SENSITIVE variable is persisted in its own set. We check membership
      // before calling reset_persisted_variables to avoid the warning that
      // MySQL emits when the variable is not in the persisted config file
      // (reset_persisted_variables with if_exists=true warns rather than
      // errors, but we want complete silence for variables that were never
      // persisted).
      Persisted_variables_uset *plugin_vars =
          pvc->get_persisted_dynamic_plugin_variables();
      Persisted_variables_uset *sensitive_plugin_vars =
          pvc->get_persisted_dynamic_sensitive_plugin_variables();
      for (const RegisteredSysVar &rv : retired) {
        std::string full_name = rv.extension_name + "." + rv.var_name;
        if (plugin_vars != nullptr && sensitive_plugin_vars != nullptr) {
          // SET PERSIST, RESET PERSIST and INSTALL EXTENSION change the sets
          // under their lock, so walk them under the lock too. It is released
          // before reset_persisted_variables, which takes it itself.
          bool persisted;
          {
            std::lock_guard<Persisted_variables_cache> guard(*pvc);
            persisted =
                std::ranges::any_of(
                    *plugin_vars,
                    [&full_name](const st_persist_var &v) {
                      // Lets a test change the set while this loop walks it;
                      // see sys_var_depopulate_persisted_lookup.test.
                      DEBUG_SYNC_C("vef_sys_var_persisted_lookup");
                      return v.key == full_name;
                    }) ||
                std::ranges::any_of(*sensitive_plugin_vars,
                                    [&full_name](const st_persist_var &v) {
                                      return v.key == full_name;
                                    });
          }
          if (!persisted) continue;
        }
        pvc->reset_persisted_variables(ctx.thd, full_name.c_str(), true);
      }
    }
  }

  PluginRegistry registry;
  if (!registry.is_valid()) return;
  unregister_vars(registry, retired);
}

}  // namespace villagesql::services
