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
#include "villagesql/sql/initialize.h"

#include "sql/bootstrap.h"
#include "sql/dd/cache/dictionary_client.h"
#include "sql/sd_notify.h"
#include "sql/sys_vars.h"
#include "sql/thd_raii.h"
#include "sql/transaction.h"
#include "villagesql/include/error.h"
#include "villagesql/include/version.h"
#include "villagesql/schema/schema_manager.h"
#include "villagesql/schema/victionary_client.h"
#include "villagesql/services/capability_registry.h"
#include "villagesql/veb/veb_file.h"

namespace villagesql {

class Sys_var_villagesql_version : public Sys_var_charptr_func {
 public:
  Sys_var_villagesql_version(const char *name_arg, const char *comment_arg);
  const uchar *global_value_ptr(THD *thd, std::string_view) override;
};

Sys_var_villagesql_version::Sys_var_villagesql_version(const char *name_arg,
                                                       const char *comment_arg)
    : Sys_var_charptr_func(name_arg, comment_arg, GLOBAL) {}

const uchar *Sys_var_villagesql_version::global_value_ptr(THD *thd,
                                                          std::string_view) {
  std::string ver = villagesql::SchemaManager::get_version().to_string();
  size_t buf_size = ver.size() + 1;
  char *buf = (char *)thd->alloc(buf_size);
  if (should_assert_if_null(buf))
    my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR), buf_size);
  else
    std::copy(ver.data(), ver.data() + ver.length() + 1, buf);
  return (uchar *)buf;
}

static Sys_var_villagesql_version Sys_villagesql_schema_version(
    "villagesql_schema_version",
    "VillageSQL schema version number. \"\" indicates not initialized.");

class Sys_var_villagesql_server_version : public Sys_var_charptr_func {
 public:
  Sys_var_villagesql_server_version(const char *name_arg,
                                    const char *comment_arg);
  const uchar *global_value_ptr(THD *thd, std::string_view) override;
};

Sys_var_villagesql_server_version::Sys_var_villagesql_server_version(
    const char *name_arg, const char *comment_arg)
    : Sys_var_charptr_func(name_arg, comment_arg, GLOBAL) {}

const uchar *Sys_var_villagesql_server_version::global_value_ptr(
    THD *thd, std::string_view) {
  std::string ver = villagesql::GetBuildVersion().to_string();
  size_t buf_size = ver.size() + 1;
  char *buf = (char *)thd->alloc(buf_size);
  if (should_assert_if_null(buf))
    my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR), buf_size);
  else
    std::copy(ver.data(), ver.data() + ver.length() + 1, buf);
  return (uchar *)buf;
}

static Sys_var_villagesql_server_version Sys_villagesql_server_version(
    "villagesql_server_version", "VillageSQL server version.");

static Sys_var_uint Sys_villagesql_vef_server_protocol(
    "villagesql_vef_server_protocol",
    "Highest VEF protocol version supported by this server build",
    READ_ONLY NON_PERSIST GLOBAL_VAR(veb::vef_server_protocol_version),
    NO_CMD_LINE, VALID_RANGE(0, 255), DEFAULT(0), BLOCK_SIZE(1));

static bool any_preview_extensions_installed() {
  VictionaryClient &vclient = VictionaryClient::instance();
  if (!vclient.is_initialized()) return false;
  auto guard = vclient.get_read_lock();
  for (const ExtensionDescriptor *desc :
       vclient.extension_descriptors().get_all_committed()) {
    const vef_registration_t *reg = desc->registration().registration;
    if (reg != nullptr && reg->required_capability_count > 0) return true;
  }
  return false;
}

static bool check_allow_preview_extensions(sys_var *, THD *, set_var *var) {
  const bool new_value = var->save_result.ulonglong_value != 0;

  if (new_value) {
    // SET GLOBAL = ON is rejected; only SET PERSIST is allowed so that
    // the value survives restart (extensions with preview capabilities
    // require it to be ON at startup).
    if (var->type == OPT_GLOBAL) {
      villagesql_error(
          "vsql_allow_preview_extensions must be set with SET PERSIST, "
          "not SET GLOBAL, to ensure the setting survives server restart",
          MYF(0));
      return true;
    }
  } else {
    // TODO(villagesql-preview): There is a TOCTOU race between this check and
    // the actual variable write: another thread could INSTALL a preview
    // extension after we check but before the variable is set to OFF. Fixing
    // this requires making the check-and-set atomic with INSTALL EXTENSION,
    // e.g. by performing the assignment inside ON_UPDATE while holding the
    // VictionaryClient write lock (which INSTALL EXTENSION also holds).
    // SET ... = OFF is rejected while any preview extensions are installed.
    if (any_preview_extensions_installed()) {
      villagesql_error(
          "Cannot set vsql_allow_preview_extensions = OFF while preview "
          "extensions are installed. Uninstall all preview extensions first.",
          MYF(0));
      return true;
    }
  }
  return false;
}

static Sys_var_bool Sys_vsql_allow_preview_extensions(
    "vsql_allow_preview_extensions",
    "Allow loading extensions that use preview capabilities. Preview "
    "capabilities are unstable and may change or be removed without notice.",
    GLOBAL_VAR(vsql_allow_preview_extensions), CMD_LINE(OPT_ARG),
    DEFAULT(false), NO_MUTEX_GUARD, NOT_IN_BINLOG,
    ON_CHECK(check_allow_preview_extensions), ON_UPDATE(nullptr), nullptr,
    sys_var::PARSE_EARLY);

bool bootstrap_for_init_file(THD *thd) {
  if (SchemaManager::bootstrap(thd)) {
    sysd::notify("STATUS=VillageSQL bootstrap for init-file unsuccessful\n");
    return true;
  }
  sysd::notify("STATUS=VillageSQL bootstap for init-file successful\n");
  return false;
}

namespace {

static bool do_init_extension_infrastructure(THD *thd) {
  // Transaction handling explanation:
  // During server startup, we need to read system tables
  // (villagesql.extensions, etc.). MySQL requires proper transaction context
  // for all table operations, even reads.
  //
  // The Disable_autocommit_guard temporarily disables autocommit, which
  // implicitly starts a transaction. This ensures all our operations happen in
  // a single transaction context.
  //
  // We need both trans_commit_stmt() and trans_commit() (or their rollback
  // versions):
  // - trans_commit_stmt(): Commits the statement-level transaction
  // - trans_commit(): Commits the full transaction
  //
  // This two-level commit is necessary because MySQL separates statement and
  // transaction contexts. Some operations might set
  // THD::transaction_rollback_request which requires the full transaction
  // rollback, not just statement rollback.
  //
  // This pattern is copied from mysql_component_infrastructure_init() to ensure
  // consistency with how MySQL handles system table access during startup.
  const Disable_autocommit_guard autocommit_guard(thd);
  const dd::cache::Dictionary_client::Auto_releaser scope_releaser(
      thd->dd_client());

  if (SchemaManager::init(thd)) {
    trans_rollback_stmt(thd);
    // Full rollback in case we have THD::transaction_rollback_request.
    trans_rollback(thd);
    return true;
  }

  // Load installed extensions from villagesql.extensions table
  // This validates manifests and cleans up orphaned expansion directories
  if (villagesql::veb::load_installed_extensions(thd)) {
    LogVSQL(ERROR_LEVEL, "Extension loading failed");
    trans_rollback_stmt(thd);
    trans_rollback(thd);
    return true;
  }

  return trans_commit_stmt(thd) || trans_commit(thd);
}

}  // namespace

/**
  For now, this just initializes villagesql system tables.

  @return Status of performed operation
  @retval false success
  @retval true failure
*/
bool init_extension_infrastructure() {
  sysd::notify("STATUS=VillageSQL initialization in progress\n");

  villagesql::services::register_builtin_capabilities();

  // We need a temporary THD during boot
  // The initialization code may update table settings, in order to avoid
  // locking and avoid asserts in the locking code, run on a bootstrap dd system
  // thread.
  if (bootstrap::run_bootstrap_thread(nullptr, nullptr,
                                      &do_init_extension_infrastructure,
                                      SYSTEM_THREAD_DD_INITIALIZE)) {
    LogVSQL(ERROR_LEVEL, "Failed to initialize");
    sysd::notify("STATUS=VillageSQL initialization unsuccessful\n");
    return true;
  }

  sysd::notify("STATUS=VillageSQL initialization successful\n");
  return false;
}

void deinit_extension_infrastructure() {
  VictionaryClient &vclient = VictionaryClient::instance();
  if (!vclient.is_initialized()) {
    LogVSQL(INFORMATION_LEVEL,
            "VillageSQL extension infrastructure never initialized");
    SchemaManager::deinit();
    return;
  }

  LogVSQL(INFORMATION_LEVEL,
          "Deinitializing VillageSQL extension infrastructure");

  {
    auto guard = vclient.get_write_lock();
    auto descriptors = vclient.extension_descriptors().get_all_committed();

    for (const ExtensionDescriptor *desc : descriptors) {
      LogVSQL(INFORMATION_LEVEL, "Unloading extension '%s' version '%s'",
              desc->extension_name().c_str(),
              desc->extension_version().c_str());
      veb::unload_vef_extension({.reason = services::UnloadReason::kShutdown},
                                desc->registration());
    }
  }

  vclient.destroy();
  SchemaManager::deinit();

  LogVSQL(INFORMATION_LEVEL,
          "VillageSQL extension infrastructure deinitialized");
}

}  // namespace villagesql
