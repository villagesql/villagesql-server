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
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, see <https://www.gnu.org/licenses/>.

// Checks every capability pin literal for the current target against the
// structurally computed fingerprint, and on failure prints the #define line
// to paste into the ABI header.
//
// The server build already fails on a stale pin (VEF_PIN_VERIFY in
// capability_registry.cc), but it accepts the empty placeholder "".  This
// test is what flags placeholders, and because it does not include
// capability_registry.cc it still builds when a stale pin has broken the
// server, so it can report the value to paste.
//
// To record pins on a target: build villagesql-unit-tests there, run this
// test, and paste each reported line over the matching #define.

#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "villagesql/sdk/include/villagesql/detail/abi_pin.h"
#include "villagesql/services/abi_specializations.h"

namespace villagesql_unittest {

namespace {

constexpr const char *current_target_suffix() {
#if defined(__APPLE__)
  return "MAC";
#elif defined(__linux__) && defined(__x86_64__)
  return "LINUX_X86";
#elif defined(__linux__) && defined(__aarch64__)
  return "LINUX_ARM";
#else
  return "UNKNOWN_TARGET";
#endif
}

template <typename T>
void check_pin(const char *type_name, const char *prefix,
               const char *pin_literal) {
  const auto computed = villagesql::detail::abi_signature_hash<T>();
  const std::string_view computed_view = computed.view();
  const std::string_view pin_view = pin_literal;
  if (pin_view == computed_view) return;
  const std::string define = std::string("#define ") + prefix + "_" +
                             current_target_suffix() + " \"" +
                             std::string(computed_view) + "\"";
  if (pin_view.empty()) {
    ADD_FAILURE() << type_name << " has no pin for this target.  Paste:\n    "
                  << define;
  } else {
    ADD_FAILURE() << type_name << " pin for this target is stale (\""
                  << pin_view << "\").  Replace it with:\n    " << define;
  }
}

#define VEF_CHECK_PIN(Type, prefix) \
  check_pin<Type>(#Type, #prefix, VEF_PIN_OF(prefix))

}  // namespace

TEST(AbiPinLiterals, Ping) {
  VEF_CHECK_PIN(vef_preview_ping_t, VEF_PREVIEW_PING_VTABLE_HASH);
}

TEST(AbiPinLiterals, Keyring) {
  VEF_CHECK_PIN(vef_preview_keyring_t, VEF_PREVIEW_KEYRING_VTABLE_HASH);
}

TEST(AbiPinLiterals, Storage) {
  VEF_CHECK_PIN(vef_preview_storage_t, VEF_PREVIEW_STORAGE_VTABLE_HASH);
}

TEST(AbiPinLiterals, ColumnStore) {
  VEF_CHECK_PIN(vef_preview_column_store_t,
                VEF_PREVIEW_COLUMN_STORE_VTABLE_HASH);
  VEF_CHECK_PIN(vef_preview_column_store_ext_desc_t,
                VEF_PREVIEW_COLUMN_STORE_CONFIG_HASH);
}

TEST(AbiPinLiterals, ThreadWorker) {
  VEF_CHECK_PIN(vef_preview_thread_worker_t,
                VEF_PREVIEW_THREAD_WORKER_VTABLE_HASH);
  VEF_CHECK_PIN(vef_thread_worker_descriptor_t,
                VEF_PREVIEW_THREAD_WORKER_CONFIG_HASH);
}

TEST(AbiPinLiterals, MysqlServices) {
  VEF_CHECK_PIN(void, VEF_PREVIEW_MYSQL_SERVICES_VTABLE_HASH);
  VEF_CHECK_PIN(vef_preview_mysql_services_t,
                VEF_PREVIEW_MYSQL_SERVICES_CONFIG_HASH);
}

TEST(AbiPinLiterals, IndexType) {
  VEF_CHECK_PIN(vef_preview_index_type_t, VEF_PREVIEW_INDEX_TYPE_VTABLE_HASH);
  VEF_CHECK_PIN(vef_preview_index_type_ext_desc_t,
                VEF_PREVIEW_INDEX_TYPE_CONFIG_HASH);
}

TEST(AbiPinLiterals, IndexProfile) {
  VEF_CHECK_PIN(vef_preview_index_profile_t,
                VEF_PREVIEW_INDEX_PROFILE_VTABLE_HASH);
  VEF_CHECK_PIN(vef_preview_index_profile_ext_desc_t,
                VEF_PREVIEW_INDEX_PROFILE_CONFIG_HASH);
}

TEST(AbiPinLiterals, Auth) {
  VEF_CHECK_PIN(vef_preview_auth_t, VEF_PREVIEW_AUTH_VTABLE_HASH);
  VEF_CHECK_PIN(vef_auth_cc_t, VEF_PREVIEW_AUTH_CONFIG_HASH);
}

TEST(AbiPinLiterals, SqlQuery) {
  VEF_CHECK_PIN(vef_preview_sql_query_t, VEF_PREVIEW_SQL_QUERY_VTABLE_HASH);
}

TEST(AbiPinLiterals, StatusVar) {
  VEF_CHECK_PIN(vef_preview_status_var_t, VEF_PREVIEW_STATUS_VAR_VTABLE_HASH);
  VEF_CHECK_PIN(vef_status_var_descriptor_list_t,
                VEF_PREVIEW_STATUS_VAR_CONFIG_HASH);
}

TEST(AbiPinLiterals, SysVar) {
  VEF_CHECK_PIN(vef_preview_sys_var_t, VEF_PREVIEW_SYS_VAR_VTABLE_HASH);
  VEF_CHECK_PIN(vef_sys_var_descriptor_list_t, VEF_PREVIEW_SYS_VAR_CONFIG_HASH);
}

TEST(AbiPinLiterals, SessionVar) {
  VEF_CHECK_PIN(vef_preview_session_var_t, VEF_PREVIEW_SESSION_VAR_VTABLE_HASH);
  VEF_CHECK_PIN(vef_session_var_descriptor_list_t,
                VEF_PREVIEW_SESSION_VAR_CONFIG_HASH);
}

TEST(AbiPinLiterals, StatementEvent) {
  VEF_CHECK_PIN(vef_preview_statement_event_t,
                VEF_PREVIEW_STATEMENT_EVENT_VTABLE_HASH);
  VEF_CHECK_PIN(vef_statement_event_cc_t,
                VEF_PREVIEW_STATEMENT_EVENT_CONFIG_HASH);
}

}  // namespace villagesql_unittest
