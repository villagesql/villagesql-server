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

#include "villagesql/veb/sql_extension_update_precheck.h"

#include <cstdio>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "villagesql/schema/victionary_client.h"
#include "villagesql/veb/veb_file.h"

namespace villagesql {
namespace veb {

namespace {

UpdatePreCheckResult fail(std::string message) {
  UpdatePreCheckResult r;
  r.ok = false;
  r.error_message = std::move(message);
  return r;
}

UpdatePreCheckResult ok() {
  UpdatePreCheckResult r;
  r.ok = true;
  return r;
}

// Storage length of a type in the target version, harvested from the loaded
// registration. Mirrors the length fields of CurrentTypeSnapshot.
struct TargetTypeLength {
  int64_t persisted_length{0};
  int64_t max_persisted_length{0};
  bool variable_length{false};
};

// Reject if a type retained across the update changes its storage length.
// Every existing column of the type has a backing field whose length was
// fixed at CREATE TABLE (persisted_length for a fixed-length type,
// max_persisted_length for a variable-length one). Opening the table
// requires that length to still match the type, so the update must keep:
//   - the length kind (fixed vs. variable),
//   - persisted_length, for a fixed-length type,
//   - max_persisted_length, for a variable-length type. A grow is rejected
//     as well as a shrink. Either one leaves existing columns mismatched.
//
// `current` is the state of the extension as installed today. The
// `target_lengths` map is built from the new version's VEB that we're being
// asked to update to.
UpdatePreCheckResult check_retained_types_storage_length(
    const UpdatePreCheckInput &current,
    const std::unordered_map<std::string, TargetTypeLength> &target_lengths) {
  for (const auto &c : current.current_types) {
    auto it = target_lengths.find(c.type_name);
    if (it == target_lengths.end()) continue;  // dropped
    const TargetTypeLength &t = it->second;
    char buf[512];
    if (t.variable_length != c.variable_length) {
      std::snprintf(
          buf, sizeof(buf),
          "Cannot update extension '%s': type '%s' changed from %s to %s. "
          "Existing stored data would be corrupted",
          current.extension_name.c_str(), c.type_name.c_str(),
          c.variable_length ? "variable-length" : "fixed-length",
          t.variable_length ? "variable-length" : "fixed-length");
      return fail(buf);
    }
    if (!c.variable_length && t.persisted_length != c.persisted_length) {
      std::snprintf(
          buf, sizeof(buf),
          "Cannot update extension '%s': type '%s' persisted_length changed "
          "from %lld to %lld. Existing stored data would be corrupted",
          current.extension_name.c_str(), c.type_name.c_str(),
          static_cast<long long>(c.persisted_length),
          static_cast<long long>(t.persisted_length));
      return fail(buf);
    }
    if (c.variable_length && t.max_persisted_length != c.max_persisted_length) {
      std::snprintf(
          buf, sizeof(buf),
          "Cannot update extension '%s': variable-length type '%s' "
          "max_persisted_length changed from %lld to %lld. Existing "
          "columns of this type would no longer match their stored length",
          current.extension_name.c_str(), c.type_name.c_str(),
          static_cast<long long>(c.max_persisted_length),
          static_cast<long long>(t.max_persisted_length));
      return fail(buf);
    }
  }
  return ok();
}

// Reject if a type present in the current registration is absent from the
// target registration AND still has dependent columns or SP params.
//
// `current` is the state of the extension as installed today (and the
// dependent columns / SP params already on disk that reference its types).
// `target_type_names` is the set of type names declared by the new
// version's VEB.
UpdatePreCheckResult check_dropped_types_have_no_dependents(
    const UpdatePreCheckInput &current,
    const std::unordered_set<std::string> &target_type_names) {
  for (const auto &col : current.dependent_columns) {
    if (target_type_names.find(col.type_name) != target_type_names.end())
      continue;
    char buf[512];
    std::snprintf(buf, sizeof(buf),
                  "Cannot update extension '%s': type '%s' is being dropped "
                  "but column %s.%s.%s depends on it",
                  current.extension_name.c_str(), col.type_name.c_str(),
                  col.db_name.c_str(), col.table_name.c_str(),
                  col.column_name.c_str());
    return fail(buf);
  }
  for (const auto &sp : current.dependent_sp_params) {
    if (target_type_names.find(sp.type_name) != target_type_names.end())
      continue;
    char buf[512];
    std::snprintf(buf, sizeof(buf),
                  "Cannot update extension '%s': type '%s' is being dropped "
                  "but stored procedure parameter %s.%s.%s depends on it",
                  current.extension_name.c_str(), sp.type_name.c_str(),
                  sp.db_name.c_str(), sp.sp_name.c_str(),
                  sp.param_name.c_str());
    return fail(buf);
  }
  return ok();
}

}  // namespace

UpdatePreCheckResult RunUpdatePreCheck(const UpdatePreCheckInput &input) {
  // Load the target .so for inspection only -- no capability populate. The
  // pure raw loader does exactly: dlopen + lookup symbols + vef_register +
  // protocol validation. We harvest target types from the returned
  // registration, then immediately unload.
  ExtensionRegistration target;
  std::string load_error;
  if (open_vef_extension(input.target_so_path,
                         static_cast<vef_protocol_t>(input.server_protocol),
                         target, load_error)) {
    return fail(std::string("Cannot update extension '") +
                input.extension_name + "': failed to load target .so at " +
                input.target_so_path + ": " + load_error);
  }

  // Harvest target-side type metadata from the loaded registration. We don't
  // hold pointers into the registration past the unload call below.
  //
  // max_persisted_length and variable_length sit past the end of older
  // vef_type_desc_t layouts, so read them only when both the type and the
  // negotiated protocol are new enough -- the same gating registration
  // applies (see validate.cc).
  std::unordered_map<std::string, TargetTypeLength> target_lengths;
  std::unordered_set<std::string> target_type_names;
  if (target.registration != nullptr) {
    const vef_registration_t *reg = target.registration;
    for (unsigned int i = 0; i < reg->type_count; ++i) {
      const vef_type_desc_t *t = reg->types[i];
      if (t == nullptr || t->name == nullptr) continue;
      TargetTypeLength length;
      length.persisted_length = static_cast<int64_t>(t->persisted_length);
      if (t->protocol >= VEF_PROTOCOL_3 &&
          target.negotiated_protocol >= VEF_PROTOCOL_3) {
        length.max_persisted_length =
            static_cast<int64_t>(t->max_persisted_length);
      }
      if (t->protocol >= VEF_PROTOCOL_4 &&
          target.negotiated_protocol >= VEF_PROTOCOL_4) {
        length.variable_length = t->variable_length;
      }
      target_lengths[t->name] = length;
      target_type_names.insert(t->name);
    }
  }

  // Unload before running the checks. The check functions operate purely on
  // the harvested data; the target .so has no business staying loaded past
  // the harvest.
  close_vef_extension(target);

  UpdatePreCheckResult r;

  r = check_retained_types_storage_length(input, target_lengths);
  if (!r.ok) return r;

  r = check_dropped_types_have_no_dependents(input, target_type_names);
  if (!r.ok) return r;

  return ok();
}

// NOTE: when adding a new extension-owned systable that participates in an
// UPDATE (i.e. its rows carry an extension_name + extension_version), both
// this function and the UNINSTALL EXTENSION code in sql_extension.cc need to
// be updated to walk the new map. See the TODO in
// veb_file.cc::load_installed_extensions for the planned centralization of
// this enumeration on VictionaryClient.
//
// TODO(villagesql): string parameters here are inconsistent -- names use
// const std::string & but target_so_path uses std::string + std::move.
// Settle on a single convention (probably std::string_view for the reads
// and std::string by-value for the one field that gets moved into `input`).
void BuildUpdatePreCheckSnapshot(const VictionaryClient &victionary,
                                 const std::string &extension_name,
                                 const std::string &current_version,
                                 const std::string &target_version,
                                 std::string target_so_path,
                                 UpdatePreCheckInput *input) {
  victionary.assert_read_or_write_lock_held();

  input->extension_name = extension_name;
  input->current_version = current_version;
  input->target_version = target_version;
  input->target_so_path = std::move(target_so_path);
  input->server_protocol = static_cast<int>(vef_server_protocol_version);

  for (const auto *td : victionary.type_descriptors().get_all_committed()) {
    if (td == nullptr || td->extension_name() != extension_name ||
        td->extension_version() != current_version)
      continue;
    CurrentTypeSnapshot s;
    s.type_name = td->type_name();
    s.persisted_length = td->persisted_length();
    s.max_persisted_length = td->max_persisted_length();
    s.variable_length = td->is_variable_length();
    input->current_types.push_back(std::move(s));
  }

  for (const auto *col : victionary.columns().get_all_committed()) {
    if (col == nullptr || col->extension_name != extension_name ||
        col->extension_version != current_version)
      continue;
    DependentColumnSnapshot s;
    s.db_name = col->db_name();
    s.table_name = col->table_name();
    s.column_name = col->column_name();
    s.type_name = col->type_name;
    input->dependent_columns.push_back(std::move(s));
  }

  for (const auto *sp : victionary.sp_params().get_all_committed()) {
    if (sp == nullptr || sp->extension_name != extension_name ||
        sp->extension_version != current_version)
      continue;
    DependentSpParamSnapshot s;
    s.db_name = sp->db_name();
    s.sp_name = sp->sp_name();
    s.param_name = sp->param_name();
    s.type_name = sp->type_name;
    input->dependent_sp_params.push_back(std::move(s));
  }
}

}  // namespace veb
}  // namespace villagesql
