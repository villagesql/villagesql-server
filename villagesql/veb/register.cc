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

#include "villagesql/veb/register.h"

#include <algorithm>

#include "sql/sql_class.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/descriptor/func_descriptor.h"
#include "villagesql/schema/descriptor/index_profile_descriptor.h"
#include "villagesql/schema/descriptor/index_type_descriptor.h"
#include "villagesql/schema/descriptor/type_descriptor.h"
#include "villagesql/schema/victionary_client.h"

namespace villagesql {
namespace veb {

bool register_validated_extension(THD &thd, ValidatedRegistration validated,
                                  std::string &error_out) {
  auto &victionary = VictionaryClient::instance();
  victionary.assert_write_lock_held();

  for (auto &descriptor : validated.types) {
    std::string type_name = descriptor.type_name();
    std::string ext_name = descriptor.extension_name();

    LogVSQL(INFORMATION_LEVEL, "Registering type '%s' from extension '%s'",
            type_name.c_str(), ext_name.c_str());

    const TypeDescriptor *existing =
        victionary.type_descriptors().get_committed(descriptor.key());
    if (existing) {
      error_out = "type '" + type_name + "' already exists";
      LogVSQL(ERROR_LEVEL, "Extension '%s': %s", ext_name.c_str(),
              error_out.c_str());
      return true;
    }

    if (victionary.type_descriptors().MarkForInsertion(thd,
                                                       std::move(descriptor))) {
      error_out = "failed to register type '" + type_name + "'";
      LogVSQL(ERROR_LEVEL, "Extension '%s': %s", ext_name.c_str(),
              error_out.c_str());
      return true;
    }

    LogVSQL(INFORMATION_LEVEL, "Successfully registered type '%s'",
            type_name.c_str());
  }

  for (auto &descriptor : validated.funcs) {
    std::string func_name = descriptor.function_name();
    std::string ext_name = descriptor.extension_name();

    LogVSQL(INFORMATION_LEVEL, "Registering VDF '%s' from extension '%s'",
            func_name.c_str(), ext_name.c_str());

    const FuncDescriptor *existing =
        victionary.funcs().get_committed(descriptor.key());
    if (existing) {
      error_out = "VDF '" + func_name + "' already exists";
      LogVSQL(ERROR_LEVEL, "Extension '%s': %s", ext_name.c_str(),
              error_out.c_str());
      return true;
    }

    if (victionary.funcs().MarkForInsertion(thd, std::move(descriptor))) {
      error_out = "failed to register VDF '" + func_name + "'";
      LogVSQL(ERROR_LEVEL, "Extension '%s': %s", ext_name.c_str(),
              error_out.c_str());
      return true;
    }

    LogVSQL(INFORMATION_LEVEL, "Successfully registered VDF '%s'",
            func_name.c_str());
  }

  return false;
}

bool register_preview_capabilities(THD &thd,
                                   ValidatedPreviewCapabilities preview,
                                   const ValidatedRegistration &validated,
                                   std::string &error_out) {
  auto &victionary = VictionaryClient::instance();
  victionary.assert_write_lock_held();

  // Validate profile references before moving any descriptor. Each profile's
  // data type and index type must resolve to exactly one entry in the current
  // batch.
  for (auto &profile : preview.index_profiles) {
    const std::string &prof_name = profile.profile_name();
    const std::string &prof_ext = profile.extension_name();

    // Resolve data type reference: check same-batch types only.
    // TODO(villagesql-indexing): Also resolve against committed types from the
    // same extension once cross-extension profile references are designed.
    std::string resolved_type_ext;
    int type_count = 0;
    for (const auto &td : validated.types) {
      if (profile.type_ref().matches_key(td.key().str())) {
        if (type_count == 0) resolved_type_ext = td.extension_name();
        type_count++;
      }
    }
    if (type_count == 0) {
      error_out = "index profile '" + prof_name +
                  "': references unknown data type '" + profile.type_name() +
                  "'";
      LogVSQL(ERROR_LEVEL, "Extension '%s': %s", prof_ext.c_str(),
              error_out.c_str());
      return true;
    }
    if (type_count > 1) {
      error_out = "index profile '" + prof_name + "': data type '" +
                  profile.type_name() +
                  "' is ambiguous; qualify as 'extension.type_name'";
      LogVSQL(ERROR_LEVEL, "Extension '%s': %s", prof_ext.c_str(),
              error_out.c_str());
      return true;
    }
    if (profile.type_ref().extension_name().empty()) {
      profile.set_type_ref(
          TypeDescriptorKeyPrefix(profile.type_name(), resolved_type_ext));
    }

    // Resolve index type reference: check same-batch index types only.
    // TODO(villagesql-indexing): Also resolve against committed index types
    // from the same extension once cross-extension profile references are
    // designed.
    std::string resolved_index_type_ext;
    int index_type_count = 0;
    for (const auto &itd : preview.index_types) {
      if (profile.index_type_ref().matches_key(itd.key().str())) {
        if (index_type_count == 0)
          resolved_index_type_ext = itd.extension_name();
        index_type_count++;
      }
    }
    if (index_type_count == 0) {
      error_out = "index profile '" + prof_name +
                  "': references unknown index type '" +
                  profile.index_type_name() + "'";
      LogVSQL(ERROR_LEVEL, "Extension '%s': %s", prof_ext.c_str(),
              error_out.c_str());
      return true;
    }
    if (index_type_count > 1) {
      error_out = "index profile '" + prof_name + "': index type '" +
                  profile.index_type_name() +
                  "' is ambiguous; qualify as 'extension.index_type_name'";
      LogVSQL(ERROR_LEVEL, "Extension '%s': %s", prof_ext.c_str(),
              error_out.c_str());
      return true;
    }
    // Qualify the index_type_ref if the extension was unqualified at parse
    // time; all descriptors must have a non-empty extension name.
    if (profile.index_type_ref().extension_name().empty()) {
      profile.set_index_type_ref(IndexTypeDescriptorKeyPrefix(
          profile.index_type_name(), resolved_index_type_ext));
    }
  }

  for (auto &descriptor : preview.index_types) {
    std::string index_type_name = descriptor.index_type_name();
    std::string ext_name = descriptor.extension_name();

    LogVSQL(INFORMATION_LEVEL,
            "Registering index type '%s' from extension '%s'",
            index_type_name.c_str(), ext_name.c_str());

    const IndexTypeDescriptor *existing =
        victionary.index_type_descriptors().get_committed(descriptor.key());
    if (existing) {
      error_out = "index type '" + index_type_name + "' already exists";
      LogVSQL(ERROR_LEVEL, "Extension '%s': %s", ext_name.c_str(),
              error_out.c_str());
      return true;
    }

    if (victionary.index_type_descriptors().MarkForInsertion(
            thd, std::move(descriptor))) {
      error_out = "failed to register index type '" + index_type_name + "'";
      LogVSQL(ERROR_LEVEL, "Extension '%s': %s", ext_name.c_str(),
              error_out.c_str());
      return true;
    }

    LogVSQL(INFORMATION_LEVEL, "Successfully registered index type '%s'",
            index_type_name.c_str());
  }

  for (auto &descriptor : preview.index_profiles) {
    std::string profile_name = descriptor.profile_name();
    std::string ext_name = descriptor.extension_name();

    LogVSQL(INFORMATION_LEVEL,
            "Registering index profile '%s' from extension '%s'",
            profile_name.c_str(), ext_name.c_str());

    const IndexProfileDescriptor *existing =
        victionary.index_profile_descriptors().get_committed(descriptor.key());
    if (existing) {
      error_out = "index profile '" + profile_name + "' already exists";
      LogVSQL(ERROR_LEVEL, "Extension '%s': %s", ext_name.c_str(),
              error_out.c_str());
      return true;
    }

    if (victionary.index_profile_descriptors().MarkForInsertion(
            thd, std::move(descriptor))) {
      error_out = "failed to register index profile '" + profile_name + "'";
      LogVSQL(ERROR_LEVEL, "Extension '%s': %s", ext_name.c_str(),
              error_out.c_str());
      return true;
    }

    LogVSQL(INFORMATION_LEVEL, "Successfully registered index profile '%s'",
            profile_name.c_str());
  }

  return false;
}

}  // namespace veb
}  // namespace villagesql
