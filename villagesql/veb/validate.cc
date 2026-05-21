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

#include "villagesql/veb/validate.h"

#include <cstring>
#include <optional>
#include <string>

#include "sql/field.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/descriptor/func_descriptor.h"
#include "villagesql/schema/descriptor/index_profile_descriptor.h"
#include "villagesql/schema/descriptor/index_type_descriptor.h"
#include "villagesql/schema/descriptor/type_descriptor.h"
#include "villagesql/sdk/include/villagesql/abi/preview/index.h"
#include "villagesql/sdk/include/villagesql/abi/preview/storage.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"
#include "villagesql/types/storage.h"
#include "villagesql/veb/veb_file.h"
#include "villagesql/veb/veb_register_type.h"

namespace villagesql {
namespace veb {

static Item_result vef_type_to_item_result(const vef_type_t &type) {
  switch (type.id) {
    case VEF_TYPE_STRING:
      return STRING_RESULT;
    case VEF_TYPE_REAL:
      return REAL_RESULT;
    case VEF_TYPE_INT:
      return INT_RESULT;
    case VEF_TYPE_CUSTOM:
      // Custom types are passed as binary strings
      return STRING_RESULT;
    default:
      return STRING_RESULT;
  }
}

std::optional<ValidatedRegistration> parse_extension_registration(
    const ExtensionRegistration &ext_reg, const std::string &extension_name,
    const std::string &extension_version, std::string &error_out) {
  ValidatedRegistration result;
  const vef_registration_t *reg = ext_reg.registration;

  if (reg != nullptr && reg->type_count > 0) {
    LogVSQL(INFORMATION_LEVEL,
            "Validating %d types from extension '%s' version '%s'",
            reg->type_count, extension_name.c_str(), extension_version.c_str());

    for (unsigned int i = 0; i < reg->type_count; i++) {
      const vef_type_desc_t *td = reg->types[i];
      if (td == nullptr || td->name == nullptr) {
        error_out = "NULL type descriptor at index " + std::to_string(i);
        LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
                error_out.c_str());
        return std::nullopt;
      }

      std::string type_name(td->name);

      if (td->max_decode_buffer_length <= 0) {
        error_out =
            "type '" + type_name + "' must set max_decode_buffer_length";
        LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
                error_out.c_str());
        return std::nullopt;
      }

      bool is_v3 = td->protocol >= VEF_PROTOCOL_3 &&
                   ext_reg.negotiated_protocol >= VEF_PROTOCOL_3;
      std::optional<TypeDescriptor> maybe_descriptor =
          is_v3 ? build_type_descriptor_v3(td, *reg, type_name, extension_name,
                                           extension_version)
                : build_type_descriptor_v1(td, type_name, extension_name,
                                           extension_version);
      if (!maybe_descriptor.has_value()) {
        error_out = "type '" + type_name + "' failed validation";
        return std::nullopt;
      }
      result.types.push_back(std::move(*maybe_descriptor));
    }
  }

  if (reg != nullptr && reg->func_count > 0) {
    LogVSQL(INFORMATION_LEVEL, "Validating %d VDFs from extension '%s'",
            reg->func_count, extension_name.c_str());

    for (unsigned int i = 0; i < reg->func_count; i++) {
      const vef_func_desc_t *func_desc = reg->funcs[i];
      if (func_desc == nullptr || func_desc->name == nullptr) {
        error_out = "NULL VDF descriptor at index " + std::to_string(i);
        LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
                error_out.c_str());
        return std::nullopt;
      }

      std::string func_name(func_desc->name);

      // clear/accumulate fields were added in PROTOCOL_3; older extensions
      // don't initialize them so we must not read them.
      if (ext_reg.negotiated_protocol >= VEF_PROTOCOL_3) {
        bool has_clear = (func_desc->clear != nullptr);
        bool has_accumulate = (func_desc->accumulate != nullptr);
        if (has_clear != has_accumulate) {
          error_out = "VDF '" + func_name +
                      "' must provide both clear and accumulate callbacks, or "
                      "neither";
          LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
                  error_out.c_str());
          return std::nullopt;
        }
      }

      Item_result return_type =
          vef_type_to_item_result(func_desc->signature->return_type);
      FuncKey key(func_name, extension_name);
      result.funcs.push_back(FuncDescriptor(key, extension_version, func_desc,
                                            ext_reg.negotiated_protocol,
                                            return_type));
    }
  }

  // Wire column storage implementations to types via the storage capability
  // extension descriptor. The descriptor lives in the capability_config field
  // of the vsql::preview::column_store capability entry.
  if (reg != nullptr && ext_reg.negotiated_protocol >= VEF_PROTOCOL_3 &&
      reg->required_capability_count > 0) {
    for (unsigned int i = 0; i < reg->required_capability_count; i++) {
      const vef_required_capability_t &cap = reg->required_capabilities[i];
      if (cap.name == nullptr ||
          strcmp(cap.name, VEF_PREVIEW_COLUMN_STORE_NAME) != 0 ||
          cap.capability_config == nullptr) {
        continue;
      }

      const auto *ext_desc =
          static_cast<const vef_preview_column_store_ext_desc_t *>(
              cap.capability_config);

      if (ext_desc->version != VEF_COLUMN_STORE_INTF_VERSION) {
        error_out = "column_store capability has unsupported version " +
                    std::to_string(ext_desc->version) + " (expected " +
                    std::to_string(VEF_COLUMN_STORE_INTF_VERSION) + ")";
        LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
                error_out.c_str());
        return std::nullopt;
      }

      for (unsigned int j = 0; j < ext_desc->type_storage_count; j++) {
        const vef_type_storage_intf_t *sd = ext_desc->type_storages[j];
        if (sd == nullptr || sd->type_name == nullptr) {
          error_out = "NULL type storage descriptor at index " +
                      std::to_string(j) + " in storage capability";
          LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
                  error_out.c_str());
          return std::nullopt;
        }

        if (!sd->create || !sd->drop || !sd->load || !sd->insert ||
            !sd->select || !sd->mark_delete || !sd->purge) {
          error_out = std::string("type storage for '") + sd->type_name +
                      "': not all storage functions are registered";
          LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
                  error_out.c_str());
          return std::nullopt;
        }

        bool found = false;
        for (auto &descriptor : result.types) {
          if (descriptor.type_name() == sd->type_name) {
            if (descriptor.storage_intf().has_value()) {
              error_out =
                  std::string("duplicate storage descriptor for type '") +
                  sd->type_name + "'";
              LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
                      error_out.c_str());
              return std::nullopt;
            }
            descriptor.set_storage_intf(StorageInterface(*sd));
            found = true;
            break;
          }
        }
        if (!found) {
          error_out = std::string("type storage refers to unknown type '") +
                      sd->type_name + "'";
          LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
                  error_out.c_str());
          return std::nullopt;
        }
      }
      break;
    }
  }

  return result;
}

std::optional<ValidatedPreviewCapabilities> parse_preview_capabilities(
    const ExtensionRegistration &ext_reg, const std::string &extension_name,
    const std::string &extension_version, std::string &error_out) {
  ValidatedPreviewCapabilities result;
  const vef_registration_t *reg = ext_reg.registration;

  if (reg == nullptr || ext_reg.negotiated_protocol < VEF_PROTOCOL_2 ||
      reg->required_capability_count == 0) {
    return result;
  }

  // Parse vsql::preview::index_type capability.
  for (unsigned int i = 0; i < reg->required_capability_count; i++) {
    const vef_required_capability_t &cap = reg->required_capabilities[i];
    if (cap.name == nullptr ||
        strcmp(cap.name, VEF_PREVIEW_INDEX_TYPE_NAME) != 0 ||
        cap.capability_config == nullptr) {
      continue;
    }

    const auto *ext_desc =
        static_cast<const vef_preview_index_type_ext_desc_t *>(
            cap.capability_config);

    if (ext_desc->version != VEF_PREVIEW_INDEX_TYPE_ABI_VERSION) {
      error_out = "index_type capability has unsupported version " +
                  std::to_string(ext_desc->version) + " (expected " +
                  std::to_string(VEF_PREVIEW_INDEX_TYPE_ABI_VERSION) + ")";
      LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
              error_out.c_str());
      return std::nullopt;
    }

    for (unsigned int j = 0; j < ext_desc->count; j++) {
      const vef_index_type_reg_t &reg_entry = ext_desc->types[j];
      if (reg_entry.name == nullptr || reg_entry.intf == nullptr) {
        error_out = "NULL index type entry at index " + std::to_string(j) +
                    " in index_type capability";
        LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
                error_out.c_str());
        return std::nullopt;
      }

      const vef_type_index_intf_t *intf = reg_entry.intf;
      if (!intf->create || !intf->drop || !intf->load || !intf->insert ||
          !intf->mark_delete || !intf->purge || !intf->scan_begin ||
          !intf->scan_position || !intf->scan_fetch || !intf->scan_save ||
          !intf->scan_restore || !intf->scan_end) {
        error_out = std::string("index type '") + reg_entry.name +
                    "': not all required function pointers are set";
        LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
                error_out.c_str());
        return std::nullopt;
      }

      result.index_types.emplace_back(
          IndexTypeDescriptorKey(reg_entry.name, extension_name,
                                 extension_version),
          *intf);
    }
    break;
  }

  // Parse vsql::preview::index_profile capability.
  for (unsigned int i = 0; i < reg->required_capability_count; i++) {
    const vef_required_capability_t &cap = reg->required_capabilities[i];
    if (cap.name == nullptr ||
        strcmp(cap.name, VEF_PREVIEW_INDEX_PROFILE_NAME) != 0 ||
        cap.capability_config == nullptr) {
      continue;
    }

    const auto *ext_desc =
        static_cast<const vef_preview_index_profile_ext_desc_t *>(
            cap.capability_config);

    if (ext_desc->version != VEF_PREVIEW_INDEX_PROFILE_ABI_VERSION) {
      error_out = "index_profile capability has unsupported version " +
                  std::to_string(ext_desc->version) + " (expected " +
                  std::to_string(VEF_PREVIEW_INDEX_PROFILE_ABI_VERSION) + ")";
      LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
              error_out.c_str());
      return std::nullopt;
    }

    for (unsigned int j = 0; j < ext_desc->count; j++) {
      const vef_index_profile_reg_t &profile = ext_desc->profiles[j];
      if (profile.name == nullptr || profile.type_name == nullptr ||
          profile.index_type_name == nullptr) {
        error_out = "NULL index profile entry at index " + std::to_string(j) +
                    " in index_profile capability";
        LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
                error_out.c_str());
        return std::nullopt;
      }

      std::vector<vef_index_profile_fn_binding_t> functions;
      functions.reserve(profile.function_count);
      for (unsigned int k = 0; k < profile.function_count; k++) {
        const vef_index_profile_fn_binding_t &fn = profile.functions[k];
        if (fn.name == nullptr) {
          error_out = std::string("index profile '") + profile.name +
                      "': function binding at index " + std::to_string(k) +
                      " has NULL name";
          LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
                  error_out.c_str());
          return std::nullopt;
        }
        if (fn.vdf == nullptr) {
          error_out = std::string("index profile '") + profile.name +
                      "': function '" + fn.name + "' has NULL vdf pointer";
          LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
                  error_out.c_str());
          return std::nullopt;
        }
        if (fn.num_params > VEF_INDEX_PROFILE_MAX_FN_PARAMS) {
          error_out = std::string("index profile '") + profile.name +
                      "': function '" + fn.name + "' declares " +
                      std::to_string(fn.num_params) + " params, max is " +
                      std::to_string(VEF_INDEX_PROFILE_MAX_FN_PARAMS);
          LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
                  error_out.c_str());
          return std::nullopt;
        }
        functions.push_back(fn);
      }

      auto [type_ext, type_bare] = parse_qualified_name(profile.type_name);
      TypeDescriptorKeyPrefix type_ref =
          type_ext.empty() ? TypeDescriptorKeyPrefix(type_bare)
                           : TypeDescriptorKeyPrefix(type_bare, type_ext);

      auto [idx_ext, idx_bare] = parse_qualified_name(profile.index_type_name);
      IndexTypeDescriptorKeyPrefix index_type_ref =
          idx_ext.empty() ? IndexTypeDescriptorKeyPrefix(idx_bare)
                          : IndexTypeDescriptorKeyPrefix(idx_bare, idx_ext);

      result.index_profiles.emplace_back(
          IndexProfileDescriptorKey(profile.name, extension_name,
                                    extension_version),
          std::move(type_ref), std::move(index_type_ref), std::move(functions),
          profile.ordering_asc != 0, profile.default_for_type != 0);
    }
    break;
  }

  return result;
}

}  // namespace veb
}  // namespace villagesql
