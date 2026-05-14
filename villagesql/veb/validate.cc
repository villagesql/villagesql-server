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

#include <optional>
#include <string>

#include "sql/field.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/descriptor/func_descriptor.h"
#include "villagesql/schema/descriptor/type_descriptor.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"
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

std::optional<ValidatedRegistration> validate_extension_registration(
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

      bool is_v2 = td->protocol >= VEF_PROTOCOL_2 &&
                   ext_reg.negotiated_protocol >= VEF_PROTOCOL_2;
      std::optional<TypeDescriptor> maybe_descriptor =
          is_v2 ? build_type_descriptor_v2(td, *reg, type_name, extension_name,
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

      // clear/accumulate fields were added in PROTOCOL_2; older extensions
      // don't initialize them so we must not read them.
      if (ext_reg.negotiated_protocol >= VEF_PROTOCOL_2) {
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

  return result;
}

bool validate_sys_var_descriptors(const std::string &extension_name,
                                  const vef_registration_t *reg,
                                  std::string &error_out) {
  for (unsigned int i = 0; i < reg->sys_var_count; i++) {
    const vef_sys_var_desc_t *v = reg->sys_vars[i];
    if (v == nullptr || v->name == nullptr) {
      error_out =
          "NULL system variable descriptor at index " + std::to_string(i);
      LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
              error_out.c_str());
      return true;
    }
  }
  return false;
}

}  // namespace veb
}  // namespace villagesql
