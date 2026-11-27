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

#include <algorithm>
#include <cassert>
#include <cstring>
#include <optional>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "sql/field.h"
#include "sql/sql_const.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/descriptor/func_descriptor.h"
#include "villagesql/schema/descriptor/index_profile_descriptor.h"
#include "villagesql/schema/descriptor/index_type_descriptor.h"
#include "villagesql/schema/descriptor/type_descriptor.h"
#include "villagesql/schema/identifier_names.h"
#include "villagesql/sdk/include/villagesql/abi/preview/index.h"
#include "villagesql/sdk/include/villagesql/abi/preview/storage.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"
#include "villagesql/types/storage.h"
#include "villagesql/veb/veb_file.h"
#include "villagesql/veb/veb_register_type.h"

namespace villagesql {
namespace veb {

// A custom column is backed by a VARBINARY field, so it costs its declared
// width plus 2 length bytes against MySQL's 65535-byte per-row record budget,
// and the row spends at least one more byte on its null bitmap unless every
// column is NOT NULL. Whether a column is nullable is decided per use at DDL
// time -- and nullable is the default -- so the widest footprint a type can
// declare and still be usable either way is the field width less that overhead.
//
constexpr int64_t kVarbinaryLengthBytes = 2;
constexpr int64_t kRowNullByte = 1;
constexpr int64_t kMaxCustomTypeDeclaredLength =
    MAX_FIELD_VARCHARLENGTH - kVarbinaryLengthBytes - kRowNullByte;

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
    case VEF_TYPE_BINARY:
      return STRING_RESULT;
    default:
      return STRING_RESULT;
  }
}

// Returns the type `name` among the types this extension registers, or
// nullptr.
static const TypeDescriptor *find_registered_type(
    const std::vector<TypeDescriptor> &types, const char *name) {
  if (name == nullptr) return nullptr;
  const std::string canonical = canonical_type_name(name);
  for (const TypeDescriptor &type : types) {
    if (canonical_type_name(type.type_name()) == canonical) return &type;
  }
  return nullptr;
}

// A cast function is reached only through CAST(x AS T), which finds it by its
// (argument type, return type) pair, so it must have exactly one argument and
// be the only cast function for that pair. A STRING or BINARY argument already
// has its route to a custom type, T::from_string and T::from_binary. The hook
// is required when either type is parameterized, because the result's
// parameters must be known before any row is read. Returns true and fills
// error_out when a rule is broken.
static bool check_cast_function(
    const vef_func_desc_t &func_desc, const std::vector<TypeDescriptor> &types,
    std::set<std::pair<std::string, std::string>> &cast_pairs,
    std::string &error_out) {
  const std::string func_name(func_desc.name);
  const vef_signature_t &signature = *func_desc.signature;
  if (signature.param_count != 1) {
    error_out = "cast function '" + func_name + "' must take one argument";
    return true;
  }
  const vef_type_t &source = signature.params[0];
  if (source.id == VEF_TYPE_STRING || source.id == VEF_TYPE_BINARY) {
    error_out = "cast function '" + func_name +
                "' cannot take a STRING or BINARY argument; those reach a "
                "custom type through its from_string or from_binary";
    return true;
  }
  const TypeDescriptor *target =
      signature.return_type.id == VEF_TYPE_CUSTOM
          ? find_registered_type(types, signature.return_type.custom_type)
          : nullptr;
  if (target == nullptr) {
    error_out = "cast function '" + func_name +
                "' must return a custom type of this extension";
    return true;
  }
  if (!func_desc.deterministic) {
    error_out = "cast function '" + func_name + "' must be deterministic";
    return true;
  }
  const TypeDescriptor *source_type =
      source.id == VEF_TYPE_CUSTOM
          ? find_registered_type(types, source.custom_type)
          : nullptr;
  if (source.id == VEF_TYPE_CUSTOM && source_type == nullptr) {
    error_out =
        "cast function '" + func_name + "' must take a type of this extension";
    return true;
  }
  const bool parameterized =
      target->is_parameterized() ||
      (source_type != nullptr && source_type->is_parameterized());
  if (parameterized && func_desc.bind_and_check_types == nullptr) {
    error_out = "cast function '" + func_name +
                "' needs a bind_and_check_types hook, because its types have "
                "parameters";
    return true;
  }
  const std::string source_key =
      source_type != nullptr ? canonical_type_name(source_type->type_name())
                             : std::to_string(source.id);
  if (!cast_pairs.emplace(source_key, canonical_type_name(target->type_name()))
           .second) {
    error_out = "cast function '" + func_name +
                "' converts the same types as another cast function";
    return true;
  }
  return false;
}

std::optional<ValidatedRegistration> parse_extension_registration(
    const ExtensionRegistration &ext_reg, const std::string &extension_name,
    const std::string &extension_version, std::string &error_out) {
  ValidatedRegistration result;
  const vef_registration_t *reg = ext_reg.registration;

  // VDF names claimed by a type as its intrinsic default
  std::set<std::string> intrinsic_default_vdf_names;

  // check_vef_registration() has already established that the counts match the
  // arrays and that every descriptor is non-NULL, named and structurally
  // complete, so the loops below index and dereference them directly.
  assert(reg == nullptr || reg->func_count == 0 || reg->funcs != nullptr);
  assert(reg == nullptr || reg->type_count == 0 || reg->types != nullptr);

  if (reg != nullptr && reg->type_count > 0) {
    LogVSQL(INFORMATION_LEVEL,
            "Validating %d types from extension '%s' version '%s'",
            reg->type_count, extension_name.c_str(), extension_version.c_str());

    for (unsigned int i = 0; i < reg->type_count; i++) {
      // Non-NULL, named, and with a usable max_decode_buffer_length: see the
      // precondition in validate.h.
      const vef_type_desc_t *td = reg->types[i];
      std::string type_name(td->name);

      // TODO(villagesql-production): validate the characters in names coming
      // from the extension: type names here, and index type, index profile and
      // VDF names below. INSTALL EXTENSION already validates extension names
      // this way. A name containing '.' breaks two things:
      // parse_qualified_name() splits a profile's type reference on the first
      // dot, and the victionary keys join their components with an unescaped
      // '.' (see the TODO on TypeDescriptorKey, which tracks the escaping fix
      // separately. That is needed regardless, since the version component is
      // not validated either).

      bool is_v5 = td->protocol >= VEF_PROTOCOL_5 &&
                   ext_reg.negotiated_protocol >= VEF_PROTOCOL_5;
      bool is_v3 = !is_v5 && (td->protocol >= VEF_PROTOCOL_3 &&
                              ext_reg.negotiated_protocol >= VEF_PROTOCOL_3);
      std::optional<TypeDescriptor> maybe_descriptor =
          is_v5 ? build_type_descriptor_v5(td, *reg, type_name, extension_name,
                                           extension_version,
                                           ext_reg.negotiated_protocol)
          : is_v3 ? build_type_descriptor_v3(td, *reg, type_name,
                                             extension_name, extension_version,
                                             ext_reg.negotiated_protocol)
                  : build_type_descriptor_v1(td, type_name, extension_name,
                                             extension_version,
                                             ext_reg.negotiated_protocol);
      if (!maybe_descriptor.has_value()) {
        error_out = "type '" + type_name + "' failed validation";
        return std::nullopt;
      }

      // A fixed non-parameterized type declares persisted_length
      // (max_persisted_length stays 0); parameterized and variable-length types
      // declare max_persisted_length, and a parameterized type's resolved
      // persisted_length is checked against it at DDL time, so bounding the
      // declared values bounds every parameterization.
      // A version update will reject a change in a type's length in
      // check_retained_types_storage_length().
      const int64_t declared_length =
          std::max(maybe_descriptor->persisted_length(),
                   maybe_descriptor->max_persisted_length());
      if (declared_length > kMaxCustomTypeDeclaredLength) {
        error_out =
            "type '" + type_name + "' declares " +
            std::to_string(declared_length) +
            " bytes of storage per value, which exceeds the maximum of " +
            std::to_string(kMaxCustomTypeDeclaredLength) +
            " bytes supported for custom-type columns today";
        LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
                error_out.c_str());
        return std::nullopt;
      }

      if ((is_v5 || is_v3) && td->intrinsic_default_vdf_name != nullptr)
        intrinsic_default_vdf_names.insert(td->intrinsic_default_vdf_name);

      result.types.push_back(std::move(*maybe_descriptor));
    }
  }

  if (reg != nullptr && reg->func_count > 0) {
    // (argument type, return type) of each cast function seen so far.
    std::set<std::pair<std::string, std::string>> cast_pairs;
    LogVSQL(INFORMATION_LEVEL, "Validating %d VDFs from extension '%s'",
            reg->func_count, extension_name.c_str());

    for (unsigned int i = 0; i < reg->func_count; i++) {
      // Non-NULL, named, and carrying a signature: see the precondition in
      // validate.h.
      const vef_func_desc_t *func_desc = reg->funcs[i];
      std::string func_name(func_desc->name);

      // clear/accumulate were added in PROTOCOL_3. The negotiated protocol is
      // the only gate needed: check_vef_registration() has already rejected a
      // descriptor declaring less than its registration, so a v3+ negotiation
      // means this descriptor really has these fields.
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

      // cast_function was added in PROTOCOL_6.
      const bool is_cast_function =
          ext_reg.negotiated_protocol >= VEF_PROTOCOL_6 &&
          func_desc->cast_function;
      if (is_cast_function && check_cast_function(*func_desc, result.types,
                                                  cast_pairs, error_out)) {
        LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
                error_out.c_str());
        return std::nullopt;
      }

      Item_result return_type =
          vef_type_to_item_result(func_desc->signature->return_type);
      FuncKey key(func_name, extension_name);
      result.funcs.push_back(FuncDescriptor(key, extension_version, func_desc,
                                            ext_reg.negotiated_protocol,
                                            return_type));
      // CAST is the only way to call a cast function.
      if (is_cast_function) result.funcs.back().set_not_callable_from_sql();
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

  // Close the SQL route to every VDF a type claims as its intrinsic default.
  // A name that resolves to no function has already failed validation in the
  // type builder, so each name here matches exactly one entry.
  for (FuncDescriptor &func : result.funcs) {
    if (intrinsic_default_vdf_names.contains(func.function_name())) {
      func.set_not_callable_from_sql();
    }
  }

  return result;
}

std::optional<ValidatedPreviewCapabilities> parse_preview_capabilities(
    const ExtensionRegistration &ext_reg, const std::string &extension_name,
    const std::string &extension_version, std::string &error_out) {
  ValidatedPreviewCapabilities result;
  const vef_registration_t *reg = ext_reg.registration;

  if (reg == nullptr || ext_reg.negotiated_protocol < VEF_PROTOCOL_3 ||
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

      auto parse_bindings =
          [&](uint32_t count, const vef_index_profile_fn_binding_t *arr,
              const char *kind, bool is_helper,
              std::vector<vef_index_profile_fn_binding_t> &out) -> bool {
        out.reserve(count);
        for (unsigned int k = 0; k < count; k++) {
          const vef_index_profile_fn_binding_t &fn = arr[k];
          if (k > 0 && fn.fn_id == arr[k - 1].fn_id) {
            error_out = std::string("index profile '") + profile.name +
                        "': " + kind + " '" + fn.name +
                        "' has duplicate fn_id " + std::to_string(fn.fn_id);
            LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
                    error_out.c_str());
            return true;
          }
          if (fn.name == nullptr) {
            error_out = std::string("index profile '") + profile.name +
                        "': " + kind + " binding at index " +
                        std::to_string(k) + " has NULL name";
            LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
                    error_out.c_str());
            return true;
          }
          if (fn.vdf == nullptr) {
            error_out = std::string("index profile '") + profile.name +
                        "': " + kind + " '" + fn.name +
                        "' has NULL vdf pointer";
            LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
                    error_out.c_str());
            return true;
          }
          if (fn.signature.param_count > 0 && fn.signature.params == nullptr) {
            error_out = std::string("index profile '") + profile.name +
                        "': " + kind + " '" + fn.name +
                        "' has non-zero param_count but NULL params";
            LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
                    error_out.c_str());
            return true;
          }
          if (fn.signature.param_count > VEF_INDEX_PROFILE_FN_MAX_ARGS) {
            error_out = std::string("index profile '") + profile.name +
                        "': " + kind + " '" + fn.name + "' declares " +
                        std::to_string(fn.signature.param_count) +
                        " parameters, exceeding the maximum of " +
                        std::to_string(VEF_INDEX_PROFILE_FN_MAX_ARGS);
            LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
                    error_out.c_str());
            return true;
          }
          // TODO(villagesql-indexing): Lift this restriction once
          // call_profile_binding() in custom_index.cc supports STRING/INT/
          // CUSTOM results, not just REAL.
          if (fn.signature.return_type.id != VEF_TYPE_REAL) {
            error_out = std::string("index profile '") + profile.name +
                        "': " + kind + " '" + fn.name +
                        "' has return type other than REAL, which is not yet "
                        "supported for index profile/helper functions";
            LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
                    error_out.c_str());
            return true;
          }
          // TODO(villagesql-indexing): Support RESTRICT profile functions.
          if (!is_helper && fn.kind == VEF_INDEX_PROFILE_FN_KIND_RESTRICT) {
            error_out = std::string("index profile '") + profile.name +
                        "': " + kind + " '" + fn.name +
                        "' has kind RESTRICT, which is not yet implemented";
            LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
                    error_out.c_str());
            return true;
          }
          const bool kind_valid =
              is_helper ? fn.kind == VEF_INDEX_PROFILE_FN_KIND_INVALID
                        : fn.kind == VEF_INDEX_PROFILE_FN_KIND_ORDER;
          if (!kind_valid) {
            error_out = std::string("index profile '") + profile.name +
                        "': " + kind + " '" + fn.name + "' has invalid kind " +
                        std::to_string(fn.kind);
            LogVSQL(ERROR_LEVEL, "Extension '%s': %s", extension_name.c_str(),
                    error_out.c_str());
            return true;
          }
          out.push_back(fn);
        }
        return false;
      };

      std::vector<vef_index_profile_fn_binding_t> functions;
      if (parse_bindings(profile.function_count, profile.functions, "function",
                         false, functions)) {
        return std::nullopt;
      }

      std::vector<vef_index_profile_fn_binding_t> helpers;
      if (parse_bindings(profile.helper_count, profile.helpers, "helper", true,
                         helpers)) {
        return std::nullopt;
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
          std::move(helpers), profile.ordering, profile.default_for_type != 0);
    }
    break;
  }

  return result;
}

}  // namespace veb
}  // namespace villagesql
