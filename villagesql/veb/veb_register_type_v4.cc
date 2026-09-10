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

// Protocol-4 type registration: builds a TypeDescriptor from a
// vef_type_desc_t that may use VDF names in addition to (or instead
// of) direct function pointers.
//
// All protocol-1, protocol-3 and protocol-4 fields are read from td.

#include "villagesql/veb/veb_register_type.h"

#include <cctype>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>

#include "sql/field.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/descriptor/type_descriptor.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"
#include "villagesql/types/type_function.h"

namespace villagesql {
namespace veb {

static const vef_func_desc_t *find_vdf_by_name(const vef_registration_t &reg,
                                               const char *name) {
  for (unsigned int i = 0; i < reg.func_count; i++) {
    const vef_func_desc_t *fd = reg.funcs[i];
    if (fd != nullptr && fd->name != nullptr && strcmp(fd->name, name) == 0) {
      return fd;
    }
  }
  return nullptr;
}

// Validate a type method VDF name that uses the "::" convention.
// If the name contains "::", checks:
//   - prefix (before "::") matches type_name (case-insensitive)
//   - suffix (after "::") is one of: from_string, to_string, compare, hash,
//     real_value
// Returns false on success, true on validation error.
// Names without "::" are always accepted (backward compatibility).
static bool validate_type_method_vdf_name(const char *vdf_name,
                                          const char *type_name,
                                          const char *field_name,
                                          const std::string &extension_name) {
  std::string_view name(vdf_name);
  size_t sep = name.find("::");
  if (sep == std::string_view::npos) {
    return false;
  }

  std::string_view prefix = name.substr(0, sep);
  std::string_view suffix = name.substr(sep + 2);

  std::string_view tname(type_name);
  if (prefix.size() != tname.size()) {
    LogVSQL(ERROR_LEVEL,
            "Type '%s' in extension '%s': %s '%s' uses '::' but prefix '%.*s' "
            "does not match type name (case-insensitive)",
            type_name, extension_name.c_str(), field_name, vdf_name,
            static_cast<int>(prefix.size()), prefix.data());
    return true;
  }
  for (size_t i = 0; i < prefix.size(); i++) {
    if (std::tolower(static_cast<unsigned char>(prefix[i])) !=
        std::tolower(static_cast<unsigned char>(tname[i]))) {
      LogVSQL(ERROR_LEVEL,
              "Type '%s' in extension '%s': %s '%s' uses '::' but prefix "
              "'%.*s' does not match type name (case-insensitive)",
              type_name, extension_name.c_str(), field_name, vdf_name,
              static_cast<int>(prefix.size()), prefix.data());
      return true;
    }
  }

  if (suffix != "from_string" && suffix != "to_string" && suffix != "compare" &&
      suffix != "hash" && suffix != "real_value") {
    LogVSQL(
        ERROR_LEVEL,
        "Type '%s' in extension '%s': %s '%s' uses '::' but suffix "
        "'%.*s' is not a valid type method (must be from_string, to_string, "
        "compare, hash, or real_value)",
        type_name, extension_name.c_str(), field_name, vdf_name,
        static_cast<int>(suffix.size()), suffix.data());
    return true;
  }

  return false;
}

// Validate that a VDF's signature matches the expected pattern for a type
// operation. Returns false on success, true on error.
static bool validate_type_vdf_signature(
    const vef_func_desc_t *fd, const char *op_name, const char *type_name,
    unsigned int expected_param_count, const vef_type_id expected_param_ids[],
    const char *expected_custom_params[], vef_type_id expected_return_id,
    const char *expected_custom_return, const std::string &extension_name) {
  if (fd->signature == nullptr) {
    LogVSQL(ERROR_LEVEL,
            "VDF '%s' in extension '%s' used as %s for type '%s' has no "
            "signature",
            fd->name, extension_name.c_str(), op_name, type_name);
    return true;
  }

  if (fd->prerun != nullptr || fd->postrun != nullptr) {
    LogVSQL(ERROR_LEVEL,
            "VDF '%s' used as %s for type '%s' must not have prerun/postrun "
            "hooks",
            fd->name, op_name, type_name);
    return true;
  }

  const vef_signature_t *sig = fd->signature;

  if (sig->param_count != expected_param_count) {
    LogVSQL(ERROR_LEVEL,
            "VDF '%s' used as %s for type '%s' must have %u parameter(s), "
            "has %u",
            fd->name, op_name, type_name, expected_param_count,
            sig->param_count);
    return true;
  }

  for (unsigned int i = 0; i < expected_param_count; i++) {
    if (sig->params[i].id != expected_param_ids[i]) {
      LogVSQL(ERROR_LEVEL,
              "VDF '%s' used as %s for type '%s': parameter %u has wrong type",
              fd->name, op_name, type_name, i);
      return true;
    }
    if (expected_custom_params[i] != nullptr &&
        (sig->params[i].custom_type == nullptr ||
         strcmp(sig->params[i].custom_type, expected_custom_params[i]) != 0)) {
      LogVSQL(ERROR_LEVEL,
              "VDF '%s' used as %s for type '%s': parameter %u must be "
              "CUSTOM('%s')",
              fd->name, op_name, type_name, i, expected_custom_params[i]);
      return true;
    }
  }

  if (sig->return_type.id != expected_return_id) {
    LogVSQL(ERROR_LEVEL,
            "VDF '%s' used as %s for type '%s' has wrong return type", fd->name,
            op_name, type_name);
    return true;
  }

  if (expected_custom_return != nullptr &&
      (sig->return_type.custom_type == nullptr ||
       strcmp(sig->return_type.custom_type, expected_custom_return) != 0)) {
    LogVSQL(ERROR_LEVEL,
            "VDF '%s' used as %s for type '%s' must return CUSTOM('%s')",
            fd->name, op_name, type_name, expected_custom_return);
    return true;
  }

  return false;
}

// Helper to resolve a VDF name for a type operation that also has a direct
// function pointer.  Checks mutual exclusion (both set = error), validates
// the name convention, looks up the VDF, and validates its signature.
// Returns: resolved VDF pointer (or nullptr if not VDF-based).
// Sets *error = true on validation failure.
static const vef_func_desc_t *resolve_type_vdf(
    const char *vdf_name, const void *func_ptr, const char *func_label,
    const char *vdf_label, const vef_registration_t &reg,
    const vef_type_desc_t *td, const std::string &type_name,
    const std::string &extension_name, unsigned int expected_param_count,
    const vef_type_id expected_param_ids[],
    const char *expected_custom_params[], vef_type_id expected_return_id,
    const char *expected_custom_return, bool *error) {
  if (func_ptr != nullptr && vdf_name != nullptr) {
    LogVSQL(ERROR_LEVEL, "Type '%s' in extension '%s' sets both %s and %s",
            type_name.c_str(), extension_name.c_str(), func_label, vdf_label);
    *error = true;
    return nullptr;
  }
  if (vdf_name == nullptr) return nullptr;

  if (validate_type_method_vdf_name(vdf_name, td->name, vdf_label,
                                    extension_name)) {
    *error = true;
    return nullptr;
  }
  const vef_func_desc_t *vdf = find_vdf_by_name(reg, vdf_name);
  if (vdf == nullptr) {
    LogVSQL(ERROR_LEVEL, "Type '%s' in extension '%s': %s '%s' not found",
            type_name.c_str(), extension_name.c_str(), vdf_label, vdf_name);
    *error = true;
    return nullptr;
  }
  if (validate_type_vdf_signature(vdf, vdf_label, td->name,
                                  expected_param_count, expected_param_ids,
                                  expected_custom_params, expected_return_id,
                                  expected_custom_return, extension_name)) {
    *error = true;
    return nullptr;
  }
  return vdf;
}

std::optional<TypeDescriptor> build_type_descriptor_v4(
    const vef_type_desc_t *td, const vef_registration_t &reg,
    const std::string &type_name, const std::string &extension_name,
    const std::string &extension_version) {
  bool error = false;

  // A type is variable-length when it sets the variable_length flag (via the
  // builder's variable_length_type()).
  const bool is_variable = td->variable_length;

  if (is_variable) {
    // Variable-length type: its persisted size is decided per value, so
    // persisted_length is not a fixed footprint.
    // max_persisted_length bounds the backing field and is always required.
    // resolve_params is required only for parameterized variants (those that
    // also expose int_to_params, validated below); a bare variable-length type
    // whose length is decided per value needs only max_persisted_length.
    if (td->persisted_length > 0) {
      LogVSQL(ERROR_LEVEL,
              "Type '%s' in extension '%s' is variable-length but also sets a "
              "positive persisted_length=%lld (variable-length types must not "
              "declare a fixed persisted_length)",
              type_name.c_str(), extension_name.c_str(),
              static_cast<long long>(td->persisted_length));
      return std::nullopt;
    }
    if (td->max_persisted_length <= 0) {
      LogVSQL(ERROR_LEVEL,
              "Type '%s' in extension '%s' is variable-length but "
              "max_persisted_length is %lld (must be > 0)",
              type_name.c_str(), extension_name.c_str(),
              static_cast<long long>(td->max_persisted_length));
      return std::nullopt;
    }
  } else {
    // Fixed length type: persisted_length must be either -1 (discover during
    // resolve_params) or a positive value. 0 and other negatives are invalid.
    if (td->persisted_length != -1 && td->persisted_length <= 0) {
      LogVSQL(ERROR_LEVEL,
              "Type '%s' in extension '%s' has invalid persisted_length %lld "
              "(must be -1 for parameterized types or > 0 for "
              "non-parameterized types)",
              type_name.c_str(), extension_name.c_str(),
              static_cast<long long>(td->persisted_length));
      return std::nullopt;
    }
    if (td->persisted_length == -1) {
      // Fixed length, parameterized type: resolve_params_vdf_name and
      // max_persisted_length are both required.
      if (td->resolve_params_vdf_name == nullptr) {
        LogVSQL(ERROR_LEVEL,
                "Type '%s' in extension '%s' has persisted_length=-1 "
                "(parameterized) but does not set resolve_params_vdf_name",
                type_name.c_str(), extension_name.c_str());
        return std::nullopt;
      }
      if (td->max_persisted_length <= 0) {
        LogVSQL(ERROR_LEVEL,
                "Type '%s' in extension '%s' has persisted_length=-1 "
                "(parameterized) but max_persisted_length is %lld "
                "(must be > 0)",
                type_name.c_str(), extension_name.c_str(),
                static_cast<long long>(td->max_persisted_length));
        return std::nullopt;
      }
    } else {
      // Fixed-length, not parameterized types (persisted_length > 0):
      // resolve_params_vdf_name, int_to_params_vdf_name, and
      // max_persisted_length must not be set.
      if (td->resolve_params_vdf_name != nullptr) {
        LogVSQL(ERROR_LEVEL,
                "Type '%s' in extension '%s' has fixed persisted_length=%lld "
                "but also sets resolve_params_vdf_name '%s' (only valid for "
                "parameterized types with persisted_length=-1)",
                type_name.c_str(), extension_name.c_str(),
                static_cast<long long>(td->persisted_length),
                td->resolve_params_vdf_name);
        return std::nullopt;
      }
      if (td->int_to_params_vdf_name != nullptr) {
        LogVSQL(ERROR_LEVEL,
                "Type '%s' in extension '%s' has fixed persisted_length=%lld "
                "but also sets int_to_params_vdf_name '%s' (only valid for "
                "parameterized types with persisted_length=-1)",
                type_name.c_str(), extension_name.c_str(),
                static_cast<long long>(td->persisted_length),
                td->int_to_params_vdf_name);
        return std::nullopt;
      }
      if (td->max_persisted_length != 0) {
        LogVSQL(ERROR_LEVEL,
                "Type '%s' in extension '%s' has fixed persisted_length=%lld "
                "but also sets max_persisted_length=%lld (only valid for "
                "parameterized types with persisted_length=-1)",
                type_name.c_str(), extension_name.c_str(),
                static_cast<long long>(td->persisted_length),
                static_cast<long long>(td->max_persisted_length));
        return std::nullopt;
      }
    }
  }

  const vef_type_id custom_id[] = {VEF_TYPE_CUSTOM};
  const char *custom_name[] = {td->name};
  const vef_type_id two_custom_ids[] = {VEF_TYPE_CUSTOM, VEF_TYPE_CUSTOM};
  const char *two_custom_names[] = {td->name, td->name};
  const vef_type_id string_id[] = {VEF_TYPE_STRING};
  const char *no_custom[] = {nullptr};

  // 1. Resolve encode / decode / compare (required; VDF or function pointer).
  // TODO(villagesql-beta): Refactor this for less code duplication.
  const vef_func_desc_t *encode_vdf = resolve_type_vdf(
      td->encode_vdf_name, reinterpret_cast<const void *>(td->encode_func),
      "encode_func", "encode_vdf_name", reg, td, type_name, extension_name, 1,
      string_id, no_custom, VEF_TYPE_CUSTOM, td->name, &error);
  if (error) return std::nullopt;

  const vef_func_desc_t *decode_vdf = resolve_type_vdf(
      td->decode_vdf_name, reinterpret_cast<const void *>(td->decode_func),
      "decode_func", "decode_vdf_name", reg, td, type_name, extension_name, 1,
      custom_id, custom_name, VEF_TYPE_STRING, nullptr, &error);
  if (error) return std::nullopt;

  const vef_func_desc_t *compare_vdf = resolve_type_vdf(
      td->compare_vdf_name, reinterpret_cast<const void *>(td->compare_func),
      "compare_func", "compare_vdf_name", reg, td, type_name, extension_name, 2,
      two_custom_ids, two_custom_names, VEF_TYPE_INT, nullptr, &error);
  if (error) return std::nullopt;

  // 2. Resolve hash (optional; VDF or function pointer).
  const vef_func_desc_t *hash_vdf = resolve_type_vdf(
      td->hash_vdf_name, reinterpret_cast<const void *>(td->hash_func),
      "hash_func", "hash_vdf_name", reg, td, type_name, extension_name, 1,
      custom_id, custom_name, VEF_TYPE_INT, nullptr, &error);
  if (error) return std::nullopt;

  const vef_func_desc_t *real_value_vdf = nullptr;
  if (td->real_value_vdf_name != nullptr) {
    real_value_vdf = find_vdf_by_name(reg, td->real_value_vdf_name);
    if (real_value_vdf == nullptr) {
      LogVSQL(ERROR_LEVEL,
              "Type '%s' in extension '%s': real_value_vdf_name "
              "'%s' not found",
              type_name.c_str(), extension_name.c_str(),
              td->real_value_vdf_name);
      return std::nullopt;
    }
    if (validate_type_vdf_signature(real_value_vdf, "real_value", td->name, 1,
                                    custom_id, custom_name, VEF_TYPE_REAL,
                                    nullptr, extension_name)) {
      return std::nullopt;
    }
  }

  // 3. Resolve int_to_params / resolve_params (VDF only, no function pointer).
  const vef_func_desc_t *int_to_params_vdf = nullptr;
  if (td->int_to_params_vdf_name != nullptr) {
    const vef_type_id int_id[] = {VEF_TYPE_INT};
    int_to_params_vdf = find_vdf_by_name(reg, td->int_to_params_vdf_name);
    if (int_to_params_vdf == nullptr) {
      LogVSQL(ERROR_LEVEL,
              "Type '%s' in extension '%s': int_to_params_vdf_name "
              "'%s' not found",
              type_name.c_str(), extension_name.c_str(),
              td->int_to_params_vdf_name);
      return std::nullopt;
    }
    if (validate_type_vdf_signature(int_to_params_vdf, "int_to_params",
                                    td->name, 1, int_id, no_custom,
                                    VEF_TYPE_STRING, nullptr, extension_name)) {
      return std::nullopt;
    }
  }

  const vef_func_desc_t *resolve_params_vdf = nullptr;
  if (td->resolve_params_vdf_name != nullptr) {
    resolve_params_vdf = find_vdf_by_name(reg, td->resolve_params_vdf_name);
    if (resolve_params_vdf == nullptr) {
      LogVSQL(ERROR_LEVEL,
              "Type '%s' in extension '%s': resolve_params_vdf_name "
              "'%s' not found",
              type_name.c_str(), extension_name.c_str(),
              td->resolve_params_vdf_name);
      return std::nullopt;
    }
    if (validate_type_vdf_signature(resolve_params_vdf, "resolve_params",
                                    td->name, 1, string_id, no_custom,
                                    VEF_TYPE_STRING, nullptr, extension_name)) {
      return std::nullopt;
    }
  }

  // 4. Resolve intrinsic default (VDF or string literal).
  const vef_func_desc_t *intrinsic_default_vdf = nullptr;
  if (td->intrinsic_default_vdf_name != nullptr) {
    intrinsic_default_vdf =
        find_vdf_by_name(reg, td->intrinsic_default_vdf_name);
    if (intrinsic_default_vdf == nullptr) {
      LogVSQL(ERROR_LEVEL,
              "Type '%s' in extension '%s': intrinsic_default_vdf_name "
              "'%s' not found",
              type_name.c_str(), extension_name.c_str(),
              td->intrinsic_default_vdf_name);
      return std::nullopt;
    }
    if (validate_type_vdf_signature(intrinsic_default_vdf, "intrinsic_default",
                                    td->name, 0, nullptr, nullptr,
                                    VEF_TYPE_STRING, nullptr, extension_name)) {
      return std::nullopt;
    }
  }

  // Validate that int_to_params requires resolve_params.
  if (int_to_params_vdf != nullptr && resolve_params_vdf == nullptr) {
    LogVSQL(ERROR_LEVEL,
            "Type '%s' in extension '%s' has int_to_params but no "
            "resolve_params",
            type_name.c_str(), extension_name.c_str());
    return std::nullopt;
  }

  // Build function objects.
  EncodeFunction encode_fn =
      encode_vdf ? EncodeFunction(encode_vdf) : EncodeFunction(td->encode_func);
  DecodeFunction decode_fn =
      decode_vdf ? DecodeFunction(decode_vdf) : DecodeFunction(td->decode_func);
  CompareFunction compare_fn = compare_vdf ? CompareFunction(compare_vdf)
                                           : CompareFunction(td->compare_func);
  std::optional<HashFunction> hash_fn;
  if (hash_vdf != nullptr)
    hash_fn.emplace(hash_vdf);
  else if (td->hash_func != nullptr)
    hash_fn.emplace(td->hash_func);

  std::optional<RealValueFunction> real_value_fn;
  if (real_value_vdf != nullptr) real_value_fn.emplace(real_value_vdf);

  std::optional<IntToParamsFunction> int_to_params_fn;
  if (int_to_params_vdf != nullptr) int_to_params_fn.emplace(int_to_params_vdf);

  std::optional<ResolveParamsFunction> resolve_params_fn;
  if (resolve_params_vdf != nullptr)
    resolve_params_fn.emplace(resolve_params_vdf);

  // 5. Build TypeDescriptor.
  TypeDescriptor descriptor(
      TypeDescriptorKey(type_name, extension_name, extension_version),
      VEF_PROTOCOL_4, MYSQL_TYPE_VARCHAR, td->persisted_length,
      td->max_decode_buffer_length, td->max_persisted_length,
      is_variable ? LengthKind::Variable : LengthKind::Fixed,
      std::move(encode_fn), std::move(decode_fn), std::move(compare_fn),
      std::move(hash_fn), std::move(real_value_fn), std::move(int_to_params_fn),
      std::move(resolve_params_fn));

  // 7. Set intrinsic default (VDF or string literal, mutually exclusive).
  if (intrinsic_default_vdf != nullptr) {
    if (td->intrinsic_default_str != nullptr) {
      LogVSQL(ERROR_LEVEL,
              "Type '%s' in extension '%s' specifies both "
              "intrinsic_default_vdf_name and intrinsic_default_str; "
              "only one may be set",
              type_name.c_str(), extension_name.c_str());
      return std::nullopt;
    }
    descriptor.set_intrinsic_default_fn(
        IntrinsicDefaultFunction(intrinsic_default_vdf));
  } else if (td->intrinsic_default_str != nullptr) {
    descriptor.set_intrinsic_default_str(
        std::string(td->intrinsic_default_str));
  }

  return std::optional<TypeDescriptor>(std::move(descriptor));
}

}  // namespace veb
}  // namespace villagesql
