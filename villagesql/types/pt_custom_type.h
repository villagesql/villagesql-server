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

#ifndef VILLAGESQL_TYPES_PT_CUSTOM_TYPE_H_
#define VILLAGESQL_TYPES_PT_CUSTOM_TYPE_H_

#include <cinttypes>
#include <cstdlib>
#include <string>

#include "lex_string.h"
#include "mysql/components/services/log_builtins.h"
#include "sql/parse_tree_column_attrs.h"
#include "sql/sql_class.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/descriptor/type_context.h"
#include "villagesql/sdk/include/villagesql/abi/types.h"
#include "villagesql/types/util.h"

namespace villagesql {

// Custom types support from VillageSQL.
class PT_custom_type : public PT_type {
  typedef PT_type super;

  // The unvalidated type name. We won't know if it is valid until after
  // we construct the object (and resolve the type).
  const LEX_STRING type_name;
  THD *thd;

  // Represents a particular concrete type - i.e. an abstract type and its
  // options (length, etc.)
  const TypeContext *type_context;

  // Optional length specification from syntax
  const char *length_spec;
  // Buffer for length string (mutable for const method)
  mutable char length_buffer[21];

 private:
  PT_custom_type(const POS &pos, THD *thd, const LEX_STRING &type_name,
                 const char *length = nullptr,
                 const TypeContext *type_context = nullptr)
      : PT_type(pos,
                (type_context
                     ? static_cast<enum_field_types>(
                           type_context->descriptor()->implementation_type())
                     : MYSQL_TYPE_VARCHAR)),
        type_name(type_name),
        thd(thd),
        type_context(type_context),
        length_spec(length) {
    if (nullptr == type_context) {
      // Record the error for now.
      std::string type_key(type_name.str, type_name.length);
      thd->syntax_error_at(
          pos, "Expected a type or a custom type instead of \"%.*s\"",
          static_cast<int>(type_name.length), type_name.str);
      return;
    }

    // If no length_spec provided, generate it from the TypeContext's storage
    // length. For fixed-length types this comes from the descriptor; for
    // variable-length types it is the descriptor's max_persisted_length
    // upper bound.
    if (nullptr == length_spec) {
      int64_t len = type_context->field_buffer_length();
      assert(len > 0);
      snprintf(length_buffer, sizeof(length_buffer), "%" PRId64, len);
      length_spec = length_buffer;
    }
  }

  // Resolve the type descriptor for the given (extension_name, type_name). On
  // success, sets descriptor (which may be nullptr if the type was not found)
  // and returns false. Returns true if descriptor resolution itself failed
  // with an error already recorded.
  static bool resolve_descriptor(const LEX_STRING &extension_name,
                                 const LEX_STRING &type_name,
                                 const TypeDescriptor *&descriptor) {
    descriptor = nullptr;
    return ResolveTypeDescriptor(to_string_view(extension_name),
                                 to_string_view(type_name), descriptor);
  }

 public:
  // During parsing, type_name is not yet validated. However, when we construct
  // this object, we could find out there actually was an error in specifying
  // this as a type.
  // Factory construction is thus required so that we can check the custom type
  // before constructing the object, as PT_type needs to be initialized with
  // the implementation_type of the custom type, and that is a const member in
  // PT_type. Although this isn't the typical pattern for PT nodes, this avoids
  // us creating the TypeContext twice.
  // Factory for custom type names. For qualified names
  // (extension_name.type_name), pass extension_name; for unqualified names,
  // pass empty LEX_STRING {} for extension_name.

  // Resolve params via the resolve_params callback and acquire the concrete
  // TypeContext for the given descriptor. Shared by both TYPE(N) and
  // TYPE('key=value,...') paths. params_str must be in canonical form
  // ("key=value,key=value,...").
  static bool resolve_params_and_context(const POS &pos, THD *thd,
                                         const TypeDescriptor *descriptor,
                                         const std::string &params_str,
                                         const TypeContext *&type_context) {
    char error_msg[VEF_MAX_ERROR_LEN] = {0};
    ResolvedTypeParams resolved = {};
    if (descriptor->resolve_params_fn()->invoke(params_str, &resolved,
                                                error_msg)) {
      thd->syntax_error_at(pos, "%s", error_msg);
      return true;
    }

    // Validate the resolved persisted_length against the type's declared
    // length kind. Fixed-length types must resolve to a concrete positive
    // footprint; variable-length types size their storage per value, so
    // they must not report a fixed persisted_length here.
    if (descriptor->is_variable_length()) {
      if (resolved.persisted_length > 0) {
        thd->syntax_error_at(
            pos,
            "Variable-length type '%s' resolved an unexpected fixed "
            "persisted_length (%" PRId64
            "); variable-length types size storage per value, so "
            "persisted_length should be <= 0 (stay unset)",
            descriptor->qualified_base_name().c_str(),
            resolved.persisted_length);
        return true;
      }
    } else if (resolved.persisted_length <= 0) {
      thd->syntax_error_at(
          pos,
          "Type '%s' resolved an invalid persisted_length (%" PRId64
          "); fixed-length types must resolve to a concrete footprint, so "
          "persisted_length should be > 0",
          descriptor->qualified_base_name().c_str(), resolved.persisted_length);
      return true;
    }

    TypeParameters params(params_str);

    type_context = nullptr;
    if (AcquireOrCreateTypeContext(descriptor, params, *thd->mem_root,
                                   type_context)) {
      return true;
    }
    return false;
  }

  // Factory for the bare, unparameterized custom type syntax: TYPE (no length,
  // no parameters).
  static PT_custom_type *create(MEM_ROOT *pt_mem_root, const POS &pos, THD *thd,
                                const LEX_STRING &extension_name,
                                const LEX_STRING &type_name) {
    // Resolve the descriptor first so we can inspect the type's
    // characteristics.
    const TypeDescriptor *descriptor = nullptr;
    if (resolve_descriptor(extension_name, type_name, descriptor)) {
      return nullptr;
    }

    if (descriptor == nullptr) {
      // Type not found - constructor records the error.
      return new (pt_mem_root)
          PT_custom_type(pos, thd, type_name, nullptr, nullptr);
    }

    const TypeContext *type_context = nullptr;

    if (descriptor->is_parameterized()) {
      if (descriptor->int_to_params_fn().has_value()) {
        // No length provided for TYPE(N)
        thd->syntax_error_at(pos, "Type '%s' requires a length specification",
                             descriptor->qualified_base_name().c_str());
        return nullptr;
      }

      // resolve_params is the single validation gate for parameterized types
      // (fixed- or variable-length). Invoke it with empty parameters so the
      // type can either resolve its defaults or report that parameters are
      // required.
      if (resolve_params_and_context(pos, thd, descriptor, std::string(),
                                     type_context))
        return nullptr;
    } else {
      // Non-parameterized type - acquire the context with empty parameters.
      if (AcquireOrCreateTypeContext(descriptor, TypeParameters(),
                                     *thd->mem_root, type_context))
        return nullptr;
    }

    return new (pt_mem_root)
        PT_custom_type(pos, thd, type_name, nullptr, type_context);
  }

  // Factory for the TYPE(N) integer length syntax. length is guaranteed to be
  // a non-null numeric string by the grammar (field_length).
  static PT_custom_type *create_with_length(MEM_ROOT *pt_mem_root,
                                            const POS &pos, THD *thd,
                                            const LEX_STRING &extension_name,
                                            const LEX_STRING &type_name,
                                            const char *length) {
    assert(length != nullptr);

    const TypeDescriptor *descriptor = nullptr;
    if (resolve_descriptor(extension_name, type_name, descriptor)) {
      return nullptr;
    }

    if (descriptor == nullptr) {
      // Type not found - constructor records the error.
      return new (pt_mem_root)
          PT_custom_type(pos, thd, type_name, nullptr, nullptr);
    }

    if (!descriptor->is_parameterized()) {
      thd->syntax_error_at(pos,
                           "Type '%s' is not parameterized and cannot have a "
                           "length specification",
                           descriptor->qualified_base_name().c_str());
      return nullptr;
    }

    // type provides resolve_params; it must also provide int_to_params to
    // support the TYPE(N) syntax.
    if (!descriptor->int_to_params_fn().has_value()) {
      thd->syntax_error_at(pos,
                           "Type '%s' does not accept a length specification",
                           descriptor->qualified_base_name().c_str());
      return nullptr;
    }

    // Parse the length string to int64_t
    char *endptr = nullptr;
    int64_t int_value = strtoll(length, &endptr, 10);
    if (endptr == length || *endptr != '\0' || int_value <= 0) {
      thd->syntax_error_at(pos, "Invalid length '%s' for type '%s'", length,
                           descriptor->qualified_base_name().c_str());
      return nullptr;
    }

    // Call int_to_params to convert integer to canonical parameter string
    std::string params_str;
    char error_msg[VEF_MAX_ERROR_LEN] = {0};
    if (descriptor->int_to_params_fn()->invoke(int_value, &params_str,
                                               error_msg)) {
      thd->syntax_error_at(pos, "%s", error_msg);
      return nullptr;
    }

    // Normalize to canonical form (lowercase, sorted) just like the
    // TYPE('k=v,...') path does via from_raw.
    TypeParameters canonical = TypeParameters::from_raw(params_str);
    if (canonical.empty()) {
      thd->syntax_error_at(pos, "Invalid parameter string for type '%s'",
                           descriptor->qualified_base_name().c_str());
      return nullptr;
    }

    // resolve_params_and_context will call AcquireOrCreateTypeContext after
    // resolving the parameters
    const TypeContext *type_context = nullptr;
    if (resolve_params_and_context(pos, thd, descriptor, canonical.str(),
                                   type_context)) {
      return nullptr;
    }

    return new (pt_mem_root)
        PT_custom_type(pos, thd, type_name, nullptr, type_context);
  }

  // Factory for TYPE('key=value,...') string parameter syntax. params_str is
  // guaranteed non-null by the grammar (TEXT_STRING_literal).
  static PT_custom_type *create_with_params(MEM_ROOT *pt_mem_root,
                                            const POS &pos, THD *thd,
                                            const LEX_STRING &extension_name,
                                            const LEX_STRING &type_name,
                                            const char *params_str,
                                            size_t params_str_len) {
    assert(params_str != nullptr);

    // Resolve the descriptor first
    const TypeDescriptor *descriptor = nullptr;
    if (resolve_descriptor(extension_name, type_name, descriptor)) {
      return nullptr;
    }

    if (descriptor == nullptr) {
      return new (pt_mem_root)
          PT_custom_type(pos, thd, type_name, nullptr, nullptr);
    }

    if (!descriptor->resolve_params_fn().has_value()) {
      thd->syntax_error_at(pos, "Type '%s' does not accept parameters",
                           descriptor->qualified_base_name().c_str());
      return nullptr;
    }

    // Normalize the raw parameter string to canonical form
    std::string input(params_str, params_str_len);
    if (input.empty()) {
      thd->syntax_error_at(pos, "Empty parameter string for type '%s'",
                           descriptor->qualified_base_name().c_str());
      return nullptr;
    }

    TypeParameters canonical = TypeParameters::from_raw(input);
    if (canonical.empty()) {
      thd->syntax_error_at(pos, "Invalid parameter string for type '%s'",
                           descriptor->qualified_base_name().c_str());
      return nullptr;
    }

    const TypeContext *type_context = nullptr;
    if (resolve_params_and_context(pos, thd, descriptor, canonical.str(),
                                   type_context)) {
      return nullptr;
    }

    PT_custom_type *ret = new (pt_mem_root)
        PT_custom_type(pos, thd, type_name, nullptr, type_context);
    return ret;
  }

  THD *get_thd() const { return thd; }

  bool is_custom_type() const override { return true; }

  // Return the type context, which should be non-nullptr!
  const TypeContext *get_type_context() const override {
    assert(type_context);
    return type_context;
  }

  const char *get_length() const override { return length_spec; }

  const CHARSET_INFO *get_charset() const override { return &my_charset_bin; }

  // Custom types delegate storage to their implementation_type, so these
  // type-specific attributes don't apply:

  // Decimal precision is only meaningful for DECIMAL/FLOAT/DOUBLE/temporal.
  const char *get_dec() const override { return nullptr; }
  // Geometry subtype is only meaningful for MYSQL_TYPE_GEOMETRY.
  uint get_uint_geom_type() const override { return 0; }
  // Interval lists are only meaningful for ENUM/SET.
  List<String> *get_interval_list() const override { return nullptr; }
  // SERIAL is a special alias for BIGINT UNSIGNED AUTO_INCREMENT UNIQUE.
  bool is_serial_type() const override { return false; }
};

}  // namespace villagesql

#endif  // VILLAGESQL_TYPES_PT_CUSTOM_TYPE_H_
