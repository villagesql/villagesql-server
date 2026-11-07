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

#include "villagesql/sql/parse_tree_items.h"

#include <string>
#include <string_view>

#include "lex_string.h"
#include "sql/item_create.h"
#include "sql/parse_tree_nodes.h"
#include "sql/sql_class.h"
#include "sql/sql_udf.h"
#include "villagesql/include/error.h"
#include "villagesql/schema/descriptor/func_descriptor.h"
#include "villagesql/schema/descriptor/type_descriptor.h"
#include "villagesql/schema/victionary_client.h"
#include "villagesql/sql/custom_vdf.h"
#include "villagesql/sql/func_lookup.h"

#include <algorithm>
#include <vector>

static bool type_has_intrinsic_method(const villagesql::TypeDescriptor &td,
                                      std::string_view method) {
  if (method == "from_string") {
    return td.has_encode_fn();
  }
  if (method == "to_string") {
    return td.has_decode_fn();
  }
  if (method == "compare") {
    return td.has_compare_fn();
  }
  if (method == "hash") {
    return td.hash_fn().has_value();
  }
  return false;
}

static std::string make_type_method_func_name(std::string_view type_name,
                                              std::string_view method) {
  return std::string(type_name) + "::" + std::string(method);
}

static bool type_method_registered_for_sql(
    villagesql::VictionaryClient &victionary, std::string_view ext_name,
    std::string_view type_name, std::string_view method) {
  const std::string func_name = make_type_method_func_name(type_name, method);
  if (!ext_name.empty()) {
    return villagesql::func_exists_locked(ext_name, func_name);
  }

  villagesql::FuncKeyPrefix prefix{func_name};
  auto matches = victionary.funcs().get_prefix_committed(prefix);
  for (const villagesql::FuncDescriptor *desc : matches) {
    if (desc->is_callable_from_sql()) {
      return true;
    }
  }
  return false;
}

static std::vector<std::string> collect_sql_type_methods(
    villagesql::VictionaryClient &victionary, std::string_view ext_name,
    std::string_view type_name) {
  const std::string func_prefix = std::string(type_name) + "::";
  villagesql::FuncKeyPrefix prefix =
      ext_name.empty()
          ? villagesql::FuncKeyPrefix{func_prefix}
          : villagesql::FuncKeyPrefix{func_prefix, std::string{ext_name}};
  auto funcs = victionary.funcs().get_prefix_committed(prefix);

  std::vector<std::string> methods;
  const size_t type_prefix_len = type_name.size() + 2;
  for (const villagesql::FuncDescriptor *desc : funcs) {
    if (!desc->is_callable_from_sql()) {
      continue;
    }
    const std::string &fname = desc->function_name();
    if (fname.size() <= type_prefix_len) {
      continue;
    }
    methods.push_back(fname.substr(type_prefix_len));
  }
  std::sort(methods.begin(), methods.end());
  methods.erase(std::unique(methods.begin(), methods.end()), methods.end());
  return methods;
}

static std::string join_methods(const std::vector<std::string> &methods) {
  std::string joined;
  for (size_t i = 0; i < methods.size(); ++i) {
    if (i > 0) {
      joined += ", ";
    }
    joined += methods[i];
  }
  return joined;
}

// Emit a targeted error when a '::' function name fails lookup.
// Checks whether the type prefix is unknown, the method suffix is invalid,
// or the type/method combination simply has no '::' VDF registered.
static void emit_type_method_error(std::string_view ext_name,
                                   const LEX_STRING &func) {
  std::string_view func_sv{func.str, func.length};
  size_t sep = func_sv.find("::");
  if (sep == std::string_view::npos) {
    return;
  }

  std::string type_name{func_sv.substr(0, sep)};
  std::string method_name{func_sv.substr(sep + 2)};

  villagesql::VictionaryClient &victionary =
      villagesql::VictionaryClient::instance();
  if (!victionary.is_initialized()) {
    villagesql_error(
        "Type method '%s' unavailable: VillageSQL not yet initialized", MYF(0),
        func.str);
    return;
  }

  auto lock = victionary.get_read_lock();

  villagesql::TypeDescriptorKeyPrefix prefix =
      ext_name.empty() ? villagesql::TypeDescriptorKeyPrefix{type_name}
                       : villagesql::TypeDescriptorKeyPrefix{
                             type_name, std::string{ext_name}};
  auto results = victionary.type_descriptors().get_prefix_committed(prefix);

  if (results.empty()) {
    villagesql_error("Unknown custom type '%s'", MYF(0), type_name.c_str());
    return;
  }

  const std::vector<std::string> sql_methods =
      collect_sql_type_methods(victionary, ext_name, type_name);

  bool has_intrinsic_method = false;
  for (const villagesql::TypeDescriptor *td : results) {
    if (type_has_intrinsic_method(*td, method_name)) {
      has_intrinsic_method = true;
      break;
    }
  }

  const bool registered = type_method_registered_for_sql(
      victionary, ext_name, type_name, method_name);
  if (has_intrinsic_method && !registered) {
    villagesql_error(
        "Type '%s' method '%s' does not support '::' syntax "
        "(not registered with a '::' name)",
        MYF(0), type_name.c_str(), method_name.c_str());
    return;
  }

  const std::string valid_methods = join_methods(sql_methods);
  if (valid_methods.empty()) {
    villagesql_error("Unknown method '%s' for type '%s'", MYF(0),
                     method_name.c_str(), type_name.c_str());
    return;
  }

  villagesql_error(
      "Unknown method '%s' for type '%s'. Valid methods: %s", MYF(0),
      method_name.c_str(), type_name.c_str(), valid_methods.c_str());
}

// Intrinsic default function should not be callable. For a parameterized
// types, the parameters are needed to resolve the default value.
// The type's parameters are unknown during SQL call. Thus, SQL call is blocked.
static void emit_not_callable_error(std::string_view ext_name,
                                    const LEX_STRING &func) {
  villagesql_error(
      "Function '%.*s.%.*s' supplies the intrinsic default for a custom type "
      "and cannot be called directly",
      MYF(0), static_cast<int>(ext_name.length()), ext_name.data(),
      static_cast<int>(func.length), func.str);
}

bool try_itemize_custom_vdf(Parse_context *pc, const LEX_STRING &extension_name,
                            const LEX_STRING &func, PT_item_list *opt_expr_list,
                            Item **res, bool *error) {
  *error = false;

  // Look up function (reference tied to statement mem_root lifetime)
  const villagesql::FuncDescriptor *func_desc = villagesql::find_func(
      to_string_view(extension_name), to_string_view(func), *pc->thd->mem_root);
  if (func_desc == nullptr) {
    // If this looks like a '::' type method call, emit a targeted error.
    if (func.str && func.length >= 3) {
      std::string_view func_sv{func.str, func.length};
      if (func_sv.find("::") != std::string_view::npos) {
        emit_type_method_error(to_string_view(extension_name), func);
        *error = true;
        return true;
      }
    }
    return false;  // Not found - let caller try other resolution
  }

  if (!func_desc->is_callable_from_sql()) {
    emit_not_callable_error(to_string_view(extension_name), func);
    *error = true;
    return true;
  }

  // Add custom function to the list of used custom routines for MDL tracking
  custom_add_used_routine(pc->thd->lex, pc->thd->stmt_arena, extension_name.str,
                          extension_name.length, func.str, func.length);

  // Create udf_func wrapper from FuncDescriptor (allocated on mem_root)
  udf_func *udf =
      villagesql::make_udf_func_from_vdf(func_desc, *pc->thd->mem_root);
  if (!udf) {
    *error = true;
    return true;  // Handled, but with allocation error
  }

  // Create UDF item using the wrapper
  *res = Create_udf_func::s_singleton.create(pc->thd, udf, opt_expr_list);
  if (*res == nullptr || (*res)->itemize(pc, res)) {
    *error = true;
    return true;  // Handled, but with error
  }

  return true;  // Successfully handled as VDF
}

bool try_itemize_unqualified_vdf(Parse_context *pc, const LEX_STRING &func,
                                 PT_item_list *opt_expr_list, Item **res,
                                 bool *error) {
  *error = false;

  std::string ambiguous_extensions;
  const villagesql::FuncDescriptor *vdf_desc =
      villagesql::find_func_unqualified(
          to_string_view(func), *pc->thd->mem_root, &ambiguous_extensions);

  if (vdf_desc == nullptr) {
    if (!ambiguous_extensions.empty()) {
      villagesql_error(
          "Ambiguous VDF '%s' - provided by multiple extensions: %s. "
          "Use qualified name (extension.function).",
          MYF(0), func.str, ambiguous_extensions.c_str());
      *error = true;
      return true;
    }
    // If this looks like a '::' type method call, emit a targeted error.
    if (func.str && func.length >= 3) {
      std::string_view func_sv{func.str, func.length};
      if (func_sv.find("::") != std::string_view::npos) {
        emit_type_method_error(std::string_view{}, func);
        *error = true;
        return true;
      }
    }
    return false;  // Not found - let caller try other resolution
  }

  const std::string &ext_name = vdf_desc->extension_name();

  if (!vdf_desc->is_callable_from_sql()) {
    emit_not_callable_error(ext_name, func);
    *error = true;
    return true;
  }

  custom_add_used_routine(pc->thd->lex, pc->thd->stmt_arena, ext_name.c_str(),
                          ext_name.length(), func.str, func.length);

  udf_func *udf =
      villagesql::make_udf_func_from_vdf(vdf_desc, *pc->thd->mem_root);
  if (udf == nullptr) {
    *error = true;
    return true;  // Handled, but with allocation error
  }

  *res = Create_udf_func::s_singleton.create(pc->thd, udf, opt_expr_list);
  if (*res == nullptr || (*res)->itemize(pc, res)) {
    *error = true;
    return true;  // Handled, but with error
  }

  return true;
}
