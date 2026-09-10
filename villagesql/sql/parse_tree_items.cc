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

  static const char *valid_methods[] = {"from_string", "to_string", "compare",
                                        "hash", "real_value"};
  bool is_valid_method = false;
  for (const char *m : valid_methods) {
    if (method_name == m) {
      is_valid_method = true;
      break;
    }
  }

  if (!is_valid_method) {
    villagesql_error(
        "Unknown method '%s' for type '%s'. Valid methods: from_string, "
        "to_string, compare, hash, real_value",
        MYF(0), method_name.c_str(), type_name.c_str());
    return;
  }

  villagesql_error(
      "Type '%s' method '%s' does not support '::' syntax "
      "(not registered with a '::' name)",
      MYF(0), type_name.c_str(), method_name.c_str());
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
