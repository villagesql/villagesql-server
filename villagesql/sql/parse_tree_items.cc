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

#include <set>
#include <string>
#include <string_view>
#include <vector>

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

  std::set<std::string> type_exts;
  bool types_use_protocol_1 = true;
  for (const auto *t : results) {
    type_exts.insert(villagesql::canonical_extension_name(t->extension_name()));
    if (t->protocol() != VEF_PROTOCOL_1) types_use_protocol_1 = false;
  }

  const std::string type_prefix =
      villagesql::canonical_extension_name(type_name) + "::";
  const std::string canonical_method =
      villagesql::canonical_extension_name(method_name);
  std::set<std::string> valid_methods;

  for (const auto *f : victionary.funcs().get_all_committed()) {
    if (!f->is_callable_from_sql() ||
        f->key().str().compare(0, type_prefix.size(), type_prefix) != 0) {
      continue;
    }

    const std::string extension_name =
        villagesql::canonical_extension_name(f->extension_name());
    const std::string method =
        villagesql::canonical_extension_name(f->function_name())
            .substr(type_prefix.size());

    if (type_exts.contains(extension_name)) {
      valid_methods.insert(method);
    }
  }

  bool is_valid_method = false;
  for (const auto &m : valid_methods) {
    if (canonical_method == m) {
      is_valid_method = true;
      break;
    }
  }

  if (!is_valid_method) {
    const std::string head =
        "Unknown method '" + method_name + "' for type '" + type_name + "'";
    const size_t budget = MYSQL_ERRMSG_SIZE - 1 > head.size()
                              ? MYSQL_ERRMSG_SIZE - 1 - head.size()
                              : 0;

    const std::vector<std::string> method_list{valid_methods.begin(),
                                               valid_methods.end()};
    const std::string methods = join_names_bounded(method_list, budget);

    if (methods.empty()) {
      if (types_use_protocol_1) {
        villagesql_error(
            "Type '%s' method '%s' does not support '::' syntax "
            "(not registered with a '::' name)",
            MYF(0), type_name.c_str(), method_name.c_str());
      } else {
        villagesql_error("%s", MYF(0), head.c_str());
      }
    } else {
      villagesql_error("%s%s", MYF(0), (head + ". Valid methods: ").c_str(),
                       methods.c_str());
    }

    return;
  }

  villagesql_error(
      "Type '%s' method '%s' does not support '::' syntax "
      "(not registered with a '::' name)",
      MYF(0), type_name.c_str(), method_name.c_str());
}

// Intrinsic default function should not be callable. For a parameterized
// types, the parameters are needed to resolve the default value.
// The type's parameters are unknown during SQL call. Thus, SQL call is
// blocked.
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

constexpr std::string_view kSeparator = ", ";
constexpr std::string_view kSuffixPrefix = ", and ";
constexpr std::string_view kSuffixSuffix = " more methods";

// Length of ", and N more methods", or 0 when nothing is omitted.
size_t omitted_suffix_len(size_t omitted_count) {
  if (omitted_count == 0) return 0;
  return kSuffixPrefix.size() + std::to_string(omitted_count).size() +
         kSuffixSuffix.size();
}

// Length of the first `name_count` names joined with the separator.
size_t joined_len(const std::vector<std::string> &names, size_t name_count) {
  if (name_count == 0) return 0;
  size_t total_len = kSeparator.size() * (name_count - 1);
  for (size_t index = 0; index < name_count; ++index) {
    total_len += names[index].size();
  }
  return total_len;
}

std::string join_first(const std::vector<std::string> &names,
                       size_t name_count) {
  std::string result;
  result.reserve(joined_len(names, name_count));
  for (size_t index = 0; index < name_count; ++index) {
    if (index > 0) result += kSeparator;
    result += names[index];
  }
  return result;
}

// Largest number of leading names that fit in `budget` together with the
// "and N more methods" suffix.
size_t max_names_that_fit(const std::vector<std::string> &names,
                          size_t budget) {
  const size_t total_names = names.size();
  size_t used_len = 0;
  size_t best_count = 0;

  for (size_t candidate_count = 1; candidate_count <= total_names;
       ++candidate_count) {
    const size_t newest_name_len = names[candidate_count - 1].size();
    const size_t separator_len = candidate_count > 1 ? kSeparator.size() : 0;
    used_len += separator_len + newest_name_len;

    const size_t omitted_count = total_names - candidate_count;
    if (used_len + omitted_suffix_len(omitted_count) > budget) break;

    best_count = candidate_count;
  }
  return best_count;
}

std::string join_names_bounded(const std::vector<std::string> &names,
                               size_t budget) {
  if (names.empty() || budget == 0) return {};

  const size_t total_names = names.size();

  // Everything fits: no truncation needed.
  if (joined_len(names, total_names) <= budget) {
    return join_first(names, total_names);
  }

  const size_t shown_count = max_names_that_fit(names, budget);

  // Not even one name fits alongside the suffix.
  if (shown_count == 0) {
    std::string fallback = std::to_string(total_names) + " not listed";
    return fallback.size() <= budget ? fallback : std::string{};
  }

  const size_t omitted_count = total_names - shown_count;
  return join_first(names, shown_count) + std::string(kSuffixPrefix) +
         std::to_string(omitted_count) + std::string(kSuffixSuffix);
}
