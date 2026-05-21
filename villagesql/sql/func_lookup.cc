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

#include "villagesql/sql/func_lookup.h"

#include <cstring>
#include <string>

#include "my_alloc.h"
#include "sql/sql_udf.h"
#include "villagesql/schema/descriptor/func_descriptor.h"
#include "villagesql/schema/victionary_client.h"

namespace villagesql {

const FuncDescriptor *find_func(std::string_view ext_name,
                                std::string_view func_name,
                                MEM_ROOT &cleanup_scope) {
  if (ext_name.empty() || func_name.empty()) {
    return nullptr;
  }

  VictionaryClient &victionary = VictionaryClient::instance();

  // During server bootstrap, VictionaryClient may not be initialized yet.
  // Return nullptr in this case - functions aren't available until extensions
  // load.
  if (!victionary.is_initialized()) {
    return nullptr;
  }

  FuncKey key{std::string{func_name}, std::string{ext_name}};

  auto lock = victionary.get_read_lock();

  return victionary.funcs().acquire(key, cleanup_scope);
}

bool func_exists(std::string_view ext_name, std::string_view func_name) {
  if (ext_name.empty() || func_name.empty()) {
    return false;
  }

  VictionaryClient &victionary = VictionaryClient::instance();

  // During server bootstrap, VictionaryClient may not be initialized yet.
  // Return false in this case - functions aren't available until extensions
  // load.
  if (!victionary.is_initialized()) {
    return false;
  }

  FuncKey key{std::string{func_name}, std::string{ext_name}};

  auto lock = victionary.get_read_lock();

  return victionary.funcs().get_committed(key) != nullptr;
}

const FuncDescriptor *find_func_unqualified(std::string_view func_name,
                                            MEM_ROOT &cleanup_scope,
                                            std::string *ambiguous_extensions) {
  ambiguous_extensions->clear();
  if (func_name.empty()) {
    return nullptr;
  }

  VictionaryClient &victionary = VictionaryClient::instance();

  // During server bootstrap, VictionaryClient may not be initialized yet.
  // Return nullptr in this case - VDFs aren't available until extensions load.
  if (!victionary.is_initialized()) {
    return nullptr;
  }

  FuncKeyPrefix prefix{std::string{func_name}};
  auto lock = victionary.get_read_lock();
  auto results = victionary.funcs().get_prefix_committed(prefix);

  if (results.empty()) {
    return nullptr;
  }
  if (results.size() > 1) {
    // Build list of extension names for error message
    for (size_t i = 0; i < results.size(); i++) {
      if (i > 0) ambiguous_extensions->append(", ");
      ambiguous_extensions->append(results[i]->extension_name());
    }
    return nullptr;
  }

  return victionary.funcs().acquire(results[0]->key(), cleanup_scope);
}

udf_func *make_udf_func_from_vdf(const FuncDescriptor *desc,
                                 MEM_ROOT &mem_root) {
  if (!desc || !desc->func_desc()) {
    return nullptr;
  }

  udf_func *udf = static_cast<udf_func *>(mem_root.Alloc(sizeof(udf_func)));
  if (!udf) {
    return nullptr;
  }
  memset(udf, 0, sizeof(*udf));

  const vef_func_desc_t *func_desc = desc->func_desc();

  // Set the function name
  size_t func_name_len = strlen(func_desc->name);
  char *name_str = static_cast<char *>(mem_root.Alloc(func_name_len + 1));
  if (!name_str) {
    return nullptr;
  }
  memcpy(name_str, func_desc->name, func_name_len + 1);
  udf->name.str = name_str;
  udf->name.length = func_name_len;

  // Set the extension name
  const std::string &ext_name = desc->extension_name();
  char *ext_str = static_cast<char *>(mem_root.Alloc(ext_name.length() + 1));
  if (!ext_str) {
    return nullptr;
  }
  memcpy(ext_str, ext_name.c_str(), ext_name.length() + 1);
  udf->extension_name.str = ext_str;
  udf->extension_name.length = ext_name.length();

  // Build qualified name: extension.function
  size_t qualified_len = ext_name.length() + 1 + func_name_len;
  char *qualified_str = static_cast<char *>(mem_root.Alloc(qualified_len + 1));
  if (!qualified_str) {
    return nullptr;
  }
  memcpy(qualified_str, ext_name.c_str(), ext_name.length());
  qualified_str[ext_name.length()] = '.';
  memcpy(qualified_str + ext_name.length() + 1, func_desc->name, func_name_len);
  qualified_str[qualified_len] = '\0';
  udf->qualified_name.str = qualified_str;
  udf->qualified_name.length = qualified_len;

  // Set VDF-specific fields
  udf->returns = desc->return_type();
  udf->type = (desc->protocol() >= VEF_PROTOCOL_3 && func_desc->clear)
                  ? UDFTYPE_AGGREGATE
                  : UDFTYPE_FUNCTION;
  udf->usage_count = 1;
  udf->calling_convention = UdfCallingConvention::VDF;
  udf->vdf_func_desc = func_desc;
  udf->vdf_protocol = desc->protocol();

  // Classic UDF fields not used for VDFs
  udf->dl = nullptr;
  udf->dlhandle = nullptr;
  udf->func = nullptr;
  udf->func_init = nullptr;
  udf->func_deinit = nullptr;
  udf->func_clear = nullptr;
  udf->func_add = nullptr;

  return udf;
}

}  // namespace villagesql
