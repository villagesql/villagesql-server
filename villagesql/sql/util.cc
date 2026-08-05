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

#include "villagesql/sql/util.h"

#include <cstring>

#include "sql/table.h"
#include "villagesql/schema/schema_manager.h"

namespace villagesql {

std::string make_udf_key(const char *extension, size_t ext_len,
                         const char *function, size_t func_len) {
  std::string func_str =
      func_len ? std::string(function, func_len) : std::string(function);
  if (extension && *extension) {
    std::string ext_str =
        ext_len ? std::string(extension, ext_len) : std::string(extension);
    return ext_str + "." + func_str;
  }
  return func_str;
}

LEX_STRING make_type_method_lex_string(const LEX_STRING &type_name,
                                       const LEX_STRING &method_name,
                                       MEM_ROOT *mem_root) {
  // Allocate buffer for "type_name::method_name\0"
  size_t total_len = type_name.length + 2 + method_name.length;
  char *buf = static_cast<char *>(mem_root->Alloc(total_len + 1));
  if (!buf) {
    return {nullptr, 0};
  }
  memcpy(buf, type_name.str, type_name.length);
  buf[type_name.length] = ':';
  buf[type_name.length + 1] = ':';
  memcpy(buf + type_name.length + 2, method_name.str, method_name.length);
  buf[total_len] = '\0';
  return {buf, total_len};
}

}  // namespace villagesql
