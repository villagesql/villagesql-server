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

#ifndef VILLAGESQL_SQL_UTIL_H_
#define VILLAGESQL_SQL_UTIL_H_

#include <string>

#include "include/my_inttypes.h"
#include "lex_string.h"
#include "my_alloc.h"

class Table_ref;

namespace villagesql {

// Build a hash key for UDF lookup. Custom UDFs use "extension.function" format,
// system UDFs use just "function".
std::string make_udf_key(const LEX_STRING &extension_name,
                         const LEX_STRING &function_name);

// Build a hash key for UDF lookup from raw strings with lengths.
// If extension is nullptr or empty, returns just the function name.
std::string make_udf_key(const char *extension, size_t ext_len,
                         const char *function, size_t func_len);

// Allocates "type_name::method_name" on mem_root and returns it as a
// LEX_STRING.
LEX_STRING make_type_method_lex_string(const LEX_STRING &type_name,
                                       const LEX_STRING &method_name,
                                       MEM_ROOT *mem_root);

}  // namespace villagesql

#endif  // VILLAGESQL_SQL_UTIL_H_
