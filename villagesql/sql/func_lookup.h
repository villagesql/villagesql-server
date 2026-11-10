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

// VillageSQL Defined Function lookup helpers.
// These functions provide the interface between VictionaryClient and the
// UDF/VDF handling code in the SQL layer.

#ifndef VILLAGESQL_SQL_FUNC_LOOKUP_H_
#define VILLAGESQL_SQL_FUNC_LOOKUP_H_

#include <string>
#include <string_view>

struct MEM_ROOT;
struct udf_func;

namespace villagesql {

class FuncDescriptor;

// Look up a function by extension and function name.
// Returns a pointer to the FuncDescriptor that is valid for the statement
// lifetime (tied to cleanup_scope).
//
// This function acquires a reference to the descriptor that is released
// when cleanup_scope is cleared, ensuring the descriptor remains valid
// even if the extension is uninstalled during statement execution.
//
// Returns: Pointer to FuncDescriptor, or nullptr if not found.
const FuncDescriptor *find_func(std::string_view ext_name,
                                std::string_view func_name,
                                MEM_ROOT &cleanup_scope);

// Quick existence check for a function (no reference counting).
// Use this when you just need to check if a function exists without acquiring
// a reference. The result should not be cached.
//
// Returns: true if the function exists, false otherwise.
bool func_exists(std::string_view ext_name, std::string_view func_name);

// Same as func_exists(), but assumes the caller already holds the
// VictionaryClient read (or write) lock and does NOT acquire it.
//
// Returns: true if the function exists, false otherwise.
bool func_exists_locked(std::string_view ext_name, std::string_view func_name);

// Find VDF by function name only (unqualified lookup).
// Returns the FuncDescriptor if exactly one extension provides the function.
// Sets *ambiguous_extensions to a comma-separated list if multiple found.
// Returns nullptr if not found or ambiguous.
const FuncDescriptor *find_func_unqualified(std::string_view func_name,
                                            MEM_ROOT &cleanup_scope,
                                            std::string *ambiguous_extensions);

// Create a udf_func wrapper from a FuncDescriptor.
// This creates a udf_func structure compatible with the existing UDF handling
// code, allocated on the provided MEM_ROOT.
//
// The udf_func structure contains the VDF-specific fields (vdf_func_desc,
// vdf_protocol, calling_convention) needed for VDF execution.
//
// Parameters:
//   desc - The FuncDescriptor to wrap
//   mem_root - MEM_ROOT for allocation
//
// Returns: Pointer to udf_func, or nullptr on allocation failure.
udf_func *make_udf_func_from_vdf(const FuncDescriptor *desc,
                                 MEM_ROOT &mem_root);

}  // namespace villagesql

#endif  // VILLAGESQL_SQL_FUNC_LOOKUP_H_
