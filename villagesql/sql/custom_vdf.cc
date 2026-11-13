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

#include "villagesql/sql/custom_vdf.h"

#include <string.h>

#include "map_helpers.h"
#include "my_psi_config.h"
#include "mysql/psi/psi_memory.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_list.h"
#include "sql/strfunc.h"
#include "villagesql/sql/util.h"

// Modeled after sp_add_used_routine(), with equivalent semantics
bool custom_add_used_routine(Query_tables_list *prelocking_ctx,
                             Query_arena *arena, const char *extension,
                             size_t ext_length, const char *function,
                             size_t func_length) {
  if (prelocking_ctx->croutines == nullptr) {
    prelocking_ctx->croutines.reset(
        new malloc_unordered_map<std::string, Croutine_hash_entry *>(
            PSI_INSTRUMENT_ME));
  }

  std::string key_str =
      villagesql::make_udf_key(extension, ext_length, function, func_length);

  if (prelocking_ctx->croutines->count(key_str) == 0) {
    Croutine_hash_entry *rn =
        (Croutine_hash_entry *)arena->alloc(sizeof(Croutine_hash_entry));
    if (!rn)  // OOM. Error will be reported using fatal_error().
      return false;
    rn->m_key = (char *)arena->alloc(key_str.length());
    if (!rn->m_key) return false;
    rn->m_key_length = key_str.length();
    memcpy(rn->m_key, key_str.c_str(), key_str.length());

    // Store extension name
    rn->m_extension_name = {nullptr, 0};
    if (ext_length > 0 &&
        lex_string_strmake(arena->mem_root, &(rn->m_extension_name), extension,
                           ext_length))
      return false;  // OOM

    // Store function name
    rn->m_function_name = {nullptr, 0};
    if (lex_string_strmake(arena->mem_root, &(rn->m_function_name), function,
                           func_length))
      return false;  // OOM

    prelocking_ctx->croutines->emplace(key_str, rn);
    prelocking_ctx->croutines_list.link_in_list(rn, &rn->next);

    return true;
  }
  return false;
}
