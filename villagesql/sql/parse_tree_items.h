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

#ifndef VILLAGESQL_SQL_PARSE_TREE_ITEMS_H_
#define VILLAGESQL_SQL_PARSE_TREE_ITEMS_H_

#include "lex_string.h"
#include "sql/parse_location.h"  // POS

class Item;
struct Parse_context;
class PT_item_list;

// Try to resolve a qualified function call (extension.function) as a custom
// VDF. Returns true if it was handled as a VDF (check *error for
// success/failure). Returns false if not a VDF, allowing caller to try other
// resolution paths (e.g., stored function).
// Note: calls to VDFs in views reconstruct the SQL and thus need to "hardcode"
// the separator. If that changes, we need to update set_vdf_qualified_name() in
// sql_udf.cc.
bool try_itemize_custom_vdf(Parse_context *pc, const POS &pos,
                            const LEX_STRING &extension_name,
                            const LEX_STRING &func, PT_item_list *opt_expr_list,
                            Item **res, bool *error);

// Try to resolve an unqualified function call as a VDF.
// Returns true if handled (check *error), false to try other resolution.
bool try_itemize_unqualified_vdf(Parse_context *pc, const POS &pos,
                                 const LEX_STRING &func,
                                 PT_item_list *opt_expr_list, Item **res,
                                 bool *error);

#endif  // VILLAGESQL_SQL_PARSE_TREE_ITEMS_H_
