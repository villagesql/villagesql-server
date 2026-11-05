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

#ifndef VILLAGESQL_SYSTEM_VIEWS_EXTENSION_INDEX_TYPES_H_
#define VILLAGESQL_SYSTEM_VIEWS_EXTENSION_INDEX_TYPES_H_

#include "sql/table.h"

class Item;
class THD;
class Table_ref;

extern ST_FIELD_INFO villagesql_extension_index_types_fields[];
int fill_extension_index_types(THD *thd, Table_ref *tables, Item *cond);

#endif  // VILLAGESQL_SYSTEM_VIEWS_EXTENSION_INDEX_TYPES_H_
