// Copyright (c) 2026 VillageSQL Contributors
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, see <https://www.gnu.org/licenses/>.

#include "villagesql/schema/util.h"

#include "sql/table.h"

namespace villagesql {

bool is_villagesql_system_table(const TABLE *table) {
  if (!table || !table->s) return false;
  return is_villagesql_schema(table->s->db.str);
}

uint count_global_tables(const Table_ref *first) {
  uint n = 0;
  for (const Table_ref *tl = first; tl != nullptr; tl = tl->next_global) ++n;
  return n;
}

}  // namespace villagesql
