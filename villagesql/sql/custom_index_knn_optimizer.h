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

#ifndef VILLAGESQL_SQL_CUSTOM_INDEX_KNN_OPTIMIZER_H_
#define VILLAGESQL_SQL_CUSTOM_INDEX_KNN_OPTIMIZER_H_

class LogicalOrderings;
class Query_block;
class THD;
template <class T>
class Mem_root_array;
struct SpatialDistanceScanInfo;
struct TABLE;

namespace villagesql {

void CollectCustomKnnOrderingsForHypergraph(
    THD *thd, Query_block *query_block, TABLE *table,
    LogicalOrderings *orderings,
    Mem_root_array<SpatialDistanceScanInfo> *spatial_indexes);

}  // namespace villagesql

#endif  // VILLAGESQL_SQL_CUSTOM_INDEX_KNN_OPTIMIZER_H_
