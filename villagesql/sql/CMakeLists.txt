# Copyright (c) 2026 VillageSQL Contributors
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, see <https://www.gnu.org/licenses/>.

SET(VILLAGESQL_SQL_SOURCES
  custom_index_backend.cc
  custom_index_hypergraph_optimizer.cc
  custom_index_runtime.cc
  custom_vdf.cc
  func_lookup.cc
  initialize.cc
  metadata_modifier.cc
  parse_tree_items.cc
  util.cc
)

# Object library for compilation into sql_main (avoids circular dependencies)
ADD_LIBRARY(villagesql_sql OBJECT
  ${VILLAGESQL_SQL_SOURCES})

TARGET_LINK_LIBRARIES(villagesql_sql PRIVATE extra::rapidjson)

ADD_DEPENDENCIES(villagesql_sql GenError)
ADD_DEPENDENCIES(villagesql_sql GenServerSource)
# initialize.cc includes the generated villagesql/include/version.h
ADD_DEPENDENCIES(villagesql_sql villagesql_version_gen)
TARGET_LINK_LIBRARIES(villagesql_sql PRIVATE extra::unordered_dense)
